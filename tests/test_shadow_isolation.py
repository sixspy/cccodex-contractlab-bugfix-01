"""影子试跑泄漏切断的回归测试。

口径（见 docs/泄漏切断说明.md）：采用乙路径——影子批次与正式批次同表，
以 ``track`` 轨道标记区分；正式对账查询现场带排除条件重算，禁止物化快照。
"""

import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

import app as app_module


def _connection_path():
    return str(app_module.DB_PATH)


def test_official_reconcile_does_not_count_shadow_passes(client, auth, leaked_ledger):
    body = client.get("/api/reconcile", headers=auth).json()
    # 正式账册：1 通过 + 1 缓冲 + 1 拒收；影子 2 个通过不得混入。
    assert body["ingress_total"] == 3
    assert body["passed"] == 1
    assert body["buffered"] == 1
    assert body["rejected"] == 1
    assert body["buffer_rows"] == 1


def test_official_ingress_list_does_not_show_shadow_rows(client, auth, leaked_ledger):
    items = client.get("/api/ingress", headers=auth).json()["items"]
    assert items, "正式入站列表不应为空"
    assert all(item.get("track") == "正式" for item in items)
    assert all(item["contract_version"] != "v4-shadow" for item in items)


def test_shadow_queue_reports_its_own_numbers(client, auth, leaked_ledger):
    body = client.get("/api/shadow/queue", headers=auth).json()
    assert body["shadow_total"] == 2
    assert body["shadow_passed"] == 2
    assert len(body["items"]) == 2
    assert all(item["track"] == "影子" for item in body["items"])


def test_shadow_refresh_writes_only_the_shadow_track(client, auth):
    resp = client.post(
        "/api/shadow/refresh",
        headers=auth,
        json={"subject": "订单入站", "payload_fields": "order_id,amount,currency,channel"},
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["result"] == "通过"

    # 影子刷新后正式账册仍是 0，影子队列是 1。
    assert client.get("/api/reconcile", headers=auth).json()["passed"] == 0
    assert client.get("/api/shadow/queue", headers=auth).json()["shadow_passed"] == 1
    with app_module.connect() as conn:
        rows = conn.execute(
            "SELECT contract_version, track FROM ingress_batches ORDER BY id"
        ).fetchall()
    assert [(r["contract_version"], r["track"]) for r in rows] == [("v4-shadow", "影子")]


def test_legacy_leak_is_backfilled_without_touching_rejected_batches(
    client, auth, leaked_ledger
):
    # 还原修复前的泄漏状态：影子批次混在正式轨道里。
    with app_module.connect() as conn:
        conn.execute("UPDATE ingress_batches SET track='正式' WHERE id IN (?,?)",
                     (leaked_ledger["shadow_pass_1"], leaked_ledger["shadow_pass_2"]))
        before = {
            r["id"]: tuple(r)
            for r in conn.execute(
                "SELECT id,subject,payload_fields,contract_version,result,detail,created_at,track"
                " FROM ingress_batches ORDER BY id"
            )
        }

    app_module.repair_shadow_leak()

    with app_module.connect() as conn:
        shadow = {
            r["id"] for r in conn.execute("SELECT id FROM ingress_batches WHERE track='影子'")
        }
        after = {
            r["id"]: tuple(r)
            for r in conn.execute(
                "SELECT id,subject,payload_fields,contract_version,result,detail,created_at,track"
                " FROM ingress_batches ORDER BY id"
            )
        }
    assert shadow == {leaked_ledger["shadow_pass_1"], leaked_ledger["shadow_pass_2"]}
    # 只有 track 一列变化；结果、说明、载荷、时间与已拒收批次全部原样。
    for batch_id, row in before.items():
        if batch_id in shadow:
            assert after[batch_id][:-1] == row[:-1]
            assert after[batch_id][-1] == "影子"
        else:
            assert after[batch_id] == row
    assert after[leaked_ledger["official_reject"]][4] == "拒收"
    # 回填不物化任何正式快照，正式对账仍靠现场排除条件重算。
    assert client.get("/api/reconcile", headers=auth).json()["passed"] == 1


def test_policy_and_review_cards_do_not_eat_shadow_numbers(client, auth, leaked_ledger):
    policy = client.get("/api/policy", headers=auth).json()
    review = client.get("/api/review", headers=auth).json()
    assert policy["official_total"] == 3
    assert review["official_total"] == 3
    assert review["passed"] == 1
    assert review["rejected"] == 1


def test_legacy_database_without_track_column_is_migrated(isolated_db):
    # 直接建出修复前的旧库形态（无 track 列），再走正式 init_db 迁移。
    app_module.DB_PATH.unlink()
    with sqlite3.connect(_connection_path()) as raw:
        raw.executescript(
            """
            CREATE TABLE ingress_batches(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              subject TEXT NOT NULL,
              payload_fields TEXT NOT NULL,
              contract_version TEXT,
              result TEXT NOT NULL,
              detail TEXT NOT NULL,
              created_at TEXT NOT NULL
            );
            INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)
              VALUES ('订单入站','order_id,amount,currency','v3','通过','字段齐全','2026-01-01T00:00:00');
            INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)
              VALUES ('订单入站','order_id,amount,currency,channel','v4-shadow','通过','影子字段齐全','2026-01-01T00:00:01');
            INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)
              VALUES ('订单入站','order_id','v3','拒收','缺字段:amount,currency','2026-01-01T00:00:02');
            """
        )
    app_module.init_db()

    with app_module.connect() as conn:
        rows = {
            r["id"]: (r["contract_version"], r["result"], r["track"])
            for r in conn.execute("SELECT id,contract_version,result,track FROM ingress_batches")
        }
    assert rows[1] == ("v3", "通过", "正式")
    assert rows[2] == ("v4-shadow", "通过", "影子")
    # 已拒收批次的结果与说明未被改写，也没被误划进影子。
    assert rows[3] == ("v3", "拒收", "正式")


def test_writing_shadow_contract_into_official_track_is_blocked_and_logged(
    client, auth
):
    """偏离隔离写路径（影子批次按正式轨道写入）必须拦下并记泄漏偏了。"""
    with app_module.connect() as conn:
        shadow_contract = conn.execute(
            "SELECT * FROM contracts WHERE track='影子'"
        ).fetchone()
        with pytest.raises(app_module.LeakDeviation):
            app_module.insert_batch(
                conn, operation="测试直写", contract=shadow_contract,
                track=app_module.OFFICIAL_TRACK, subject="订单入站",
                payload_fields="order_id,amount,currency",
                result="通过", detail="试图混入正式轨道",
            )
        assert conn.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"] == 0

    logged = client.get("/api/leak/deviations", headers=auth).json()["deviations"]
    assert logged and logged[0]["operation"] == "测试直写"
    assert client.get("/api/reconcile", headers=auth).json()["passed"] == 0


def test_concurrent_reconcile_and_shadow_refresh_queue_and_record_contention(
    client, auth, monkeypatch
):
    monkeypatch.setattr(app_module, "CALC_HOLD_SECONDS", 0.25)
    barrier = threading.Barrier(2)
    results = {}

    def hit(name, method, path, payload=None):
        thread_client = TestClient(app_module.app)
        barrier.wait(timeout=5)
        resp = thread_client.request(
            method, path, headers=auth, json=payload
        )
        results[name] = resp

    payload = {"subject": "订单入站", "payload_fields": "order_id,amount,currency,channel"}
    threads = [
        threading.Thread(target=hit, args=("reconcile", "GET", "/api/reconcile")),
        threading.Thread(target=hit, args=("shadow", "POST", "/api/shadow/refresh", payload)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert results["reconcile"].status_code == 200, results["reconcile"].text
    assert results["shadow"].status_code == 200, results["shadow"].text

    body = client.get("/api/reconcile", headers=auth).json()
    assert body["contention"] >= 1
    with app_module.connect() as conn:
        logged = conn.execute("SELECT COUNT(*) c FROM calculation_contention").fetchone()["c"]
    assert logged >= 1
