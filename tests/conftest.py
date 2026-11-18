import json
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

import app as app_module


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Every test gets a private SQLite file and a clean session table."""
    monkeypatch.setattr(app_module, "DB_PATH", tmp_path / "t.db")
    app_module.SESSIONS.clear()
    app_module.init_db()
    yield


@pytest.fixture
def client():
    return TestClient(app_module.app)


@pytest.fixture
def auth(client):
    resp = client.post("/api/login", json={"username": "admin", "password": "123456"})
    assert resp.status_code == 200, resp.text
    return {"Authorization": "Bearer " + resp.json()["token"]}


@pytest.fixture
def insert_batch():
    """Insert a batch through the same storage the app reads.

    This deliberately works both before and after the fix: when the
    ``track`` column does not exist yet it inserts the legacy shape, so the
    very same fixture can reproduce the leak on the old code.
    """

    def _insert(
        *,
        subject="订单入站",
        payload_fields="order_id,amount,currency",
        contract_version="v3",
        result="通过",
        detail="fixture",
        track=None,
    ):
        with app_module.connect() as conn:
            columns = {row["name"] for row in conn.execute("PRAGMA table_info(ingress_batches)")}
            now = datetime.now().isoformat(timespec="seconds")
            if "track" in columns:
                if track is None:
                    shadow_versions = {
                        row["version"]
                        for row in conn.execute("SELECT version FROM contracts WHERE track='影子'")
                    }
                    track = "影子" if contract_version in shadow_versions else "正式"
                cur = conn.execute(
                    "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,track)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (subject, payload_fields, contract_version, result, detail, now, track),
                )
            else:
                cur = conn.execute(
                    "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at)"
                    " VALUES (?,?,?,?,?,?)",
                    (subject, payload_fields, contract_version, result, detail, now),
                )
            batch_id = cur.lastrowid
        return batch_id

    return _insert


@pytest.fixture
def leaked_ledger(insert_batch):
    """Official ledger plus a shadow queue sharing the same batch table.

    Official: 1 passed, 1 buffered (with its buffer row), 1 rejected.
    Shadow:   2 passed trial runs (the leaked rows this task is about).
    """
    ids = {}
    ids["official_pass"] = insert_batch(result="通过")
    ids["official_buffer"] = insert_batch(
        result="缓冲", payload_fields="order_id,amount", detail="缺字段:currency"
    )
    ids["official_reject"] = insert_batch(
        result="拒收", payload_fields="order_id,amount", detail="缺字段:currency"
    )
    with app_module.connect() as conn:
        conn.execute(
            "INSERT INTO buffers(batch_id,reason) VALUES (?,?)",
            (ids["official_buffer"], "缺字段进双写缓冲"),
        )
    ids["shadow_pass_1"] = insert_batch(
        result="通过",
        contract_version="v4-shadow",
        payload_fields="order_id,amount,currency,channel",
    )
    ids["shadow_pass_2"] = insert_batch(
        result="通过",
        contract_version="v4-shadow",
        payload_fields="order_id,amount,currency,channel",
    )
    return ids
