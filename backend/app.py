
import hashlib, secrets, sqlite3, threading, time
from contextlib import contextmanager
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 正式对账与影子刷新共用同一把算账锁：同时抢算须排队，后到者只记争用，
# 不得交叉读取对方尚未提交的中间状态。CALC_HOLD_SECONDS 仅用于放大测试窗口。
CALC_LOCK = threading.Lock()
CALC_HOLD_SECONDS = 0.0

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL,
          track TEXT NOT NULL DEFAULT '正式'
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS calculation_contention(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          operation TEXT NOT NULL,
          reason TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS leak_deviations(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          operation TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        _migrate_batch_track(conn)
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3","订单入站","严格","正式","启用","order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow","订单入站","兼容","影子","试跑","order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2","库存入站","严格","正式","停用","sku,qty"))
        repair_shadow_leak(conn)

@app.on_event("startup")
def _s():
    init_db()

SHADOW_TRACK = "影子"
OFFICIAL_TRACK = "正式"

def _shadow_versions(conn) -> set[str]:
    return {
        row["version"]
        for row in conn.execute("SELECT version FROM contracts WHERE track=?", (SHADOW_TRACK,))
    }

def _migrate_batch_track(conn) -> bool:
    """给旧库补 track 列；历史回填统一由 init_db 末尾的 repair_shadow_leak 做。"""
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(ingress_batches)")}
    if "track" in columns:
        return False
    conn.execute("ALTER TABLE ingress_batches ADD COLUMN track TEXT NOT NULL DEFAULT '正式'")
    return True

def repair_shadow_leak(conn=None) -> int:
    """把历史影子批次标回影子轨道。

    只写 track 一列，绝不改写任何批次的结果、说明、载荷与时间；已拒收批次
    的结果原样保留。重复执行无副作用。
    """
    owns = conn is None
    conn = conn or connect()
    try:
        versions = sorted(_shadow_versions(conn))
        if not versions:
            return 0
        placeholders = ",".join("?" for _ in versions)
        cur = conn.execute(
            f"""UPDATE ingress_batches SET track=?
                WHERE track=? AND contract_version IN ({placeholders})""",
            (SHADOW_TRACK, OFFICIAL_TRACK, *versions),
        )
        if owns:
            conn.commit()
        return cur.rowcount
    finally:
        if owns:
            conn.close()

@contextmanager
def calculation_lock(operation: str):
    """正式对账与影子刷新共用算账锁：抢不到的一方记一条争用后排队。"""
    acquired = CALC_LOCK.acquire(blocking=False)
    if not acquired:
        log = connect()
        try:
            with log:
                log.execute(
                    "INSERT INTO calculation_contention(operation,reason,created_at) VALUES (?,?,?)",
                    (operation, "后到排队争用", datetime.now().isoformat(timespec="seconds")),
                )
        finally:
            log.close()
        CALC_LOCK.acquire()
    try:
        if CALC_HOLD_SECONDS:
            time.sleep(CALC_HOLD_SECONDS)
        yield
    finally:
        CALC_LOCK.release()

def _contention_count(conn) -> int:
    return conn.execute("SELECT COUNT(*) c FROM calculation_contention").fetchone()["c"]

class LeakDeviation(Exception):
    """写入路径试图偏离泄漏切断说明：拦下并记一条泄漏偏了。"""

    def __init__(self, operation: str, detail: str):
        super().__init__(detail)
        self.operation = operation
        self.detail = detail

def record_leak_deviation(operation: str, detail: str) -> None:
    """用独立连接落账：即使业务写入随后回滚，泄漏偏了的记录也必须留下。"""
    with connect() as conn:
        conn.execute(
            "INSERT INTO leak_deviations(operation,detail,created_at) VALUES (?,?,?)",
            (operation, detail, datetime.now().isoformat(timespec="seconds")),
        )

def _guard_track_write(operation: str, contract: sqlite3.Row, track: str) -> None:
    """正式轨道只收正式契约，影子轨道只收影子契约；否则拦下并记泄漏偏了。"""
    if contract["track"] != track:
        detail = f"契约 {contract['version']} 轨道={contract['track']} 不得写入 {track} 轨道"
        # 用独立连接落账：即使业务写入随后回滚，泄漏偏了的记录也必须留下。
        record_leak_deviation(operation, detail)
        raise LeakDeviation(operation, detail)

def insert_batch(conn, *, operation: str, contract: sqlite3.Row, track: str,
                 subject: str, payload_fields: str, result: str, detail: str) -> int:
    """唯一的批次写入路径：轨道由契约决定，写错轨道当场拦下。"""
    _guard_track_write(operation, contract, track)
    now = datetime.now().isoformat(timespec="seconds")
    cur = conn.execute(
        "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,track)"
        " VALUES (?,?,?,?,?,?,?)",
        (subject, payload_fields, contract["version"], result, detail, now, track),
    )
    return cur.lastrowid

@app.exception_handler(LeakDeviation)
def _leak_deviation_handler(request, exc: LeakDeviation):
    # 偏了已由守卫落账；这里只负责把请求拦成 409。
    return JSONResponse(status_code=409, content={"detail": "泄漏偏了：" + exc.detail})

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}

class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present

@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    with connect() as c:
        ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1", (body.subject,)).fetchone()
        if not ctr:
            raise HTTPException(404, "无启用正式契约")
        need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
        missing = sorted(need - fields)
        if missing:
            if ctr["mode"] == "兼容":
                batch_id = insert_batch(c, operation="正式入站", contract=ctr, track=OFFICIAL_TRACK,
                                        subject=body.subject, payload_fields=body.payload_fields,
                                        result="缓冲", detail="缺字段:" + ",".join(missing))
                c.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (batch_id, "缺字段进双写缓冲"))
                return {"result":"缓冲", "version": ctr["version"], "missing": missing, "batch_id": batch_id}
            batch_id = insert_batch(c, operation="正式入站", contract=ctr, track=OFFICIAL_TRACK,
                                    subject=body.subject, payload_fields=body.payload_fields,
                                    result="拒收", detail="缺字段:" + ",".join(missing))
            return {"result":"拒收", "version": ctr["version"], "missing": missing, "batch_id": batch_id}
        batch_id = insert_batch(c, operation="正式入站", contract=ctr, track=OFFICIAL_TRACK,
                                subject=body.subject, payload_fields=body.payload_fields,
                                result="通过", detail="字段齐全")
        return {"result":"通过", "version": ctr["version"], "batch_id": batch_id}

class ShadowIn(BaseModel):
    subject: str
    payload_fields: str

@app.post("/api/shadow/refresh")
def shadow_refresh(body: ShadowIn, authorization: str | None = Header(default=None)):
    """影子试跑：只落影子轨道，正式对账永不读取该轨道的行。"""
    require_user(authorization)
    with calculation_lock("影子刷新"):
        fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
        with connect() as c:
            ctr = c.execute("SELECT * FROM contracts WHERE subject=? AND track=? AND status='试跑' ORDER BY id DESC LIMIT 1",
                            (body.subject, SHADOW_TRACK)).fetchone()
            if not ctr:
                raise HTTPException(404, "无试跑影子契约")
            need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
            missing = sorted(need - fields)
            result = "拒收" if missing else "通过"
            detail = "缺字段:" + ",".join(missing) if missing else "影子字段齐全"
            batch_id = insert_batch(c, operation="影子刷新", contract=ctr, track=SHADOW_TRACK,
                                    subject=body.subject, payload_fields=body.payload_fields,
                                    result=result, detail=detail)
            # 影子批次只进影子表，绝不写正式 buffers；也绝不改已拒收批次。
            return {"result": result, "version": ctr["version"], "missing": missing,
                    "batch_id": batch_id, "track": SHADOW_TRACK}

@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM ingress_batches WHERE track=? ORDER BY id DESC", (OFFICIAL_TRACK,)).fetchall()
        return {"items":[dict(r) for r in rows], "shadow_excluded": True}

@app.get("/api/shadow/queue")
def shadow_queue(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM ingress_batches WHERE track=? ORDER BY id DESC", (SHADOW_TRACK,)).fetchall()
    items = [dict(r) for r in rows]
    return {"shadow_total": len(items),
            "shadow_passed": sum(1 for r in items if r["result"] == "通过"),
            "shadow_rejected": sum(1 for r in items if r["result"] == "拒收"),
            "items": items}

@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields FROM buffers b
                            JOIN ingress_batches i ON i.id=b.batch_id
                            WHERE i.track=? ORDER BY b.id DESC""", (OFFICIAL_TRACK,)).fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    """正式对账：现场带轨道排除条件重算，禁止读影子行，也不物化快照。"""
    require_user(authorization)
    with calculation_lock("正式对账"):
        with connect() as c:
            total = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE track=?", (OFFICIAL_TRACK,)).fetchone()["c"]
            passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE track=? AND result='通过'", (OFFICIAL_TRACK,)).fetchone()["c"]
            buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE track=? AND result='缓冲'", (OFFICIAL_TRACK,)).fetchone()["c"]
            rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE track=? AND result='拒收'", (OFFICIAL_TRACK,)).fetchone()["c"]
            buf_rows = c.execute("""SELECT COUNT(*) c FROM buffers b
                                    JOIN ingress_batches i ON i.id=b.batch_id
                                    WHERE i.track=?""", (OFFICIAL_TRACK,)).fetchone()["c"]
            contention = _contention_count(c)
        return {"ingress_total": total, "passed": passed, "buffered": buffered, "rejected": rejected,
                "buffer_rows": buf_rows, "aligned": buffered == buf_rows,
                "contention": contention, "track": OFFICIAL_TRACK}

@app.get("/api/policy")
def policy(authorization: str | None = Header(default=None)):
    """策略附调页：只汇总正式轨道。"""
    require_user(authorization)
    with connect() as c:
        official_total = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE track=?", (OFFICIAL_TRACK,)).fetchone()["c"]
        contracts = c.execute("SELECT COUNT(*) c FROM contracts WHERE track=?", (OFFICIAL_TRACK,)).fetchone()["c"]
    return {"official_total": official_total, "official_contracts": contracts,
            "track": OFFICIAL_TRACK}

@app.get("/api/review")
def review(authorization: str | None = Header(default=None)):
    """复核卡片：只吃正式轨道计数。"""
    require_user(authorization)
    with connect() as c:
        official_total = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE track=?", (OFFICIAL_TRACK,)).fetchone()["c"]
        passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE track=? AND result='通过'", (OFFICIAL_TRACK,)).fetchone()["c"]
        rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE track=? AND result='拒收'", (OFFICIAL_TRACK,)).fetchone()["c"]
    return {"official_total": official_total, "passed": passed, "rejected": rejected}

@app.get("/api/leak/deviations")
def leak_deviations(authorization: str | None = Header(default=None)):
    """泄漏偏了：任何想绕开隔离写路径的尝试都记在这里。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM leak_deviations ORDER BY id DESC").fetchall()
        contention = c.execute("SELECT * FROM calculation_contention ORDER BY id DESC").fetchall()
    return {"deviations": [dict(r) for r in rows],
            "contention": [dict(r) for r in contention]}

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
