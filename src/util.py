# -*- coding: utf-8 -*-
"""公共基础设施。

对应用户硬要求的设计原则：
1. 每条数据都带两个时间戳：source_ts（数据源返回的真实时间）与 fetched_at_ms（本机抓取时刻）。
   程序只信任 source_ts 做"数据是否实时"的判断，绝不把旧数据当新数据用。
2. 启动及每小时与 OKX 服务器时间对表；漂移超过阈值即告警，避免用错时间做交易。
3. 数据采集失败必须显式失败（返回 None 并记录原因），绝不静默用上一次的值顶替。
"""
from __future__ import annotations

import csv
import gzip
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
DATA_DIR = ROOT / "data"
LOG_DIR = ROOT / "logs"
CONFIG_DIR = ROOT / "config"
WEB_DIR = ROOT / "web"
STATE_DIR = ROOT / "state"
for _d in (DATA_DIR, LOG_DIR, CONFIG_DIR, WEB_DIR, STATE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

CONFIG_PATH = ROOT / "config.json"
THRESHOLDS_PATH = CONFIG_DIR / "thresholds.json"
RUN_LOG = LOG_DIR / "run.log"

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) sol-signal/1.0 (local decision-support)"
OKX_BASE = "https://www.okx.com"
SSL_CTX = ssl.create_default_context()

_server_offset_ms = 0
_server_time_ok = False

# ----------------------------------------------------------------------------- time


def now_ms() -> int:
    """本机 UTC 毫秒时间戳。"""
    return int(time.time() * 1000)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ms_to_iso(ms) -> str:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).isoformat(timespec="seconds")


def iso_to_ms(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp() * 1000)


def iso_ms(ms: int) -> str:
    """毫秒时间戳 → TradingView 需要的 ISO 串（带 .000Z）。"""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def hhmm_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%m-%d %H:%M:%S")


def server_now_ms() -> int:
    """经服务器校时后的当前时间（用于判断数据新鲜度）。"""
    return now_ms() + _server_offset_ms


def server_time_status() -> dict:
    return {
        "synced": _server_time_ok,
        "offset_ms": _server_offset_ms,
        "local_utc": utc_now().isoformat(timespec="seconds"),
        "server_utc": ms_to_iso(server_now_ms()),
    }


def sync_server_time() -> dict:
    """与 OKX 服务器对表。返回漂移信息。"""
    global _server_offset_ms, _server_time_ok
    data = okx("/api/v5/public/time", timeout=10, retries=2)
    server_ms = int(data[0]["ts"])
    local_ms = now_ms()
    _server_offset_ms = server_ms - local_ms
    _server_time_ok = True
    drift = abs(_server_offset_ms)
    if drift > 5000:
        log(f"[告警] 本机时间与OKX服务器漂移 {drift} ms，超过5秒，交易时间判断可能失真", "WARN")
    return {
        "server_ms": server_ms,
        "local_ms": local_ms,
        "offset_ms": _server_offset_ms,
        "server_utc": ms_to_iso(server_ms),
        "drift_ok": drift <= 5000,
    }


# ----------------------------------------------------------------------------- http


class DataError(RuntimeError):
    """数据获取失败（区别于程序bug）。"""


def http_request(url: str, *, method: str = "GET", body=None, timeout: int = 15,
                 retries: int = 3, headers: dict | None = None) -> bytes:
    last_err = None
    hdrs = {"User-Agent": UA, "Accept": "application/json,text/plain,*/*"}
    if headers:
        hdrs.update(headers)
    data = None
    if body is not None:
        data = body if isinstance(body, bytes) else json.dumps(body).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
            with urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX) as resp:
                raw = resp.read()
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                return raw
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", "replace")[:300]
            except Exception:
                pass
            last_err = f"HTTP {e.code} {e.reason} {detail}"
            if e.code == 429:          # 限流：退避后重试（财经日历等免费源常见）
                if attempt < retries:
                    time.sleep(6.0 * attempt)
                    continue
            if e.code in (400, 401, 403, 404, 405, 451):
                break
        except Exception as e:  # noqa: BLE001
            last_err = f"{type(e).__name__}: {e}"
        if attempt < retries:
            time.sleep(0.6 * attempt)
    raise DataError(f"{url} -> {last_err}")


def http_json(url: str, **kw):
    raw = http_request(url, **kw)
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:  # noqa: BLE001
        raise DataError(f"{url} -> 非JSON响应: {e}") from e


def okx(path: str, params: dict | None = None, *, timeout: int = 15, retries: int = 3):
    """调用 OKX 公开接口，返回 data 列表。code!=0 直接抛错（不静默）。"""
    url = OKX_BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    payload = http_json(url, timeout=timeout, retries=retries)
    code = str(payload.get("code"))
    if code != "0":
        raise DataError(f"OKX {path} code={code} msg={payload.get('msg')}")
    return payload.get("data", [])


# ----------------------------------------------------------------------------- files


def write_json(path, obj) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)
    os.replace(tmp, path)


def read_json(path, default=None):
    path = Path(path)
    if not path.exists():
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return default


def append_csv(path, row: dict, field_order: list | None = None) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    new = not path.exists()
    fields = field_order or list(row.keys())
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if new:
            w.writeheader()
        w.writerow(row)


def read_csv(path) -> list[dict]:
    path = Path(path)
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def load_config() -> dict:
    cfg = read_json(CONFIG_PATH)
    if not cfg:
        raise DataError(f"找不到配置文件 {CONFIG_PATH}")
    return cfg


def load_thresholds() -> dict:
    return read_json(THRESHOLDS_PATH, {}) or {}


CACHE_DIR = DATA_DIR / "cache"


def cached_payload(cache_name: str, ttl_s: float, fetcher):
    """带磁盘缓存的抓取。

    用于低频、易被限流的源（如财经日历、CoinGecko）。返回 (数据, 抓取时刻ms, 是否来自缓存)。
    抓取失败时若存在旧缓存则兜底，并在日志中明确标注使用的是哪个时刻的缓存。
    注意：绝不把缓存伪装成实时——调用方必须把 fetch_ms 作为 source_ts 并如实标记 cached。
    """
    path = CACHE_DIR / cache_name
    cur = read_json(path)
    if cur and (now_ms() - int(cur.get("fetched_at_ms", 0))) < ttl_s * 1000:
        return cur["data"], int(cur["fetched_at_ms"]), True
    try:
        data = fetcher()
        write_json(path, {"fetched_at_ms": now_ms(), "data": data})
        return data, now_ms(), False
    except Exception as e:  # noqa: BLE001
        if cur:
            log(f"[缓存兜底] {cache_name} 抓取失败({e})，改用 {ms_to_iso(cur['fetched_at_ms'])} 的缓存", "WARN")
            return cur["data"], int(cur["fetched_at_ms"]), True
        raise


# ----------------------------------------------------------------------------- log


def log(msg: str, level: str = "INFO") -> None:
    line = f"{utc_now().strftime('%Y-%m-%d %H:%M:%S')}Z [{level}] {msg}"
    print(line, flush=True)
    try:
        with open(RUN_LOG, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:  # noqa: BLE001
        pass


def is_stale(source_ts_ms, max_age_s: float) -> tuple[bool, float]:
    """判断数据是否过期。返回 (是否过期, 实际年龄秒)。"""
    if source_ts_ms is None:
        return True, float("inf")
    age = (server_now_ms() - int(source_ts_ms)) / 1000.0
    return age > max_age_s, age


def _env_ok(dataset: str, data, source_ts=None, instId: str | None = None) -> dict:
    """成功信封。"""
    return {
        "dataset": dataset, "instId": instId, "source_ts": source_ts,
        "source_utc": ms_to_iso(source_ts) if source_ts else None,
        "fetched_at_ms": now_ms(), "ok": True, "error": None, "data": data,
    }


def safe_call(dataset: str, fn, inst_id: str | None = None) -> dict:
    """执行采集函数；失败时返回失败信封并记录日志，绝不抛出中断主循环。"""
    try:
        env = fn()
        if env is None:
            raise DataError("返回空")
        return env
    except Exception as e:  # noqa: BLE001
        log(f"[数据采集失败] {dataset}: {e}", "WARN")
        return {
            "dataset": dataset, "instId": inst_id, "source_ts": None, "source_utc": None,
            "fetched_at_ms": now_ms(), "ok": False, "error": f"{type(e).__name__}: {e}",
            "data": None,
        }
