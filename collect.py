#!/usr/bin/env python3
"""Token-holder collector.

Python port of CSVFallbackDownload.ps1. Same tokens, free public sources,
CSV columns and UTF-8 BOM. Writes under data/ and keeps the BSC holder
cache in state/ so later runs only scan new blocks.

BSC log endpoints follow the script (publicnode, then nownodes). Pocket is
also in the script's RPC list; it is used only as a log fallback, and only
at the block-range size that endpoint currently allows, because larger
ranges are rejected or inconsistent. On HTTP 429 the request moves to the
next RPC immediately.
"""

from __future__ import annotations

import argparse
import base64
import json
import math
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

CN = ZoneInfo("Asia/Shanghai")
ROOT = Path(__file__).resolve().parent

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)
RPC_UA = "CSVFallback/1.0 (WindowsPowerShell)"

TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
ZERO_ADDR = "0x0000000000000000000000000000000000000000"
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
B58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
TOKEN_PROGRAMS = {
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
}

TOKENS = [
    {
        "folder": "ben",
        "chain": "bsc",
        "address": "0x3030a6d0b748c02e5543cad9200D715c7Bfb7777",
        "created_block": 115310671,
    },
    {"folder": "WOULD", "chain": "sol", "address": "J1Wpmugrooj1yMyQKrdZ2vwRXG5rhfx3vTnYE39gpump"},
    {"folder": "GIGACHAD", "chain": "sol", "address": "63LfDmNb3MQ8mw9MtZ2To9bEA2M71kZUUGq5tiJxcqj9"},
    {"folder": "Catecoin", "chain": "sol", "address": "Ai66LHZG9MCzg1WKdawwqduVAXpNDUuV8M3uyq5ppump"},
    {"folder": "The Black Bull", "chain": "sol", "address": "9cRCn9rGT8V2imeM2BaKs13yhMEais3ruM3rPvTGpump"},
    {"folder": "ni hao cat", "chain": "sol", "address": "4CxjZyMNoF1p6eXHXHv1TWpS2X47CuUmLwtGe18spump"},
]

SOL_RPCS = [
    "https://api.mainnet-beta.solana.com",
    "https://public.rpc.solanavibestation.com",
]

# General BSC calls (block number, Multicall3, balanceOf). Same list as the script.
BSC_RPCS = [
    "https://bsc-dataseed.bnbchain.org",
    "https://bsc-dataseed1.bnbchain.org",
    "https://bsc-dataseed2.bnbchain.org",
    "https://bsc-dataseed1.binance.org",
    "https://bsc-dataseed2.binance.org",
    "https://bsc-dataseed3.binance.org",
    "https://bsc-dataseed4.binance.org",
    "https://bsc.publicnode.com",
    "https://bsc-rpc.publicnode.com",
    "https://1rpc.io/bnb",
    "https://public-bsc.nownodes.io",
    "https://bsc.api.pocket.network",
]

# eth_getLogs. Publicnode is first, as in the script, but it now refuses
# archive ranges ("personal token" required). Pocket is already in BSC_RPCS;
# it is placed ahead of the slow nownodes archive for ranges publicnode will
# not serve. getLogs still rotates through every URL on failure.
BSC_LOG_PUBLICNODE = [
    "https://bsc.publicnode.com",
    "https://bsc-rpc.publicnode.com",
]
BSC_LOG_POCKET = "https://bsc.api.pocket.network"
BSC_LOG_NOWNODES = "https://public-bsc.nownodes.io"

DEX_PAIRS = {
    "ben": "0x103f0e8cac08d41afe09a4445587054f80f05fec",
    "WOULD": "Hga48QXtpCgLSTsfysDirPJzq8aoBPjvePUgmXhFGDro",
    "GIGACHAD": "4xxM4cdb6MEsCxM52xvYqkNbzvdeWWsPDZrBcTqVGUar",
    "Catecoin": "HMzvsEEmtzHhvZNw9uwbaG85HCTmFnkbhzUx16cy7ca3",
    "The Black Bull": "FnzKY6x7entQ1eR3D225dQyT7ybfka4PskBMQhb8L3CC",
    "ni hao cat": "Ho1aSG6WGH1FfXdEHrK2f4Ggt1Hoxo9s5jcLMDqeyeBz",
}
DEX_CHAINS = {"ben": "bsc"}
DEX_FOLDERS = ["ben", "WOULD", "GIGACHAD", "Catecoin", "The Black Bull", "ni hao cat"]

BLOCKS_PER_DAY = 192000

_log_lock = threading.RLock()
_once_keys: set[str] = set()
_tls = threading.local()
_bsc_lock = threading.Lock()
_bsc_index = 0
LOG_PATH: Path | None = None
PUBLICNODE_MIN = 10**18


class RpcError(Exception):
    def __init__(self, message: str, status: int = 0):
        super().__init__(message)
        self.status = status


def china_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(CN)


def write_log(msg: str) -> None:
    line = f"{china_now().strftime('%Y-%m-%d %H:%M:%S')} CST  {msg}"
    with _log_lock:
        print(line, flush=True)
        if LOG_PATH is None:
            return
        try:
            LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            new = (not LOG_PATH.exists()) or LOG_PATH.stat().st_size == 0
            with LOG_PATH.open("a", encoding="utf-8-sig" if new else "utf-8", newline="") as fh:
                fh.write(line + "\r\n")
        except OSError:
            pass


def log_once(key: str, msg: str) -> None:
    with _log_lock:
        if key in _once_keys:
            return
        _once_keys.add(key)
    write_log(msg)


def get_session() -> requests.Session:
    sess = getattr(_tls, "session", None)
    if sess is None:
        sess = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=8, pool_maxsize=8, max_retries=0)
        sess.mount("https://", adapter)
        sess.mount("http://", adapter)
        _tls.session = sess
    return sess


def save_text_atomic(path: Path, text: str, bom: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = Path(str(path) + ".part")
    encoding = "utf-8-sig" if bom else "utf-8"
    with tmp.open("w", encoding=encoding, newline="") as fh:
        fh.write(text)
    os.replace(tmp, path)


def save_csv(path: Path, text: str) -> None:
    save_text_atomic(path, text, bom=True)


def parse_snapshot_time(name: str) -> datetime | None:
    m = re.search(r"(\d{4}-\d{2}-\d{2})_(\d{4})", name)
    if m:
        dt = datetime.strptime(m.group(1) + m.group(2), "%Y-%m-%d%H%M")
        return dt.replace(tzinfo=CN)
    m = re.search(r"_(\d{13})\.csv$", name)
    if m:
        return datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc)
    return None


def have_recent(directory: Path, minutes: int = 30) -> bool:
    """Skip when a snapshot from the last `minutes` is already in the folder.

    The timestamp encoded in the filename is the snapshot time. Git checkout
    rewrites mtimes, so filename time is what keeps the 30-minute guard
    correct on GitHub Actions. Files without a timestamp fall back to mtime.
    """
    if not directory.is_dir():
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    for entry in directory.iterdir():
        if not entry.is_file() or entry.suffix.lower() != ".csv":
            continue
        try:
            if entry.stat().st_size < 100:
                continue
        except OSError:
            continue
        stamp = parse_snapshot_time(entry.name)
        if stamp is None:
            stamp = datetime.fromtimestamp(entry.stat().st_mtime, tz=timezone.utc)
        if stamp.astimezone(timezone.utc) > cutoff:
            return True
    return False


def retry(action, what: str, tries: int = 3):
    last: Exception | None = None
    for i in range(1, tries + 1):
        try:
            return action()
        except Exception as exc:  # noqa: BLE001 - same as the script: log and retry
            last = exc
            write_log(f"  {what}: attempt {i}/{tries} failed: {exc}")
            if i < tries:
                time.sleep(5 * i)
    raise RuntimeError(f"{what} failed after {tries} attempts") from last


def post_json(url: str, body: str, timeout: int):
    headers = {
        "Content-Type": "application/json",
        "User-Agent": RPC_UA,
        "Accept": "application/json",
    }
    try:
        resp = get_session().post(url, data=body.encode("utf-8"), headers=headers, timeout=timeout)
    except requests.RequestException as exc:
        raise RpcError(f"{type(exc).__name__}: {exc}") from exc
    if resp.status_code >= 400:
        raise RpcError(f"HTTP {resp.status_code} from {url}: {resp.text[:300]}", resp.status_code)
    try:
        return resp.json()
    except ValueError as exc:
        raise RpcError(f"bad JSON from {url}: {resp.text[:200]}") from exc


def rpc_error_of(payload):
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and item.get("error"):
                return item["error"]
        return None
    if isinstance(payload, dict) and payload.get("error"):
        return payload["error"]
    return None


def bsc_rotate(body: str, timeout: int, what: str, urls: list[str] | None = None):
    global _bsc_index
    pool = urls or BSC_RPCS
    n = len(pool)
    last = ""
    for round_i in range(3):
        for i in range(n):
            with _bsc_lock:
                idx = (_bsc_index + i) % n
            url = pool[idx]
            try:
                payload = post_json(url, body, timeout)
                err = rpc_error_of(payload)
                if err is not None:
                    raise RpcError(
                        f"RPC error from {url}: {json.dumps(err, separators=(',', ':'))}"
                    )
                with _bsc_lock:
                    _bsc_index = idx
                return url, payload
            except Exception as exc:  # noqa: BLE001 - rotate on every failure, including HTTP 429
                last = str(exc)
                write_log(f"  ben: {what} via {url} failed: {last}")
                with _bsc_lock:
                    _bsc_index = (idx + 1) % n
        if round_i < 2:
            time.sleep(2 * (round_i + 1))
    raise RpcError(f"{what} failed after trying all free BSC RPCs: {last}")


def bsc_call(method: str, params_json: str, timeout: int = 120, urls: list[str] | None = None):
    body = '{"jsonrpc":"2.0","id":1,"method":"' + method + '","params":' + params_json + "}"
    return bsc_rotate(body, timeout, method, urls)


def hex_pad(value: str, nbytes: int) -> str:
    h = value.lower()
    if h.startswith("0x"):
        h = h[2:]
    if len(h) > nbytes * 2:
        raise ValueError(f"hex longer than {nbytes} bytes: {value}")
    return h.rjust(nbytes * 2, "0")


def multicall_aggregate(token: str, addresses: list[str]) -> str:
    """ABI-encode Multicall3.aggregate((address,bytes)[]) of balanceOf(address)."""
    n = len(addresses)
    parts = ["0x252dba42", hex_pad("20", 32), hex_pad(format(n, "x"), 32)]
    tuple_size = 160
    first_tuple = n * 32
    for i in range(n):
        parts.append(hex_pad(format(first_tuple + i * tuple_size, "x"), 32))
    token_pad = hex_pad(token, 32)
    for addr in addresses:
        parts.append(token_pad)
        parts.append(hex_pad("40", 32))
        parts.append(hex_pad("24", 32))
        parts.append("70a08231")
        parts.append(hex_pad(addr, 32))
        parts.append("00" * 28)
    return "".join(parts)


def decode_multicall(result_hex: str, expected: int) -> list[str]:
    h = result_hex.lower()
    if h.startswith("0x"):
        h = h[2:]
    if len(h) < 192:
        raise RpcError(f"Multicall3 result too short ({len(h)} hex chars)")
    arr_off = int(h[64:128], 16)
    arr_start = arr_off * 2
    if arr_start + 64 > len(h):
        raise RpcError("Multicall3 result missing array")
    n = int(h[arr_start:arr_start + 64], 16)
    if n != expected:
        raise RpcError(f"Multicall3 returned {n} of {expected}")
    values: list[str] = []
    for i in range(n):
        word = h[arr_start + 64 + i * 64:arr_start + 64 + i * 64 + 64]
        elem_off = int(word, 16)
        elem_pos = arr_start + 64 + elem_off * 2
        length = int(h[elem_pos:elem_pos + 64], 16)
        data = h[elem_pos + 64:elem_pos + 64 + length * 2]
        values.append(data)
    return values


def bsc_balances_batch(contract: str, addresses: list[str]) -> list[str]:
    values: list[str] = []
    chunk = 200
    for start in range(0, len(addresses), chunk):
        sub = addresses[start:start + chunk]
        calls = []
        for k, addr in enumerate(sub):
            data = "0x70a08231" + ("0" * 24) + addr[2:]
            calls.append(
                {
                    "jsonrpc": "2.0",
                    "id": k,
                    "method": "eth_call",
                    "params": [{"to": contract, "data": data}, "latest"],
                }
            )
        body = json.dumps(calls, separators=(",", ":"))
        _url, payload = bsc_rotate(body, 90, "balanceOf batch")
        rows = payload if isinstance(payload, list) else [payload]
        if len(rows) != len(sub):
            raise RpcError(f"batch returned {len(rows)} of {len(sub)}")
        by_id: dict[int, str] = {}
        for item in rows:
            if not isinstance(item, dict) or item.get("result") is None:
                err = item.get("error") if isinstance(item, dict) else item
                raise RpcError("batch item error: " + json.dumps(err, separators=(",", ":")))
            by_id[int(item["id"])] = str(item["result"]).removeprefix("0x").removeprefix("0X")
        for k in range(len(sub)):
            if k not in by_id:
                raise RpcError(f"batch item {k} missing")
            values.append(by_id[k])
    return values


def bsc_balances(contract: str, addresses: list[str]) -> tuple[list[str], str]:
    sizes = (500, 250, 100)
    for si, size in enumerate(sizes):
        if size > len(addresses) and si != 0:
            continue
        values: list[str] = []
        ok = True
        for start in range(0, len(addresses), size):
            sub = addresses[start:start + size]
            try:
                data = multicall_aggregate(contract, sub)
                params = json.dumps(
                    [{"to": MULTICALL3, "data": data}, "latest"],
                    separators=(",", ":"),
                )
                _url, payload = bsc_call("eth_call", params, 90)
                values.extend(decode_multicall(payload["result"], len(sub)))
            except Exception as exc:  # noqa: BLE001
                write_log(f"  ben: Multicall3 x{len(sub)} via current RPC failed: {exc}")
                ok = False
                break
        if ok and len(values) == len(addresses):
            return values, f"Multicall3 x{min(size, len(addresses))}"
    write_log("  ben: falling back to JSON-RPC batch balanceOf()")
    return bsc_balances_batch(contract, addresses), "JSON-RPC batch"


def format_units(raw: int, decimals: int, thousands: bool = False) -> str:
    text = str(raw).rjust(decimals + 1, "0")
    if decimals:
        int_part, frac = text[:-decimals], text[-decimals:]
    else:
        int_part, frac = text, ""
    if thousands:
        int_part = f"{int(int_part):,}"
    frac = frac.rstrip("0")
    if frac:
        return f"{int_part}.{frac}"
    return int_part


def b58encode(data: bytes) -> str:
    number = int.from_bytes(data, "big")
    chars: list[str] = []
    while number > 0:
        number, rem = divmod(number, 58)
        chars.append(B58_ALPHABET[rem])
    pad = 0
    for byte in data:
        if byte == 0:
            pad += 1
        else:
            break
    encoded = "".join(reversed(chars))
    return ("1" * pad) + encoded


def fmt_n1(seconds: float) -> str:
    return f"{seconds:,.1f}"


def load_address_state(path: Path) -> tuple[int, list[str]]:
    with path.open(encoding="utf-8-sig") as fh:
        payload = json.load(fh)
    addresses = payload.get("addresses") or []
    if isinstance(addresses, str):
        addresses = [addresses]
    return int(payload["end"]), [str(a).lower() for a in addresses]


def save_address_state(path: Path, end_block: int, contract: str, addresses: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    body = {
        "end": end_block,
        "contract": contract,
        "addresses": addresses,
    }
    tmp = Path(str(path) + ".part")
    with tmp.open("w", encoding="utf-8", newline="") as fh:
        json.dump(body, fh, separators=(",", ":"))
        fh.write("\n")
    os.replace(tmp, path)


def topic_address(topic: str) -> str:
    if not isinstance(topic, str) or len(topic) < 42:
        raise RpcError(f"bad transfer topic {topic!r}")
    hex_part = topic[2:] if topic[:2].lower() == "0x" else topic
    return "0x" + hex_part[-40:].lower()


def log_urls_for(start_block: int) -> list[str]:
    if start_block >= PUBLICNODE_MIN:
        return BSC_LOG_PUBLICNODE + [BSC_LOG_POCKET, BSC_LOG_NOWNODES]
    return [BSC_LOG_POCKET, BSC_LOG_NOWNODES] + BSC_LOG_PUBLICNODE


_RANGE_LIMIT_RE = re.compile(
    r"maximum(?:\s+block\s+range)?\s+(\d+)|limited to\s+\d+\s*-\s*(\d+)\s+blocks|over\s+(\d+)\s+blocks",
    re.IGNORECASE,
)


def range_limit_from_error(message: str) -> int | None:
    match = _RANGE_LIMIT_RE.search(message)
    if not match:
        return None
    for group in match.groups():
        if group:
            return int(group)
    return None


def rpc_get_logs(url: str, contract: str, start_block: int, end_block: int, timeout: int = 90):
    params = [
        {
            "address": contract,
            "topics": [TRANSFER_TOPIC],
            "fromBlock": hex(start_block),
            "toBlock": hex(end_block),
        }
    ]
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": "eth_getLogs", "params": params},
        separators=(",", ":"),
    )
    payload = post_json(url, body, timeout)
    err = rpc_error_of(payload)
    if err is not None:
        raise RpcError(f"RPC error from {url}: {json.dumps(err, separators=(',', ':'))}")
    result = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(result, list):
        raise RpcError(f"RPC missing log list from {url}")
    return result


def fetch_logs(contract: str, start_block: int, end_block: int, depth: int = 0) -> list:
    """eth_getLogs with the script's 5000-block split, 429 rotation, and bisection.

    When an endpoint answers with an explicit max range, that range is split
    to the advertised size and retried (pocket sometimes allows 5000 and
    sometimes only 500). A hard failure below 200 blocks raises, as the script does.
    """
    if end_block < start_block:
        return []
    if depth > 40:
        raise RpcError(f"log scan recursion at blocks {start_block}-{end_block}")
    if (end_block - start_block) >= 5000:
        logs: list = []
        block = start_block
        while block <= end_block:
            stop = min(block + 4999, end_block)
            logs.extend(fetch_logs(contract, block, stop, depth + 1))
            block = stop + 1
        return logs

    urls = log_urls_for(start_block)
    last: Exception | None = None
    span = end_block - start_block + 1
    for url in urls:
        attempts = 2
        attempt = 0
        while attempt < attempts:
            attempt += 1
            try:
                return rpc_get_logs(url, contract, start_block, end_block)
            except Exception as exc:  # noqa: BLE001
                last = exc
                limit = range_limit_from_error(str(exc))
                status = exc.status if isinstance(exc, RpcError) else 0
                if limit and limit >= 1 and span > limit:
                    log_once(
                        f"limit:{url}:{limit}",
                        f"  ben: {url} allows at most {limit} blocks per eth_getLogs, splitting",
                    )
                    logs = []
                    block = start_block
                    while block <= end_block:
                        stop = min(block + limit - 1, end_block)
                        logs.extend(fetch_logs(contract, block, stop, depth + 1))
                        block = stop + 1
                    return logs
                # HTTP 429: leave this host immediately. Other errors get one retry.
                if status == 429:
                    log_once(f"429:{url}", f"  ben: HTTP 429 from {url}, switching RPC")
                    break
                if attempt < attempts:
                    continue
                write_log(f"  ben: eth_getLogs {start_block}-{end_block} via {url} failed: {exc}")
                break
    if (end_block - start_block) < 200:
        raise RpcError(f"log scan failed at blocks {start_block}-{end_block}: {last}")
    mid = (start_block + end_block) // 2
    left = fetch_logs(contract, start_block, mid, depth + 1)
    right = fetch_logs(contract, mid + 1, end_block, depth + 1)
    return left + right


def extract_transfer_addresses(logs: list) -> tuple[int, set[str]]:
    found: set[str] = set()
    counted = 0
    for entry in logs:
        topics = entry.get("topics") if isinstance(entry, dict) else None
        if not topics or len(topics) < 3:
            continue
        counted += 1
        for topic in (topics[1], topics[2]):
            addr = topic_address(topic)
            if addr != ZERO_ADDR:
                found.add(addr)
    return counted, found


def probe_publicnode_min(created_block: int, head: int) -> int:
    """Lowest recent block publicnode will serve without an archive token."""

    def serves(block: int) -> bool:
        url = BSC_LOG_PUBLICNODE[0]
        for attempt in range(3):
            try:
                rpc_get_logs(url, TOKENS[0]["address"].lower(), block, block, timeout=20)
                return True
            except Exception as exc:  # noqa: BLE001
                message = str(exc)
                if "Archive" in message or "personal token" in message or "403" in message:
                    return False
                time.sleep(0.4 * (attempt + 1))
        return False

    if head <= created_block:
        return head
    if not serves(max(created_block, head - 20)):
        write_log("  ben: publicnode eth_getLogs unavailable, using pocket/nownodes")
        return head + 1
    lo, hi = created_block, head - 20
    answer = hi
    while lo <= hi:
        mid = (lo + hi) // 2
        if serves(mid):
            answer = mid
            hi = mid - 1
        else:
            lo = mid + 1
    # Keep a margin so a rolling archive window does not flip mid-scan.
    safe = min(head, answer + 50000)
    write_log(f"  ben: publicnode eth_getLogs available from block {safe} (probed {answer})")
    return safe


def iter_chunks(start_block: int, end_block: int, size: int = 5000):
    block = start_block
    while block <= end_block:
        stop = min(block + size - 1, end_block)
        yield block, stop
        block = stop + 1


def discover_bsc_addresses(contract: str, start_block: int, end_block: int, addresses: set[str], state_path: Path, workers: int) -> tuple[int, int]:
    chunks = list(iter_chunks(start_block, end_block, 5000))
    if not chunks:
        return 0, 0
    done = [False] * len(chunks)
    prefix = 0
    index_by_start = {fr: i for i, (fr, _to) in enumerate(chunks)}
    log_count = 0
    new_count = 0
    started = time.time()
    last_checkpoint = started
    last_logged = 0

    def mark(start_at: int) -> int | None:
        nonlocal prefix
        done[index_by_start[start_at]] = True
        while prefix < len(done) and done[prefix]:
            prefix += 1
        if prefix == 0:
            return None
        return chunks[prefix - 1][1]

    def checkpoint(scanned_through: int) -> None:
        try:
            save_address_state(state_path, scanned_through, contract, sorted(addresses))
        except OSError as exc:
            write_log(f"  ben: warning, could not write cache file: {exc}")

    def work(span: tuple[int, int]):
        fr, to = span
        last: Exception | None = None
        for attempt in range(3):
            try:
                logs = fetch_logs(contract, fr, to)
                counted, found = extract_transfer_addresses(logs)
                return fr, to, counted, found
            except Exception as exc:  # noqa: BLE001
                last = exc
                time.sleep(1 + attempt)
        assert last is not None
        raise last

    write_log(
        f"  ben: scanning blocks {start_block}-{end_block} "
        f"({len(chunks)} x 5000-block requests, {workers} workers)"
    )
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = [pool.submit(work, span) for span in chunks]
        finished = 0
        for fut in as_completed(futures):
            try:
                fr, to, counted, found = fut.result()
            except Exception:
                if prefix > 0:
                    checkpoint(chunks[prefix - 1][1])
                raise
            fresh = found - addresses
            addresses.update(found)
            log_count += counted
            new_count += len(fresh)
            finished += 1
            scanned = mark(fr)
            now = time.time()
            if scanned is not None and (now - last_checkpoint >= 20 or finished == len(chunks)):
                checkpoint(scanned)
                last_checkpoint = now
            if finished - last_logged >= 100 or finished == len(chunks):
                elapsed = now - started
                rate = finished / elapsed if elapsed else 0
                remain = (len(chunks) - finished) / rate if rate else 0
                write_log(
                    f"  ben: progress {finished}/{len(chunks)} chunks, "
                    f"block {scanned or start_block}/{end_block}, "
                    f"addresses={len(addresses)}, logs={log_count}, "
                    f"eta {remain / 60:.0f} min"
                )
                last_logged = finished
    return log_count, new_count


def csv_seed_addresses(directory: Path) -> tuple[Path, list[str]] | None:
    if not directory.is_dir():
        return None
    candidates = []
    for entry in directory.iterdir():
        if not entry.is_file():
            continue
        name = entry.name
        if name.startswith("tokenholders-") or name.startswith("fallback_ben_"):
            candidates.append(entry)
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    chosen = candidates[0]
    found: list[str] = []
    with chosen.open(encoding="utf-8-sig", errors="replace") as fh:
        for line in fh:
            match = re.match(r'^"?(0x[0-9a-fA-F]{40})"?,,?', line) or re.match(
                r'^"?(0x[0-9a-fA-F]{40})"?,', line
            )
            if match:
                found.append(match.group(1).lower())
    return chosen, found


def get_bsc_holders(token: dict, base: Path, state_dir: Path, workers: int) -> dict:
    global PUBLICNODE_MIN
    contract = token["address"].lower()
    state_path = state_dir / f"bsc_{contract}.json"
    addresses: set[str] = set()
    head = int(bsc_call("eth_blockNumber", "[]", 30)[1]["result"], 16)
    end_block = head - 15
    created = int(token["created_block"])
    start_block: int

    if state_path.exists():
        saved_end, saved = load_address_state(state_path)
        addresses.update(saved)
        start_block = saved_end + 1
        write_log(f"  ben: cache has {len(addresses)} addresses up to block {saved_end}")
    else:
        seed = ROOT / "seed_ben.json"
        if seed.exists():
            saved_end, saved = load_address_state(seed)
            addresses.update(saved)
            start_block = saved_end + 1
            write_log(f"  ben: starting from seed file ({len(addresses)} addresses, block {saved_end})")
        else:
            previous = csv_seed_addresses(base / token["folder"])
            if previous:
                csv_path, saved = previous
                addresses.update(saved)
                age_days = math.ceil((time.time() - csv_path.stat().st_mtime) / 86400) + 2
                start_block = max(created, head - int(age_days * BLOCKS_PER_DAY))
                write_log(
                    f"  ben: starting from {csv_path.name} ({len(addresses)} addresses), "
                    f"rescanning {age_days} days"
                )
            else:
                start_block = created
                write_log(
                    "  ben: no cache/seed/old CSV - scanning full history from contract creation (very slow, hours)"
                )

    if start_block < created:
        start_block = created

    PUBLICNODE_MIN = probe_publicnode_min(created, head)
    sw_logs = time.time()
    log_count = 0
    new_count = 0
    if start_block <= end_block:
        before = len(addresses)
        log_count, new_count = discover_bsc_addresses(
            contract, start_block, end_block, addresses, state_path, workers
        )
        # new_count from the worker is exact; keep the script's wording.
        new_count = len(addresses) - before
    sw_logs_s = time.time() - sw_logs
    write_log(
        f"  ben: scanned blocks {start_block}-{end_block}: {log_count} transfer events, "
        f"{new_count} new addresses, {len(addresses)} total ({fmt_n1(sw_logs_s)}s)"
    )

    ordered = sorted(addresses)
    sw_bal = time.time()
    values, method = bsc_balances(contract, ordered)
    rows: list[tuple[str, int]] = []
    for addr, hex_val in zip(ordered, values):
        word = hex_val or "0"
        if word.startswith("0x") or word.startswith("0X"):
            word = word[2:]
        if not word:
            word = "0"
        balance = int(word, 16)
        if balance > 0:
            rows.append((addr, balance))
    bal_s = time.time() - sw_bal
    write_log(
        f"  ben: balances for {len(ordered)} addresses in {fmt_n1(bal_s)}s via {method}"
    )
    if not rows:
        raise RpcError("no holders with balance found")

    try:
        save_address_state(state_path, end_block, contract, sorted(addr for addr, _bal in rows))
    except OSError as exc:
        write_log(f"  ben: warning, could not update cache file: {exc}")

    rows.sort(key=lambda item: (-item[1], item[0]))
    lines = ['"HolderAddress","Balance","PendingBalanceUpdate"\n']
    for addr, balance in rows:
        lines.append(f'"{addr}","{format_units(balance, 18, thousands=True)}","No"\n')
    return {"text": "".join(lines), "rows": len(rows)}


def solscan_export(mint: str) -> dict:
    url = f"https://api-v2.solscan.io/v2/token/holder/export?address={mint}"
    headers = {
        "User-Agent": BROWSER_UA,
        "Accept": "text/csv,*/*",
        "Referer": "https://solscan.io/",
    }
    try:
        resp = get_session().get(url, headers=headers, timeout=120)
    except requests.RequestException as exc:
        raise RpcError(f"Solscan: {exc}") from exc
    if resp.status_code >= 400:
        raise RpcError(f"Solscan: HTTP {resp.status_code}")
    raw = resp.content
    if raw.startswith(b"\xef\xbb\xbf"):
        raw = raw[3:]
    text = raw.decode("utf-8")
    if not text.startswith("Account,Token Account,Quantity,Percentage"):
        snip = re.sub(r"\s+", " ", text[:80])
        raise RpcError(f"Solscan: not a CSV (probably Cloudflare check): {snip}")
    rows = len([line for line in text.split("\n") if line.strip()]) - 1
    if rows < 1:
        raise RpcError("Solscan: CSV has no rows")
    return {"text": text, "rows": rows}


def sol_rpc(url: str, method: str, params_json: str, timeout: int = 120):
    body = '{"jsonrpc":"2.0","id":1,"method":"' + method + '","params":' + params_json + "}"
    payload = post_json(url, body, timeout)
    err = rpc_error_of(payload)
    if err is not None:
        raise RpcError(f"RPC error from {url}: {json.dumps(err, separators=(',', ':'))}")
    if not isinstance(payload, dict) or "result" not in payload:
        raise RpcError(f"RPC missing result from {url}")
    return payload["result"]


def save_jsonrpc_to_file(url: str, method: str, params_json: str, out_file: Path, timeout: int) -> None:
    body = '{"jsonrpc":"2.0","id":1,"method":"' + method + '","params":' + params_json + "}"
    headers = {
        "Content-Type": "application/json",
        "User-Agent": RPC_UA,
        "Accept-Encoding": "gzip, deflate",
    }
    out_file.parent.mkdir(parents=True, exist_ok=True)
    with get_session().post(
        url, data=body.encode("utf-8"), headers=headers, timeout=timeout, stream=True
    ) as resp:
        if resp.status_code >= 400:
            detail = resp.text[:300]
            raise RpcError(f"HTTP {resp.status_code} from {url}: {detail}", resp.status_code)
        with out_file.open("wb") as fh:
            for chunk in resp.iter_content(1024 * 1024):
                if chunk:
                    fh.write(chunk)


def parse_program_accounts(path: Path):
    elem_rx = re.compile(r'\{"(?:pubkey|account)"(?:[^{}]|\{[^{}]*\})*\}')
    pk_rx = re.compile(r'"pubkey"\s*:\s*"([1-9A-HJ-NP-Za-km-z]+)"')
    data_rx = re.compile(r'"data"\s*:\s*\[\s*"([A-Za-z0-9+/=]*)"')
    amounts: list[int] = []
    pubkeys: list[str] = []
    datas: list[str] = []
    carry = ""
    saw_error = False
    first = True
    with path.open("r", encoding="utf-8", newline="") as fh:
        while True:
            buf = fh.read(4 * 1024 * 1024)
            if not buf:
                break
            blob = carry + buf
            if first:
                first = False
                if re.match(r'^\s*\{[^\[]*"error"', blob):
                    saw_error = True
                    carry = blob
                    break
            last = 0
            for match in elem_rx.finditer(blob):
                last = match.end()
                pk = pk_rx.search(match.group(0))
                data_match = data_rx.search(match.group(0))
                if not pk or not data_match:
                    continue
                raw = base64.b64decode(data_match.group(1))
                if len(raw) < 40:
                    continue
                amount = int.from_bytes(raw[32:40], "little")
                if amount == 0:
                    continue
                amounts.append(amount)
                pubkeys.append(pk.group(1))
                datas.append(data_match.group(1))
            carry = blob[last:]
            if len(carry) > 8 * 1024 * 1024:
                raise RpcError("parse error: response format not recognised")
    if saw_error or (not amounts and '"error"' in carry):
        raise RpcError(f"RPC error: {carry[:200]}")
    if not amounts:
        raise RpcError("RPC returned no holders")
    return amounts, pubkeys, datas


def sol_rpc_holders(mint: str, rpc: str, state_dir: Path) -> dict:
    info = sol_rpc(
        rpc,
        "getAccountInfo",
        '["' + mint + '",{"encoding":"base64","dataSlice":{"offset":0,"length":0}}]',
    )
    program = ((info or {}).get("value") or {}).get("owner")
    if program not in TOKEN_PROGRAMS:
        raise RpcError(f"unexpected mint owner {program}")
    supply_info = sol_rpc(rpc, "getTokenSupply", '["' + mint + '"]')
    decimals = int(supply_info["value"]["decimals"])
    supply = Decimal(str(supply_info["value"]["amount"]))
    params = (
        '["' + program + '",{"encoding":"base64","dataSlice":{"offset":32,"length":40},'
        '"filters":[{"memcmp":{"offset":0,"bytes":"' + mint + '"}}]}]'
    )
    outfile = state_dir / f"gpa_{mint}.json"
    try:
        save_jsonrpc_to_file(rpc, "getProgramAccounts", params, outfile, 600)
        amounts, pubkeys, datas = parse_program_accounts(outfile)
    finally:
        try:
            outfile.unlink()
        except OSError:
            pass
    order = sorted(range(len(amounts)), key=lambda i: amounts[i])  # ascending, read from the end
    take = min(1000, len(order))
    lines = ["Account,Token Account,Quantity,Percentage\n"]
    for k in range(take):
        i = order[len(order) - 1 - k]
        amount = amounts[i]
        raw = base64.b64decode(datas[i])
        owner = b58encode(raw[:32])
        qty = format_units(amount, decimals)
        pct = (Decimal(amount) * Decimal(100) / supply).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        lines.append(f"{owner},{pubkeys[i]},{qty},{pct:.2f}\n")
    return {"text": "".join(lines), "rows": take, "total": len(amounts)}


def fmt_liquidity(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    number = Decimal(str(value))
    text = format(number, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def dexscreener_snapshot(folder: str, dstr: str, base: Path, force: bool) -> None:
    out_dir = base / folder / "dexscreener"
    out_path = out_dir / f"dexscreener_{folder.replace(' ', '_')}_{dstr}.csv"
    if not force and have_recent(out_dir):
        write_log(f"[{folder}] DexScreener: recent file already present (within 30 min) - skip")
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    pair = DEX_PAIRS[folder]
    chain = DEX_CHAINS.get(folder, "solana")
    url = f"https://api.dexscreener.com/latest/dex/pairs/{chain}/{pair}"

    def fetch():
        resp = get_session().get(url, headers={"User-Agent": BROWSER_UA}, timeout=30)
        if resp.status_code >= 400:
            raise RpcError(f"HTTP {resp.status_code}")
        return resp.json()

    payload = retry(fetch, f"[{folder}] DexScreener", 3)
    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    if not pairs:
        raise RpcError("DexScreener returned no pair")
    pair_info = pairs[0]
    base_symbol = (pair_info.get("baseToken") or {}).get("symbol") or ""
    quote_symbol = (pair_info.get("quoteToken") or {}).get("symbol") or ""
    liquidity = pair_info.get("liquidity") or {}
    header = [
        "time_china",
        "token",
        "pair_address",
        "dex",
        "PRICE USD",
        f"PRICE ({quote_symbol})",
        "LIQUIDITY (USD)",
        f"Pooled {base_symbol}",
        f"Pooled {quote_symbol}",
        "url",
    ]
    row = [
        china_now().strftime("%Y-%m-%d %H:%M"),
        base_symbol,
        pair,
        pair_info.get("dexId") or "",
        "" if pair_info.get("priceUsd") is None else str(pair_info.get("priceUsd")),
        "" if pair_info.get("priceNative") is None else str(pair_info.get("priceNative")),
        fmt_liquidity(liquidity.get("usd")),
        fmt_liquidity(liquidity.get("base")),
        fmt_liquidity(liquidity.get("quote")),
        pair_info.get("url") or "",
    ]
    text = ",".join(f'"{item}"' for item in header) + "\r\n"
    text += ",".join(f'"{item}"' for item in row) + "\r\n"
    with out_path.open("w", encoding="utf-8-sig", newline="") as fh:
        fh.write(text)
    write_log(f"[{folder}] DexScreener OK -> {out_path}")


def collect_token(token: dict, base: Path, state_dir: Path, dstr: str, force: bool, skip_solscan: bool, workers: int) -> None:
    folder = token["folder"]
    directory = base / folder
    if not force and have_recent(directory):
        write_log(f"[{folder}] recent file already present (within 30 min) - skip")
        return
    directory.mkdir(parents=True, exist_ok=True)
    result = None
    source = ""
    if token["chain"] == "sol":
        if not skip_solscan:
            try:
                result = retry(lambda: solscan_export(token["address"]), f"[{folder}] Solscan export", 2)
                source = "Solscan export"
            except Exception:  # noqa: BLE001
                write_log(f"[{folder}] Solscan unavailable, switching to Solana RPC")
        if result is None:
            for rpc in SOL_RPCS:
                try:
                    result = retry(
                        lambda rpc=rpc: sol_rpc_holders(token["address"], rpc, state_dir),
                        f"[{folder}] RPC {rpc}",
                        2,
                    )
                    source = f"Solana RPC {rpc}"
                    break
                except Exception as exc:  # noqa: BLE001
                    write_log(f"  {exc}")
        if result is None:
            raise RpcError("all Solana sources failed")
        ms = int(time.time() * 1000)
        out = directory / f"export_token_holders_{token['address']}_{ms}.csv"
    else:
        result = get_bsc_holders(token, base, state_dir, workers)
        source = "BSC RPC log scan"
        out = directory / f"tokenholders-0x3030a6d0_{dstr}.csv"
    save_csv(out, result["text"])
    write_log(f"[{folder}] OK  {result['rows']} rows via {source} -> {out}")


def run(argv: list[str] | None = None) -> int:
    global LOG_PATH
    parser = argparse.ArgumentParser(description="Collect token-holder and DexScreener snapshots.")
    parser.add_argument("--base", type=Path, default=ROOT / "data", help="output root (default: data/)")
    parser.add_argument("--state", type=Path, default=ROOT / "state", help="BSC cache directory (default: state/)")
    parser.add_argument("--force", action="store_true", help="ignore the 30-minute recent-file skip")
    parser.add_argument("--skip-solscan", action="store_true", help="use the Solana RPC method directly")
    parser.add_argument("--only", action="append", default=[], help="folder name(s), comma-separated")
    parser.add_argument("--workers", type=int, default=8, help="parallel BSC log workers (default: 8)")
    parser.add_argument("--check", action="store_true", help="run offline format checks and exit")
    args = parser.parse_args(argv)

    if args.check:
        check_offline()
        print("self-check ok")
        return 0

    base = args.base.resolve()
    state_dir = args.state.resolve()
    base.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)
    LOG_PATH = base / "_fallback.log"

    only: list[str] = []
    for item in args.only:
        only.extend(part.strip() for part in item.split(",") if part.strip())
    only_cf = {name.casefold() for name in only}

    now = china_now()
    dstr = now.strftime("%Y-%m-%d_%H%M")
    write_log(f"==== fallback run for {dstr} (China time) ====")
    failed = 0
    for token in TOKENS:
        if only_cf and token["folder"].casefold() not in only_cf:
            continue
        try:
            collect_token(token, base, state_dir, dstr, args.force, args.skip_solscan, args.workers)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            write_log(f"[{token['folder']}] FAILED: {exc}")
        time.sleep(3)

    for folder in DEX_FOLDERS:
        if only_cf and folder.casefold() not in only_cf:
            continue
        try:
            dexscreener_snapshot(folder, dstr, base, args.force)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            write_log(f"[{folder}] DexScreener FAILED: {exc}")
    write_log(f"==== done, {failed} failed ====")
    return failed


def check_offline() -> None:
    assert format_units(0, 18) == "0"
    assert format_units(10**18, 18) == "1"
    assert format_units(15 * 10**17, 18) == "1.5"
    assert format_units(1000 * 10**18, 18, thousands=True) == "1,000"
    assert format_units(1234567890123456789, 18, thousands=True) == "1.234567890123456789"
    assert format_units(1, 18) == "0.000000000000000001"
    assert b58encode(b"\x00" * 32) == "1" * 32
    assert b58encode(b"\x00\x00\x01") == "112"
    encoded = multicall_aggregate(
        "0x3030a6d0b748c02e5543cad9200D715c7Bfb7777",
        ["0x0000000000000000000000000000000000000001"],
    )
    assert encoded.startswith("0x252dba42")
    assert "70a08231" in encoded
    pct = (Decimal(1) * Decimal(100) / Decimal(3)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    assert f"{pct:.2f}" == "33.33"
    pct = (Decimal(1) * Decimal(100) / Decimal(8)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    assert f"{pct:.2f}" == "12.50"


if __name__ == "__main__":
    sys.exit(run())
