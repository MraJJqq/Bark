#!/usr/bin/env python3
"""Single-authority Bark notifier for Vegas scan results."""
import json, os, sys, time, tempfile
from datetime import datetime, timedelta, timezone
from urllib.parse import quote
import requests

CST = timezone(timedelta(hours=8))
API = "https://api.github.com"
STATE_FILE = "state.json"
SCANNER_REPO = os.environ.get("SCANNER_REPO", "MraJJqq/vegas")
SCANNER_TOKEN = os.environ["SCANNER_TOKEN"]
SCANNER_WORKFLOW = os.environ.get("SCANNER_WORKFLOW", "vegas-scan.yml")
SCANNER_BRANCH = os.environ.get("SCANNER_BRANCH", "main")
BARK_KEY = os.environ["BARK_KEY"]
BARK_SERVER = os.environ.get("BARK_SERVER", "https://api.day.app").rstrip("/")
RESCAN_TIMES, WAIT_SECONDS, POLL_SECONDS, KEEP_DAYS = 2, 900, 15, 7
SYMBOLS = [
    "MSFTUSDT","GOOGLUSDT","AMZNUSDT","NVDAUSDT","OPENAIUSDT","AMDUSDT",
    "TSMUSDT","AVGOUSDT","ORCLUSDT","ARMUSDT","MUUSDT","ASMLUSDT",
    "SNDKUSDT","SKHYUSDT","MRVLUSDT","GLWUSDT","HOODUSDT","SPCXUSDT",
    "BTCUSDT","HYPEUSDT","XAUUSDT","CLUSDT",
]
TITLE = "▸▸ 𝐕𝐞𝐠𝐚𝐬 𝐒𝐢𝐠𝐧𝐚𝐥 ◂◂"
NO_SIGNAL, SCAN_FAILED = "𝐍𝐨 𝐒𝐢𝐠𝐧𝐚𝐥", "𝐒𝐜𝐚𝐧 𝐅𝐚𝐢𝐥𝐞𝐝"
SEP = "\u00a0▰\u00a0"
BOLD = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
    "𝐀𝐁𝐂𝐃𝐄𝐅𝐆𝐇𝐈𝐉𝐊𝐋𝐌𝐍𝐎𝐏𝐐𝐑𝐒𝐓𝐔𝐕𝐖𝐗𝐘𝐙𝟎𝟏𝟐𝟑𝟒𝟓𝟔𝟕𝟖𝟗")
HEADERS = {"Authorization": f"Bearer {SCANNER_TOKEN}",
    "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
S = requests.Session()
S.headers.update(HEADERS)

class UncertainDelivery(RuntimeError): pass

def parse_time(value):
    if not isinstance(value, str): raise ValueError("generated_at missing")
    return datetime.strptime(value.replace(" (UTC+8)", ""), "%Y-%m-%d %H:%M:%S").replace(tzinfo=CST)

def load_state():
    if not os.path.exists(STATE_FILE):
        return {"initialized": False, "sent": {}, "uncertain": {},
                "no_signal_slots": [], "failure_slots": [], "uncertain_alerts": {}}
    try:
        with open(STATE_FILE, encoding="utf-8") as f: x = json.load(f)
        if not isinstance(x, dict): raise ValueError("root is not object")
        for k, default in (("initialized", False), ("sent", {}), ("uncertain", {}),
                           ("no_signal_slots", []), ("failure_slots", []), ("uncertain_alerts", {})):
            x.setdefault(k, default)
        if not isinstance(x["sent"], dict) or not isinstance(x["uncertain"], dict):
            raise ValueError("signal state malformed")
        if not isinstance(x["no_signal_slots"], list) or not isinstance(x["failure_slots"], list):
            raise ValueError("slot state malformed")
        if not isinstance(x["uncertain_alerts"], dict):
            raise ValueError("uncertain alert state malformed")
        return x
    except Exception as e:
        raise RuntimeError(f"state.json damaged; refusing reset: {e}") from e

def save_state(x):
    fd, tmp = tempfile.mkstemp(prefix=".state-", dir=".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(x, f, ensure_ascii=False, indent=2); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, STATE_FILE)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)

def prune(x):
    cutoff = int(time.time()) - KEEP_DAYS * 86400
    for field in ("sent", "uncertain"):
        x[field] = {k:v for k,v in x[field].items()
                    if isinstance(v, dict) and isinstance(v.get("ts"), int) and v["ts"] >= cutoff}
    keep = (datetime.now(CST).date() - timedelta(days=KEEP_DAYS)).isoformat()
    for field in ("no_signal_slots", "failure_slots"):
        x[field] = [v for v in x[field] if str(v)[:10] >= keep]
    x["uncertain_alerts"] = {
        k:v for k,v in x["uncertain_alerts"].items()
        if isinstance(v, dict) and isinstance(v.get("ts"), int) and v["ts"] >= cutoff
    }

def gh(url, method="GET", **kwargs):
    r = S.request(method, url, timeout=25, **kwargs)
    r.raise_for_status()
    return r.json() if r.content else {}

def workflow_url():
    return f"{API}/repos/{SCANNER_REPO}/actions/workflows/{quote(SCANNER_WORKFLOW, safe='')}"

def latest_run():
    x = gh(workflow_url()+"/runs", params={"branch":SCANNER_BRANCH,"per_page":1})
    runs = x.get("workflow_runs", [])
    return runs[0] if runs else None

def read_result():
    url = f"{API}/repos/{SCANNER_REPO}/contents/signals.json"
    r = S.get(url, params={"ref":SCANNER_BRANCH},
        headers={**HEADERS,"Accept":"application/vnd.github.raw+json"}, timeout=25)
    r.raise_for_status()
    return r.json()

def validate(x):
    if not isinstance(x, dict): raise ValueError("signals.json is not an object")
    if x.get("scanned") != 22 or x.get("succeeded") != 22:
        raise ValueError(f"scan incomplete: scanned={x.get('scanned')} succeeded={x.get('succeeded')}")
    if x.get("errors") != []: raise ValueError(f"scan errors: {x.get('errors')!r}")
    if not isinstance(x.get("signals"), list): raise ValueError("signals must be a list")
    generated = parse_time(x.get("generated_at"))
    age = (datetime.now(CST)-generated).total_seconds()
    if age < -300 or age > 90*60: raise ValueError(f"result stale/future: {int(age)} seconds")
    ids, allowed = set(), set(SYMBOLS)
    for item in x["signals"]:
        if not isinstance(item, dict): raise ValueError("malformed signal item")
        sid, symbol = item.get("id"), item.get("symbol")
        if not isinstance(sid, str) or not sid.strip() or sid in ids: raise ValueError("missing/duplicate signal ID")
        if symbol not in allowed: raise ValueError(f"unexpected symbol: {symbol}")
        ids.add(sid)
    return generated

def rescan(previous_id, previous_generated):
    r = S.post(workflow_url()+"/dispatches", json={"ref":SCANNER_BRANCH}, timeout=25)
    if r.status_code not in (201,204): raise RuntimeError(f"workflow_dispatch failed: HTTP {r.status_code}")
    deadline = time.monotonic()+WAIT_SECONDS
    run = None
    while time.monotonic() < deadline:
        latest = latest_run()
        if latest and int(latest["id"]) > int(previous_id or 0):
            run = latest; break
        time.sleep(POLL_SECONDS)
    if not run: raise TimeoutError("new Vegas scan run not found within 15 minutes")
    run_id = run["id"]
    while time.monotonic() < deadline:
        run = gh(f"{API}/repos/{SCANNER_REPO}/actions/runs/{run_id}")
        if run.get("status") == "completed":
            if run.get("conclusion") != "success":
                raise RuntimeError(f"Vegas scan run {run_id}: {run.get('conclusion')}")
            result = read_result()
            generated = validate(result)
            if previous_generated is not None and generated <= previous_generated:
                raise RuntimeError("signals.json did not refresh after rescan")
            return result
        time.sleep(POLL_SECONDS)
    raise TimeoutError(f"Vegas scan run {run_id} exceeded 15-minute wait")

def acquire_result():
    event_status = None
    path = os.environ.get("GITHUB_EVENT_PATH")
    if path and os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                event_status = (json.load(f).get("client_payload") or {}).get("scanner_job_status")
        except Exception: pass
    original, error = None, None
    try:
        original = read_result()
        validate(original)
        if event_status == "failure": raise ValueError("Vegas workflow reported failure")
        return original
    except Exception as e: error = e
    previous_generated = None
    if original:
        try: previous_generated = parse_time(original.get("generated_at"))
        except Exception: pass
    last = latest_run()
    previous_id = last.get("id", 0) if last else 0
    for attempt in range(1, RESCAN_TIMES+1):
        try:
            print(f"Recovery scan {attempt}/{RESCAN_TIMES}")
            return rescan(previous_id, previous_generated)
        except Exception as e:
            print(f"Recovery scan {attempt} failed: {e}", file=sys.stderr)
            latest = latest_run()
            if latest: previous_id = latest.get("id", previous_id)
    raise RuntimeError(f"Vegas incomplete after 2 recovery scans; initial error: {error}")

def slot_key(generated):
    for day_offset in (0,-1):
        d = generated.date()+timedelta(days=day_offset)
        for h in (8,10,16,20,22):
            t = datetime(d.year,d.month,d.day,h,5,tzinfo=CST)
            if 0 <= (generated-t).total_seconds() <= 2*3600: return t.strftime("%Y-%m-%d-%H")
    return generated.strftime("%Y-%m-%d-manual-%H%M")

def push(body):
    url = f"{BARK_SERVER}/{quote(BARK_KEY,safe='')}/{quote(TITLE,safe='')}/{quote(body,safe='')}"
    try:
        r = requests.get(url, params={"group":"Vegas Signal","level":"active"}, timeout=(10,25))
    except requests.RequestException as e: raise UncertainDelivery(str(e)) from e
    if r.status_code >= 500: raise UncertainDelivery(f"Bark HTTP {r.status_code}")
    r.raise_for_status()
    try: payload = r.json()
    except ValueError as e: raise RuntimeError("Bark response is not JSON") from e
    if payload.get("code") != 200: raise RuntimeError(f"Bark rejected: {payload}")
    return True

def failure_notice(state, slot):
    if slot in state["failure_slots"] or slot in state["uncertain_alerts"]: return
    try: push(SCAN_FAILED)
    except UncertainDelivery as e:
        state["uncertain_alerts"][slot] = {"ts":int(time.time()),"body":SCAN_FAILED}
        save_state(state); raise RuntimeError(f"failure notice delivery uncertain: {e}") from e
    state["failure_slots"].append(slot); save_state(state)

def main():
    state = load_state(); prune(state)
    try:
        result = acquire_result()
        generated = validate(result)
    except Exception as e:
        slot = datetime.now(CST).strftime("%Y-%m-%d-%H")
        print(f"SCAN FAILED: {e}", file=sys.stderr)
        failure_notice(state, slot); save_state(state); raise
    slot = slot_key(generated)
    signals = result["signals"]
    ids = {s["id"] for s in signals}
    if not state["initialized"]:
        stamp = int(time.time())
        state["sent"] = {sid:{"ts":stamp,"baseline":True} for sid in ids}
        state["initialized"] = True
        state["baseline_generated_at"] = result["generated_at"]
        save_state(state)
        print(f"Baseline stored ({len(ids)} IDs); historical signals not pushed")
        return
    if not signals:
        if slot in state["no_signal_slots"] or slot in state["uncertain_alerts"]:
            print(f"No-signal slot already handled: {slot}"); save_state(state); return
        try: push(NO_SIGNAL)
        except UncertainDelivery as e:
            state["uncertain_alerts"][slot] = {"ts":int(time.time()),"body":NO_SIGNAL}
            save_state(state); raise RuntimeError(f"No Signal delivery uncertain: {e}") from e
        state["no_signal_slots"].append(slot); save_state(state)
        print(f"No Signal accepted for {slot}"); return
    fresh = [s for s in signals if s["id"] not in state["sent"] and s["id"] not in state["uncertain"]]
    if not fresh:
        print("No new IDs; existing signals are not a No Signal condition"); save_state(state); return
    fresh_symbols = {s["symbol"] for s in fresh}
    ordered = [s for s in SYMBOLS if s in fresh_symbols]
    body = SEP.join(s.removesuffix("USDT").translate(BOLD) for s in ordered)
    try: push(body)
    except UncertainDelivery as e:
        stamp = int(time.time())
        for item in fresh: state["uncertain"][item["id"]] = {"ts":stamp,"slot":slot}
        save_state(state)
        raise RuntimeError(f"delivery uncertain; IDs quarantined against duplicate retry: {e}") from e
    stamp = int(time.time())
    for item in fresh: state["sent"][item["id"]] = {"ts":stamp,"slot":slot}
    save_state(state)
    print(f"Bark accepted {len(fresh)} IDs / {len(ordered)} unique symbols")

if __name__ == "__main__": main()
