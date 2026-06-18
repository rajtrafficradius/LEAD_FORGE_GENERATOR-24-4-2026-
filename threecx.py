"""threecx.py — 3CX call-data ingestion for the CRM layer (Phase 3).

Captures every call's metadata + recording + transcript and ties it to a lead
and the BDE who placed it, for the lead's Call-History panel.

Two ingestion paths (both land in the lead_calls table via LeadCallsRepo):

  1. WEBHOOK PUSH (works as soon as you configure 3CX / Make / Zapier to POST
     call events to /api/3cx/webhook with the shared THREECX_WEBHOOK_SECRET).
     This is the primary, no-extra-config path.
  2. CDR PULL (optional, env-gated): when THREECX_PBX_URL + THREECX_APP_ID +
     THREECX_SECRET are set, an admin can trigger a pull of recent Call Detail
     Records via the 3CX v20 API. Built defensively; off until configured.

Mapping:
  * lead  ← master_leads.phone_e164 matched on the dialed/caller number (suffix).
  * BDE   ← users.mobile_e164 matched on the calling number (suffix) — this is
            the "which mobile number was used to contact which lead" link.

Transcription: if a recording URL is present and OPENAI_API_KEY is set, the
recording is fetched and transcribed with Whisper (whisper-1). 3CX has no
native transcription, so this is how transcripts are produced.

Every failure path is non-fatal — a bad call event never breaks the endpoint.
"""
from __future__ import annotations

import hashlib
import io
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import requests

log = logging.getLogger("leadforge.threecx")

# Flexible field aliases — different 3CX templates / middlewares name things
# differently, so we accept the common variants.
_F_UUID = ("call_uuid", "callid", "call_id", "callId", "id", "uuid", "CallId")
_F_DIR = ("direction", "type", "call_type", "CallType")
_F_FROM = ("from_number", "from", "caller", "source", "from_no", "fromCaller",
           "caller_number", "src", "FromNo")
_F_TO = ("to_number", "to", "callee", "destination", "dialed", "to_no",
         "dest", "ToNo", "called")
_F_AGENT = ("agent", "agent_name", "extension", "from_dn", "agentName", "ext",
            "operator", "user")
_F_START = ("started_at", "start", "start_time", "time_start", "timestamp",
            "StartTime", "datetime")
_F_DUR = ("duration_sec", "duration", "talk_time", "talking", "TalkTime",
          "duration_seconds")
_F_REC = ("recording_url", "recording", "recordingUrl", "rec_url", "RecordingUrl",
          "recording_link")


def _pick(payload: Dict[str, Any], keys) -> Any:
    for k in keys:
        if k in payload and payload[k] not in (None, ""):
            return payload[k]
    return None


def _to_seconds(v: Any) -> int:
    """Accept 90 / '90' / '00:01:30' / '1:30' → seconds."""
    if v is None:
        return 0
    if isinstance(v, (int, float)):
        return int(v)
    s = str(v).strip()
    if s.isdigit():
        return int(s)
    if ":" in s:
        parts = [int(p) for p in s.split(":") if p.isdigit()]
        if len(parts) == 3:
            return parts[0] * 3600 + parts[1] * 60 + parts[2]
        if len(parts) == 2:
            return parts[0] * 60 + parts[1]
    try:
        return int(float(s))
    except Exception:
        return 0


def _norm_direction(v: Any) -> str:
    s = (str(v or "")).strip().lower()
    if s in ("inbound", "in", "incoming", "received"):
        return "inbound"
    if s in ("outbound", "out", "outgoing", "dialed", "placed"):
        return "outbound"
    return "unknown"


def normalize_call(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Map an arbitrary 3CX-ish payload onto our lead_calls shape."""
    from_n = str(_pick(payload, _F_FROM) or "")
    to_n = str(_pick(payload, _F_TO) or "")
    start = _pick(payload, _F_START)
    started_at = None
    if start:
        try:
            started_at = datetime.fromisoformat(str(start).replace("Z", "+00:00"))
            started_at = started_at.replace(tzinfo=None)
        except Exception:
            started_at = None
    uuid = _pick(payload, _F_UUID)
    if not uuid:
        seed = f"{from_n}|{to_n}|{start or ''}".encode("utf-8")
        uuid = "auto-" + hashlib.sha1(seed).hexdigest()[:24]
    return {
        "call_uuid": str(uuid)[:96],
        "direction": _norm_direction(_pick(payload, _F_DIR)),
        "from_number": from_n[:40],
        "to_number": to_n[:40],
        "agent_name": (str(_pick(payload, _F_AGENT) or ""))[:128],
        "started_at": started_at,
        "duration_sec": _to_seconds(_pick(payload, _F_DUR)),
        "recording_url": (str(_pick(payload, _F_REC) or ""))[:768],
        "raw_json": payload,
    }


def transcribe(recording_url: str, openai_key: str,
               auth_header: Optional[Dict[str, str]] = None) -> str:
    """Download a recording and transcribe with Whisper. '' on any failure."""
    if not recording_url or not openai_key:
        return ""
    try:
        rec = requests.get(recording_url, headers=auth_header or {}, timeout=60)
        if rec.status_code >= 400 or not rec.content:
            log.debug("recording fetch HTTP %s for %s", rec.status_code, recording_url)
            return ""
        fname = recording_url.split("?")[0].rsplit("/", 1)[-1] or "call.mp3"
        if "." not in fname:
            fname += ".mp3"
        files = {"file": (fname, io.BytesIO(rec.content))}
        data = {"model": os.environ.get("THREECX_WHISPER_MODEL", "whisper-1")}
        resp = requests.post(
            "https://api.openai.com/v1/audio/transcriptions",
            headers={"Authorization": f"Bearer {openai_key}"},
            files=files, data=data, timeout=180,
        )
        if resp.status_code == 200:
            return (resp.json().get("text") or "").strip()
        log.debug("whisper HTTP %s: %s", resp.status_code, resp.text[:200])
    except Exception as e:
        log.debug("transcribe failed: %s", e)
    return ""


def ingest(payload: Dict[str, Any], transcribe_audio: bool = True) -> Tuple[int, Dict[str, Any]]:
    """Normalize → match lead + BDE → transcribe (best-effort) → store.
    Returns (call_id, normalized_call)."""
    import db
    call = normalize_call(payload)
    try:
        call["lead_id"] = db.LeadCallsRepo.match_lead_by_phone(
            call["to_number"], call["from_number"])
    except Exception as e:
        log.debug("lead match failed: %s", e)
        call["lead_id"] = None
    try:
        # BDE is whoever's saved mobile placed/took the call.
        call["bde_user_id"] = db.LeadCallsRepo.match_bde_by_mobile(
            call["from_number"], call["to_number"])
    except Exception:
        call["bde_user_id"] = None

    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if transcribe_audio and call.get("recording_url") and openai_key:
        auth = None
        api_key = os.environ.get("THREECX_API_KEY", "")
        if api_key and _pbx_host() and _pbx_host() in call["recording_url"]:
            auth = {"Authorization": f"Bearer {api_key}"}
        t = transcribe(call["recording_url"], openai_key, auth_header=auth)
        if t:
            call["transcript"] = t

    call_id = db.LeadCallsRepo.record(call)
    return call_id, call


# ── Optional CDR pull (3CX v20) — only active when configured ────────────────

def _pbx_host() -> str:
    return (os.environ.get("THREECX_PBX_URL", "") or "").rstrip("/")


def is_pull_configured() -> bool:
    return bool(_pbx_host() and os.environ.get("THREECX_APP_ID")
                and os.environ.get("THREECX_SECRET"))


_token_cache: Dict[str, Any] = {"token": "", "exp": 0.0}


def _get_token() -> str:
    """3CX v20 OAuth client-credentials token (cached ~the hour it's valid).
    Returns '' if unconfigured/fails."""
    if not is_pull_configured():
        return ""
    import time as _t
    if _token_cache["token"] and _t.time() < _token_cache["exp"]:
        return _token_cache["token"]
    try:
        r = requests.post(
            f"{_pbx_host()}/connect/token",
            data={"grant_type": "client_credentials",
                  "client_id": os.environ.get("THREECX_APP_ID", ""),
                  "client_secret": os.environ.get("THREECX_SECRET", "")},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=20,
        )
        if r.status_code == 200:
            j = r.json()
            tok = j.get("access_token", "") or ""
            exp = int(j.get("expires_in", 3600) or 3600)
            _token_cache["token"] = tok
            _token_cache["exp"] = _t.time() + max(60, exp - 60)  # refresh a min early
            return tok
        log.warning("3CX token HTTP %s: %s", r.status_code, r.text[:200])
    except Exception as e:
        log.warning("3CX token error: %s", e)
    return ""


def _device_id(dev: Dict[str, Any]) -> str:
    for k in ("device_id", "deviceId", "dummy_id", "id", "dn"):
        if isinstance(dev, dict) and dev.get(k) not in (None, ""):
            return str(dev[k])
    return ""


def get_devices(dn: str, token: str) -> list:
    """GET /callcontrol/{dn} → that extension's registered devices (each carries a
    device_id needed for make-call). v20 Call Control API."""
    try:
        r = requests.get(
            f"{_pbx_host()}/callcontrol/{dn}",
            headers={"Authorization": f"Bearer {token}"}, timeout=20,
        )
        if r.status_code == 200:
            data = r.json()
            devs = data.get("devices") if isinstance(data, dict) else data
            return devs or []
        log.debug("get_devices(%s) HTTP %s: %s", dn, r.status_code, r.text[:200])
    except Exception as e:
        log.debug("get_devices(%s) failed: %s", dn, e)
    return []


def make_call(extension: str, destination: str) -> Dict[str, Any]:
    """Click-to-call via the 3CX v20 Call Control API. Flow:
      1. OAuth token (cached).
      2. GET /callcontrol/{dn} → find a registered device for the agent extension.
      3. POST /callcontrol/{dn}/devices/{deviceId}/makecall {reason,destination,timeout}
         → rings the agent's device, then dials the lead on answer (HTTP 202).
    Path overridable via THREECX_MAKECALL_PATH (use {dn} and {device}); a fixed
    device can be forced via THREECX_DEVICE_ID. Clear reason returned until set."""
    if not is_pull_configured():
        return {"ok": False, "reason": "3CX not configured "
                "(set THREECX_PBX_URL, THREECX_APP_ID, THREECX_SECRET on Railway)"}
    if not extension:
        return {"ok": False, "reason": "no 3CX extension set for your user — add it in My Profile"}
    if not destination:
        return {"ok": False, "reason": "this lead has no phone number yet — add one first"}
    token = _get_token()
    if not token:
        return {"ok": False, "reason": "could not obtain a 3CX token (check Client ID/secret + that Call Control API access is enabled)"}
    device_id = os.environ.get("THREECX_DEVICE_ID", "")
    if not device_id:
        devs = get_devices(extension, token)
        if devs:
            device_id = _device_id(devs[0])
    if not device_id:
        return {"ok": False, "reason": f"no registered device found for extension {extension} — "
                "the extension must be online (3CX app/softphone/desk phone registered), and the "
                "extension must be in the API app's 'Select Extensions' list"}
    path = (os.environ.get("THREECX_MAKECALL_PATH", "/callcontrol/{dn}/devices/{device}/makecall")
            .replace("{dn}", str(extension)).replace("{device}", str(device_id)))
    try:
        r = requests.post(
            f"{_pbx_host()}{path}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"reason": "LeadForge click-to-call", "destination": str(destination),
                  "timeout": int(os.environ.get("THREECX_CALL_TIMEOUT", "30") or "30")},
            timeout=20,
        )
        if r.status_code in (200, 201, 202):
            return {"ok": True, "detail": f"ringing ext {extension} → {destination}…", "device": device_id}
        return {"ok": False, "reason": f"3CX HTTP {r.status_code}", "body": r.text[:300]}
    except Exception as e:
        return {"ok": False, "reason": str(e)}


def test_connection() -> Dict[str, Any]:
    """Verify the 3CX credentials from inside the deployment: PBX reachable, token
    obtainable, and which API scopes work (XAPI users, Call Control). The one-click
    'is 3CX wired up?' check for the admin."""
    out: Dict[str, Any] = {"pbx_url": _pbx_host(), "app_id": os.environ.get("THREECX_APP_ID", ""),
                           "secret_present": bool(os.environ.get("THREECX_SECRET")),
                           "configured": is_pull_configured(), "token": False}
    if not _pbx_host():
        out["reason"] = "THREECX_PBX_URL not set (the address you log into 3CX with)"
        return out
    if not out["configured"]:
        out["reason"] = "THREECX_APP_ID and/or THREECX_SECRET not set"
        return out
    token = _get_token()
    out["token"] = bool(token)
    if not token:
        out["reason"] = "token request failed — check Client ID/secret and that the PBX URL is correct"
        return out
    # XAPI (Configuration API) probe — confirms reporting/CDR access.
    try:
        r = requests.get(f"{_pbx_host()}/xapi/v1/Users?$top=1",
                         headers={"Authorization": f"Bearer {token}"}, timeout=20)
        out["xapi_http"] = r.status_code
    except Exception as e:
        out["xapi_http"] = f"err: {e}"
    # Call Control probe — confirms dialing access is enabled.
    try:
        r = requests.get(f"{_pbx_host()}/callcontrol",
                         headers={"Authorization": f"Bearer {token}"}, timeout=20)
        out["callcontrol_http"] = r.status_code
        out["callcontrol_enabled"] = (r.status_code == 200)
    except Exception as e:
        out["callcontrol_http"] = f"err: {e}"
    out["ok"] = bool(token)
    return out


def pull_recent_calls(limit: int = 100) -> Dict[str, Any]:
    """Pull recent CDR rows from the PBX and ingest them. Endpoint paths vary by
    3CX edition, so the exact CDR route is configurable via THREECX_CDR_PATH.
    Returns a summary. No-op (with reason) when not configured."""
    if not is_pull_configured():
        return {"ok": False, "reason": "3CX pull not configured "
                "(set THREECX_PBX_URL, THREECX_APP_ID, THREECX_SECRET)"}
    token = _get_token()
    if not token:
        return {"ok": False, "reason": "could not obtain 3CX token"}
    # v20 XAPI call-history view (OData). Overridable via THREECX_CDR_PATH.
    path = os.environ.get("THREECX_CDR_PATH", "/xapi/v1/CallHistoryView")
    try:
        r = requests.get(
            f"{_pbx_host()}{path}",
            headers={"Authorization": f"Bearer {token}"},
            params={"$top": int(limit), "$orderby": "StartTime desc"}, timeout=40,
        )
        if r.status_code != 200:
            return {"ok": False, "reason": f"CDR HTTP {r.status_code}", "body": r.text[:300]}
        data = r.json()
        rows = data.get("value") if isinstance(data, dict) else data
        ingested = 0
        for row in (rows or []):
            try:
                ingest(row)
                ingested += 1
            except Exception:
                pass
        return {"ok": True, "ingested": ingested, "fetched": len(rows or [])}
    except Exception as e:
        return {"ok": False, "reason": str(e)}
