from __future__ import annotations

import json
from datetime import datetime, timezone

import requests

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

WMO_LIST_URL = "https://severeweather.wmo.int/list.html"
WMO_ALL_URL = "https://severeweather.wmo.int/json/wmo_all.json"

USGS_MAP_URL = "https://earthquake.usgs.gov/earthquakes/map"
USGS_SUMMARY_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/2.5_day.geojson"

MIROVA_NRT_URL = "https://www.mirovaweb.it/NRT/"

GDACS_ALERTS_URL = "https://www.gdacs.org/Alerts/"
GDACS_EVENTS_API_URL = (
    "https://www.gdacs.org/gdacsapi/api/events/geteventlist/ARCHIVE"
    "?eventlist=EQ;TC;FL;VO;WF"
)

TSUNAMI_HOME_URL = "https://www.tsunami.gov/"
TSUNAMI_NTWC_ATOM_URL = "https://www.tsunami.gov/events/xml/PAAQAtom.xml"
TSUNAMI_PTWC_ATOM_URL = "https://www.tsunami.gov/events/xml/PHEBAtom.xml"

JTWC_ABIO_URL = "https://www.metoc.navy.mil/jtwc/products/abioweb.txt"
JTWC_ABPW_URL = "https://www.metoc.navy.mil/jtwc/products/abpwweb.txt"

NHC_TWOS = [
    "https://www.nhc.noaa.gov/text/MIATWOAT.shtml?text=",
    "https://www.nhc.noaa.gov/text/MIATWOEP.shtml?text=",
    "https://www.nhc.noaa.gov/text/HFOTWOCP.shtml?text=",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()



def _safe_fetch_text(url: str, *, timeout: int = 30) -> dict:
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout)
        resp.raise_for_status()
        return {
            "ok": True,
            "url": url,
            "final_url": resp.url,
            "status_code": resp.status_code,
            "content_type": resp.headers.get("content-type", ""),
            "text": resp.text,
            "error": "",
        }
    except Exception as exc:
        return {
            "ok": False,
            "url": url,
            "final_url": url,
            "status_code": None,
            "content_type": "",
            "text": "",
            "error": str(exc),
        }



def _safe_fetch_json(url: str, *, timeout: int = 30) -> dict:
    try:
        resp = requests.get(url, headers=REQUEST_HEADERS, timeout=timeout)
        resp.raise_for_status()
        try:
            data = resp.json()
        except Exception:
            data = json.loads(resp.text)
        return {
            "ok": True,
            "url": url,
            "final_url": resp.url,
            "status_code": resp.status_code,
            "content_type": resp.headers.get("content-type", ""),
            "data": data,
            "error": "",
        }
    except Exception as exc:
        return {
            "ok": False,
            "url": url,
            "final_url": url,
            "status_code": None,
            "content_type": "",
            "data": None,
            "error": str(exc),
        }



def _build_stub(url: str, reason: str) -> dict:
    return {
        "ok": False,
        "url": url,
        "final_url": url,
        "status_code": None,
        "content_type": "",
        "error": reason,
    }



def _fetch_wmo_payloads() -> dict:
    # Only fetch the canonical JSON feed. The page shell and the content blob were
    # redundant for our pipeline and increased payload size / I/O for no benefit.
    return {
        "wmo_all": _safe_fetch_json(WMO_ALL_URL),
        "wmo_list": _build_stub(WMO_LIST_URL, "not_fetched_by_design"),
        "wmo_all_content": _build_stub(
            "https://severeweather.wmo.int/json/wmo_all_content.json",
            "not_fetched_by_design",
        ),
    }



def _fetch_usgs_payload() -> dict:
    return {
        "usgs_map": {
            "ok": True,
            "url": USGS_MAP_URL,
            "final_url": USGS_MAP_URL,
            "status_code": None,
            "content_type": "",
            "text": "",
            "error": "",
        },
        "usgs_earthquakes": _safe_fetch_json(USGS_SUMMARY_URL),
    }



def _fetch_mirova_payload() -> dict:
    return {
        "mirova_nrt": _safe_fetch_text(MIROVA_NRT_URL),
    }



def _fetch_gdacs_payload() -> dict:
    # We keep the public alerts page only as a source-page reference and for
    # troubleshooting. The actual structured data comes from the events API.
    return {
        "gdacs_alerts_page": {
            "ok": True,
            "url": GDACS_ALERTS_URL,
            "final_url": GDACS_ALERTS_URL,
            "status_code": None,
            "content_type": "",
            "text": "",
            "error": "",
        },
        "gdacs_events_api": _safe_fetch_json(GDACS_EVENTS_API_URL),
    }



def _fetch_typhoon_payload() -> dict:
    nhc_twos = [_safe_fetch_text(url, timeout=30) for url in NHC_TWOS]
    return {
        "jtwc_abio": _safe_fetch_text(JTWC_ABIO_URL, timeout=30),
        "jtwc_abpw": _safe_fetch_text(JTWC_ABPW_URL, timeout=30),
        "nhc_twos": nhc_twos,
    }



def _fetch_tsunami_payload() -> dict:
    return {
        "tsunami_home": _safe_fetch_text(TSUNAMI_HOME_URL, timeout=30),
        # Atom feeds are intentionally left as diagnostics-only stubs for now.
        # The active parser reads the homepage's main warning block.
        "tsunami_ntwc_atom": _build_stub(TSUNAMI_NTWC_ATOM_URL, "not_fetched_by_design"),
        "tsunami_ptwc_atom": _build_stub(TSUNAMI_PTWC_ATOM_URL, "not_fetched_by_design"),
    }



def fetch_natural_hazard_payloads() -> dict:
    fetched_at = utc_now_iso()
    payload = {"fetched_at": fetched_at}
    payload.update(_fetch_wmo_payloads())
    payload.update(_fetch_usgs_payload())
    payload.update(_fetch_mirova_payload())
    payload.update(_fetch_gdacs_payload())
    payload.update(_fetch_tsunami_payload())
    payload.update(_fetch_typhoon_payload())
    return payload



def fetch_weather_alerts_raw() -> dict:
    payload = fetch_natural_hazard_payloads()
    return {
        "fetched_at": payload["fetched_at"],
        "wmo_all": payload.get("wmo_all", {}),
        "wmo_list": payload.get("wmo_list", {}),
        "wmo_all_content": payload.get("wmo_all_content", {}),
    }



def fetch_disaster_payloads() -> dict:
    payload = fetch_natural_hazard_payloads()
    return {
        "fetched_at": payload["fetched_at"],
        "usgs_map": payload.get("usgs_map", {}),
        "usgs_earthquakes": payload.get("usgs_earthquakes", {}),
        "mirova_nrt": payload.get("mirova_nrt", {}),
        "gdacs_alerts_page": payload.get("gdacs_alerts_page", {}),
        "gdacs_events_api": payload.get("gdacs_events_api", {}),
        "tsunami_home": payload.get("tsunami_home", {}),
        "tsunami_ntwc_atom": payload.get("tsunami_ntwc_atom", {}),
        "tsunami_ptwc_atom": payload.get("tsunami_ptwc_atom", {}),
        "jtwc_abio": payload.get("jtwc_abio", {}),
        "jtwc_abpw": payload.get("jtwc_abpw", {}),
        "nhc_twos": payload.get("nhc_twos", []),
    }


# compatibility wrapper for older disaster_ingest imports

def fetch_weather_alerts_payloads():
    return fetch_weather_alerts_raw()
