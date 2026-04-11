from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import warnings

import requests
from requests.exceptions import SSLError
from urllib3.exceptions import InsecureRequestWarning

from collectors.common import DEFAULT_HEADERS, utc_now_iso

try:
    import certifi
except Exception:  # pragma: no cover
    certifi = None  # type: ignore


MND_PLA_SOURCE_NAME = "MND PLA Around Taiwan"
MND_PLA_BASE_URL = "https://www.mnd.gov.tw"
MND_PLA_LIST_URL = f"{MND_PLA_BASE_URL}/news/plaactlist"


@dataclass
class MndPlaFetchResult:
    source_name: str
    url: str
    success: bool
    status_code: Optional[int]
    html: str
    fetched_at: str
    error_message: str = ""
    used_insecure_fallback: bool = False


_VERIFY_DEFAULT = certifi.where() if certifi else True


def _request_text(
    url: str,
    *,
    timeout: int = 30,
    allow_insecure_fallback: bool = True,
) -> tuple[bool, Optional[int], str, str, bool]:
    try:
        response = requests.get(
            url,
            headers=DEFAULT_HEADERS,
            timeout=timeout,
            verify=_VERIFY_DEFAULT,
        )
        response.raise_for_status()
        return True, response.status_code, response.text, "", False
    except SSLError as exc:
        if not allow_insecure_fallback:
            return False, None, "", str(exc), False
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", InsecureRequestWarning)
                response = requests.get(
                    url,
                    headers=DEFAULT_HEADERS,
                    timeout=timeout,
                    verify=False,
                )
            response.raise_for_status()
            return True, response.status_code, response.text, "", True
        except requests.RequestException as inner_exc:
            status_code = getattr(getattr(inner_exc, "response", None), "status_code", None)
            return False, status_code, "", str(inner_exc), True
    except requests.RequestException as exc:
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        return False, status_code, "", str(exc), False



def fetch_mnd_pla_list_page(
    url: str = MND_PLA_LIST_URL,
    *,
    timeout: int = 30,
    allow_insecure_fallback: bool = True,
) -> MndPlaFetchResult:
    fetched_at = utc_now_iso()
    success, status_code, html, error_message, used_insecure = _request_text(
        url,
        timeout=timeout,
        allow_insecure_fallback=allow_insecure_fallback,
    )
    return MndPlaFetchResult(
        source_name=MND_PLA_SOURCE_NAME,
        url=url,
        success=success,
        status_code=status_code,
        html=html,
        fetched_at=fetched_at,
        error_message=error_message,
        used_insecure_fallback=used_insecure,
    )



def fetch_mnd_pla_detail_page(
    url: str,
    *,
    timeout: int = 30,
    allow_insecure_fallback: bool = True,
) -> MndPlaFetchResult:
    fetched_at = utc_now_iso()
    success, status_code, html, error_message, used_insecure = _request_text(
        url,
        timeout=timeout,
        allow_insecure_fallback=allow_insecure_fallback,
    )
    return MndPlaFetchResult(
        source_name=MND_PLA_SOURCE_NAME,
        url=url,
        success=success,
        status_code=status_code,
        html=html,
        fetched_at=fetched_at,
        error_message=error_message,
        used_insecure_fallback=used_insecure,
    )
