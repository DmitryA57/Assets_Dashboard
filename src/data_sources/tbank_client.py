from __future__ import annotations

import ssl
import tempfile
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.poolmanager import PoolManager
import certifi


TINVEST_BASE_URL = "https://invest-public-api.tbank.ru/rest"
DEFAULT_TIMEOUT_SECONDS = 30
WINDOWS_CERT_STORES = ("ROOT", "CA")
_WINDOWS_CA_BUNDLE_PATH: str | None = None


class SSLContextAdapter(HTTPAdapter):
    def __init__(self, ssl_context: ssl.SSLContext, *args: object, **kwargs: object) -> None:
        self.ssl_context = ssl_context
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, connections: int, maxsize: int, block: bool = False, **pool_kwargs: object) -> None:
        pool_kwargs["ssl_context"] = self.ssl_context
        self.poolmanager = PoolManager(
            num_pools=connections,
            maxsize=maxsize,
            block=block,
            **pool_kwargs,
        )


def _windows_store_pem_bytes() -> bytes:
    pem_chunks: list[bytes] = []
    if not hasattr(ssl, "enum_certificates"):
        return b""

    for store_name in WINDOWS_CERT_STORES:
        try:
            certificates = ssl.enum_certificates(store_name)
        except Exception:
            continue
        for cert_bytes, encoding_type, trust in certificates:
            if encoding_type != "x509_asn":
                continue
            try:
                pem_chunks.append(ssl.DER_cert_to_PEM_cert(cert_bytes).encode("ascii"))
            except Exception:
                continue
    return b"\n".join(pem_chunks)


def merged_ca_bundle_path(extra_ca_bundle_path: str | None = None) -> str:
    global _WINDOWS_CA_BUNDLE_PATH

    base_bundle = Path(certifi.where()).read_bytes()
    windows_bundle = _windows_store_pem_bytes()
    extra_bundle = b""

    if extra_ca_bundle_path:
        extra_path = Path(extra_ca_bundle_path).expanduser()
        if not extra_path.exists():
            raise ValueError(f"T-Invest CA bundle was not found: {extra_path}")
        extra_bundle = extra_path.read_bytes()

    if _WINDOWS_CA_BUNDLE_PATH is None:
        merged = b"\n".join(part for part in (base_bundle, windows_bundle, extra_bundle) if part)
        with tempfile.NamedTemporaryFile(prefix="tbank-ca-", suffix=".pem", delete=False) as handle:
            handle.write(merged)
            _WINDOWS_CA_BUNDLE_PATH = handle.name
        return _WINDOWS_CA_BUNDLE_PATH

    if extra_bundle:
        merged = b"\n".join(part for part in (base_bundle, windows_bundle, extra_bundle) if part)
        with tempfile.NamedTemporaryFile(prefix="tbank-ca-", suffix=".pem", delete=False) as handle:
            handle.write(merged)
            return handle.name

    return _WINDOWS_CA_BUNDLE_PATH


def first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def quotation_to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    if not isinstance(value, dict):
        return None

    units = first_present(value, "units")
    nanos = first_present(value, "nano", "nanos")
    if units is None and nanos is None:
        return None
    try:
        units_decimal = Decimal(str(units or 0))
        nanos_decimal = Decimal(str(nanos or 0)) / Decimal("1000000000")
    except Exception:
        return None
    return float(units_decimal + nanos_decimal)


def parse_timestamp(value: Any) -> pd.Timestamp | None:
    if value in (None, ""):
        return None
    if isinstance(value, pd.Timestamp):
        return value.tz_localize(None) if value.tzinfo is not None else value
    if isinstance(value, datetime):
        timestamp = pd.Timestamp(value)
        return timestamp.tz_localize(None) if timestamp.tzinfo is not None else timestamp
    if isinstance(value, dict):
        seconds = first_present(value, "seconds")
        nanos = first_present(value, "nanos", "nano")
        if seconds is None:
            return None
        try:
            timestamp = pd.Timestamp(float(seconds), unit="s", tz="UTC")
        except Exception:
            return None
        if nanos is not None:
            timestamp += pd.to_timedelta(int(nanos), unit="ns")
        return timestamp.tz_localize(None)

    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).tz_localize(None)


def format_enum_label(value: Any) -> str:
    if value in (None, ""):
        return ""
    text = str(value).strip()
    for prefix in ("SECURITY_TRADING_STATUS_", "INSTRUMENT_TYPE_", "TRADING_STATUS_"):
        if text.startswith(prefix):
            text = text.removeprefix(prefix)
    words = [word for word in text.split("_") if word]
    return " ".join(word.capitalize() for word in words) if words else str(value)


@dataclass(slots=True)
class TBankApiError(RuntimeError):
    message: str
    status_code: int | None = None
    code: str | None = None

    def __str__(self) -> str:
        details: list[str] = []
        if self.status_code is not None:
            details.append(f"HTTP {self.status_code}")
        if self.code:
            details.append(self.code)
        if details:
            return f"{' / '.join(details)}: {self.message}"
        return self.message


class TBankClient:
    def __init__(
        self,
        token: str,
        *,
        base_url: str = TINVEST_BASE_URL,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
        trust_env: bool = False,
        verify_ssl: bool = True,
        ca_bundle_path: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        if not token or not token.strip():
            raise ValueError("Missing T-Invest token.")
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.session = session or requests.Session()
        self.session.trust_env = trust_env
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token.strip()}",
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )
        self._configure_transport(verify_ssl=verify_ssl, ca_bundle_path=ca_bundle_path)

    def _configure_transport(self, *, verify_ssl: bool, ca_bundle_path: str | None) -> None:
        if not verify_ssl:
            self.session.verify = False
            return

        ssl_context = ssl.create_default_context()
        bundle_path = merged_ca_bundle_path(ca_bundle_path)
        ssl_context.load_verify_locations(cafile=bundle_path)

        adapter = SSLContextAdapter(ssl_context)
        self.session.mount("https://", adapter)
        self.session.verify = bundle_path

    def post(self, method: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        try:
            response = self.session.post(
                f"{self.base_url}/{method}",
                json=payload or {},
                timeout=self.timeout_seconds,
            )
        except requests.exceptions.SSLError as error:
            raise TBankApiError(
                message=(
                    "SSL verification failed while connecting to T-Invest API. "
                    "Try the system certificate store default, or set TINVEST_CA_BUNDLE "
                    "in Streamlit secrets to your corporate/root CA PEM file."
                )
            ) from error
        except requests.exceptions.RequestException as error:
            raise TBankApiError(message=str(error)) from error
        try:
            response_payload = response.json()
        except ValueError:
            response_payload = {}

        if response.status_code >= 400:
            raise TBankApiError(
                message=str(first_present(response_payload, "message", "description") or response.reason or "T-Bank API request failed."),
                status_code=response.status_code,
                code=str(first_present(response_payload, "code", "errorCode") or "") or None,
            )

        if isinstance(response_payload, dict) and first_present(response_payload, "code", "errorCode"):
            raise TBankApiError(
                message=str(first_present(response_payload, "message", "description") or "T-Bank API returned an error."),
                status_code=response.status_code,
                code=str(first_present(response_payload, "code", "errorCode")),
            )

        return response_payload if isinstance(response_payload, dict) else {}
