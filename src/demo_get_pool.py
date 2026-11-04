"""Optional fixed-DEMO GET hook; no credential/proxy discovery or activation.

Pass an open instance (or its get method) as DemoReadOnlyClient.http_get only
after separate deployment review. Each call makes at most one HTTP attempt.
Pool, connect and read waits are bounded separately; this is not a five-second
account-evidence guarantee. Existing lifecycle clocks remain authoritative.
"""
from __future__ import annotations

import ipaddress
import http.client
import json
import logging
import math
import re
import socket
import ssl
import threading
from urllib.parse import unquote_to_bytes, urlsplit

from ._vendor import urllib3
from .demo_transport import (DEFAULT_PATH_PREFIX, DEMO_BASE, DEMO_HEADER,
                             DEMO_HEADER_VALUE, DEMO_HOST, MAX_BODY,
                             MAX_TIMEOUT, NETWORK_FAILURE_CATEGORIES,
                             DemoTransportError)


_LOG = logging.getLogger(urllib3.__name__)
_LOG.handlers = [logging.NullHandler()]
_LOG.propagate = False
_HEADERS = frozenset({"ok-access-key", "ok-access-sign", "ok-access-timestamp",
                      "ok-access-passphrase", DEMO_HEADER, "accept", "user-agent", "accept-encoding"})
_PATH = re.compile(r"/api/v5/[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*\Z")
_DNS = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?\Z")
_NETWORK_CAUSE_LIMIT = 8
# Exact local class identities only. Error messages, socket addresses and URLs
# never participate in classification, including for classes with similar names.
_NETWORK_LEAVES = {
    socket.gaierror: "DNS",
    ssl.SSLCertVerificationError: "TLS_CERTIFICATE",
    ssl.SSLEOFError: "TLS_EOF",
    ssl.SSLZeroReturnError: "TLS",
    ssl.SSLError: "TLS",
    urllib3.exceptions.NameResolutionError: "DNS",
    urllib3.exceptions.ConnectTimeoutError: "CONNECT_TIMEOUT",
    urllib3.exceptions.ReadTimeoutError: "READ_TIMEOUT",
    urllib3.exceptions.EmptyPoolError: "POOL_WAIT_TIMEOUT",
    urllib3.exceptions.NewConnectionError: "CONNECTION",
    ConnectionError: "CONNECTION",
    ConnectionRefusedError: "CONNECTION",
    ConnectionResetError: "CONNECTION",
    ConnectionAbortedError: "CONNECTION",
    BrokenPipeError: "CONNECTION",
    EOFError: "CONNECTION",
    TimeoutError: "TIMEOUT",
    http.client.RemoteDisconnected: "PROTOCOL",
    http.client.IncompleteRead: "PROTOCOL",
    http.client.BadStatusLine: "PROTOCOL",
    urllib3.exceptions.IncompleteRead: "PROTOCOL",
    urllib3.exceptions.InvalidChunkLength: "PROTOCOL",
    urllib3.exceptions.ResponseNotChunked: "PROTOCOL",
}
_NETWORK_WRAPPERS = {
    urllib3.exceptions.ProxyError: "PROXY",
    urllib3.exceptions.MaxRetryError: "UNKNOWN",
    urllib3.exceptions.SSLError: "TLS",
    urllib3.exceptions.ProtocolError: "PROTOCOL",
}


def _network_kind(kind, classes):
    # Identity comparisons also avoid invoking an unknown metaclass's equality
    # or hash hooks while recognizing an otherwise untrusted exception class.
    return next((label for known, label in classes.items() if kind is known), None)


def _known_network_category(error):
    """Return a bounded diagnostic label; this classifier never dispatches a retry.

Only the pinned wrapper classes may expose a child. Explicit causes and their
known constructor fields must agree if both are present. Unknown children keep
the known outer category; unknown root classes, cycles, ambiguous children and
overlong chains are UNKNOWN. No arbitrary exception attributes are inspected.
    """
    seen = set()
    for _ in range(_NETWORK_CAUSE_LIMIT):
        kind = type(error)
        if id(error) in seen:
            return "UNKNOWN"
        seen.add(id(error))
        category = _network_kind(kind, _NETWORK_LEAVES)
        if category is not None:
            return category if category in NETWORK_FAILURE_CATEGORIES else "UNKNOWN"
        fallback = _network_kind(kind, _NETWORK_WRAPPERS)
        if fallback is None:
            return "UNKNOWN"
        children = []
        cause = BaseException.__getattribute__(error, "__cause__")
        if cause is not None:
            children.append(cause)
        if kind is urllib3.exceptions.ProxyError:
            children.append(BaseException.__getattribute__(error, "original_error"))
        elif kind is urllib3.exceptions.MaxRetryError:
            children.append(BaseException.__getattribute__(error, "reason"))
        else:
            args = BaseException.__getattribute__(error, "args")
            if type(args) is tuple:
                if kind is urllib3.exceptions.SSLError and len(args) == 1 and (
                        _network_kind(type(args[0]), _NETWORK_LEAVES) is not None
                        or _network_kind(type(args[0]), _NETWORK_WRAPPERS) is not None):
                    children.append(args[0])
                elif kind is urllib3.exceptions.ProtocolError and len(args) == 2 and (
                        _network_kind(type(args[1]), _NETWORK_LEAVES) is not None
                        or _network_kind(type(args[1]), _NETWORK_WRAPPERS) is not None):
                    children.append(args[1])
        children = [child for child in children if child is not None]
        if not children:
            return fallback if fallback in NETWORK_FAILURE_CATEGORIES else "UNKNOWN"
        if any(child is not children[0] for child in children):
            return "UNKNOWN"
        child = children[0]
        if (_network_kind(type(child), _NETWORK_LEAVES) is None
                and _network_kind(type(child), _NETWORK_WRAPPERS) is None):
            return fallback if fallback in NETWORK_FAILURE_CATEGORIES else "UNKNOWN"
        error = child
    return "UNKNOWN"


def _network_category(error):
    # Malformed known wrapper objects still retain the original network refusal.
    # No diagnostic failure is allowed to replace it with a different exception.
    try:
        return _known_network_category(error)
    except Exception:
        return "UNKNOWN"


def _network_refuse(error):
    refused = DemoTransportError("DEMO_NETWORK_ERROR")
    refused.network_category = _network_category(error)
    raise refused from None


def _refuse(code):
    raise DemoTransportError(code) from None


def _tls_context():
    # create_default_context() and urllib3's context factory honor SSLKEYLOGFILE.
    # This explicit client context never enables key logging from process state.
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.verify_mode = ssl.CERT_REQUIRED
    context.check_hostname = True
    context.hostname_checks_common_name = False
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_default_certs()
    context.keylog_filename = None
    return context


def _visible_ascii(value):
    return type(value) is str and value.isascii() and all(32 < ord(char) < 127 for char in value)


def _url(url):
    if not _visible_ascii(url) or len(url) > 8192 or "\\" in url or "#" in url:
        _refuse("DEMO_POOL_URL_INVALID")
    try:
        parts = urlsplit(url)
        if (parts.scheme != "https" or parts.netloc != DEMO_HOST or parts.username is not None
                or parts.password is not None or parts.fragment or not parts.path.startswith(DEFAULT_PATH_PREFIX)
                or _PATH.fullmatch(parts.path) is None or not url.startswith(DEMO_BASE + DEFAULT_PATH_PREFIX)):
            _refuse("DEMO_BASE_LOCKED")
        if re.search(r"%(?![0-9A-Fa-f]{2})", parts.query):
            _refuse("DEMO_POOL_URL_INVALID")
        if any(value < 32 or value == 127 for value in unquote_to_bytes(parts.query)):
            _refuse("DEMO_POOL_URL_INVALID")
    except DemoTransportError:
        raise
    except Exception:
        _refuse("DEMO_POOL_URL_INVALID")
    return url


def _headers(headers):
    if type(headers) is not dict or not 1 <= len(headers) <= len(_HEADERS):
        _refuse("DEMO_POOL_HEADERS_INVALID")
    result, names = {}, set()
    for name, value in headers.items():
        if (type(name) is not str or not name.isascii() or name.lower() not in _HEADERS or name.lower() in names
                or type(value) is not str or not value or len(value) > 4096
                or not value.isascii() or any(ord(char) < 32 or ord(char) == 127 for char in value)
                or name != name.strip() or value != value.strip()):
            _refuse("DEMO_POOL_HEADERS_INVALID")
        lower = name.lower()
        if lower == "accept-encoding" and value != "identity":
            _refuse("DEMO_POOL_HEADERS_INVALID")
        names.add(lower)
        result[name] = value
    simulated = next((value for name, value in result.items() if name.lower() == DEMO_HEADER), None)
    if simulated != DEMO_HEADER_VALUE:
        _refuse("DEMO_POOL_HEADERS_INVALID")
    if "accept-encoding" not in names:
        result["Accept-Encoding"] = "identity"
    return result


def _proxy(proxy_url):
    if not _visible_ascii(proxy_url) or len(proxy_url) > 2048 or "\\" in proxy_url or "#" in proxy_url:
        _refuse("DEMO_POOL_PROXY_INVALID")
    try:
        parts = urlsplit(proxy_url)
        host, port = parts.hostname, parts.port
        if (parts.scheme not in {"http", "https"} or not host or parts.username is not None
                or parts.password is not None or parts.query or parts.fragment or parts.path not in {"", "/"}
                or "%" in parts.netloc or parts.netloc.endswith(":")
                or (port is not None and not 1 <= port <= 65535)):
            _refuse("DEMO_POOL_PROXY_INVALID")
        try:
            ipaddress.ip_address(host)
        except ValueError:
            if (_DNS.fullmatch(host) is None or len(host) > 253
                    or any(not label or len(label) > 63 or label.startswith("-") or label.endswith("-")
                           for label in host.split("."))):
                _refuse("DEMO_POOL_PROXY_INVALID")
    except DemoTransportError:
        raise
    except Exception:
        _refuse("DEMO_POOL_PROXY_INVALID")
    return proxy_url, host


class DemoGetPool:
    """Thread-safe, shared 16-connection pool; no writes or automatic retries."""
    def __init__(self, *, proxy_url=None):
        proxy = None if proxy_url is None else _proxy(proxy_url)
        self._condition = threading.Condition()
        self._closed, self._cleared, self._active, self._close_failed = False, False, 0, False
        try:
            options = {"num_pools": 1, "maxsize": 16, "block": True, "retries": False,
                       "ssl_context": _tls_context(), "cert_reqs": ssl.CERT_REQUIRED}
            if proxy is None:
                self._manager = urllib3.PoolManager(**options)
            else:
                self._manager = urllib3.ProxyManager(proxy[0], proxy_ssl_context=_tls_context(),
                    use_forwarding_for_https=False, **options)
        except Exception:
            _refuse("DEMO_POOL_INIT_FAILED")

    def __enter__(self):
        with self._condition:
            if self._closed:
                _refuse("DEMO_POOL_CLOSED")
        return self

    def __exit__(self, *_):
        self.close()

    def close(self):
        with self._condition:
            if self._closed:
                while not self._cleared:
                    self._condition.wait()
                if self._close_failed:
                    _refuse("DEMO_POOL_CLOSE_FAILED")
                return
            self._closed = True
            while self._active:
                self._condition.wait()
        failed = False
        try:
            # clear() alone drops container references in this pinned urllib3.
            # Tracebacks/responses may retain a pool, so explicitly close every
            # owned idle pool before clearing the manager. No get remains active
            # and the closed flag prevents creation of another target pool.
            for key in list(self._manager.pools.keys()):
                pool = self._manager.pools.get(key)
                if pool is not None:
                    try:
                        pool.close()
                    except Exception:
                        failed = True
        except Exception:
            failed = True
        finally:
            try:
                self._manager.clear()
            except Exception:
                failed = True
            finally:
                with self._condition:
                    self._close_failed = failed
                    self._cleared = True
                    self._condition.notify_all()
        if failed:
            _refuse("DEMO_POOL_CLOSE_FAILED")

    def __call__(self, url: str, headers: dict, timeout: int):
        return self.get(url, headers, timeout)

    def get(self, url: str, headers: dict, timeout: int):
        url, headers = _url(url), _headers(headers)
        if type(timeout) not in {int, float} or not 0 < timeout <= MAX_TIMEOUT or not math.isfinite(timeout):
            _refuse("DEMO_POOL_TIMEOUT_INVALID")
        with self._condition:
            if self._closed:
                _refuse("DEMO_POOL_CLOSED")
            self._active += 1
        try:
            return self._get_once(url, headers, timeout)
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def _get_once(self, url, headers, timeout):
        response, consumed = None, False
        try:
            response = self._manager.request("GET", url, headers=headers, retries=False, redirect=False,
                timeout=urllib3.Timeout(total=timeout, connect=timeout, read=timeout), pool_timeout=timeout,
                preload_content=False, decode_content=False, release_conn=False, enforce_content_length=True)
            status = response.status
            if type(status) is not int or not 100 <= status <= 599:
                _refuse("DEMO_BAD_RESPONSE")
            if 300 <= status <= 399:
                _refuse("DEMO_REDIRECT_FORBIDDEN")
            if status != 200:
                raise DemoTransportError("DEMO_HTTP_ERROR", status) from None
            if response.headers.get("Content-Encoding") not in {None, "", "identity"}:
                _refuse("DEMO_BAD_RESPONSE")
            raw = response.read(MAX_BODY + 1, decode_content=False, cache_content=False)
            if not isinstance(raw, (bytes, bytearray)):
                _refuse("DEMO_BAD_RESPONSE")
            consumed = response.closed is True
            if len(raw) > MAX_BODY:
                _refuse("DEMO_RESPONSE_TOO_LARGE")
            if not consumed:
                _refuse("DEMO_BAD_RESPONSE")
            # EOF may already have returned the connection to urllib3's pool.
            # A semantic JSON failure must never close a reused connection.
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, ValueError):
                _refuse("DEMO_BAD_JSON")
            if not isinstance(payload, dict):
                _refuse("DEMO_BAD_RESPONSE")
            return status, payload
        except DemoTransportError:
            raise
        except Exception as error:
            _network_refuse(error)
        finally:
            if response is not None:
                failed = False
                if not consumed:
                    try:
                        response.close()
                    except Exception:
                        failed = True
                try:
                    response.release_conn()
                except Exception:
                    failed = True
                if failed:
                    _refuse("DEMO_POOL_RELEASE_FAILED")
