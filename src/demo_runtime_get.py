"""Scoped ordinary DEMO GET reuse, after the caller's locks and local gates.

Only the two explicit HTTPS proxy fields or two public Windows static-proxy
values are selected. No credentials, proxy discovery, PAC, registry mutation,
account request, or activation happens at import. The existing GET pool keeps
its single-attempt transport policy; client retry budgets remain authoritative.
"""
from __future__ import annotations

from contextlib import contextmanager
import os
from urllib.parse import urlsplit

from .demo_get_pool import DemoGetPool


WINDOWS_PROXY_KEY = r"Software\Microsoft\Windows\CurrentVersion\Internet Settings"


class DemoRuntimeGetRefused(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def selected_proxy_url():
    # Deliberately do not enumerate environment fields or use urllib discovery.
    lower = os.environ.get("https_proxy")
    upper = os.environ.get("HTTPS_PROXY")
    if lower is not None and upper is not None and lower != upper:
        raise DemoRuntimeGetRefused("DEMO_RUNTIME_PROXY_ENV_AMBIGUOUS")
    if lower is not None or upper is not None:
        return lower if lower is not None else upper
    return windows_static_proxy_url()


def parse_windows_proxy_server(value):
    """Reviewed static HTTP(S) syntax; errors never include the address."""
    code = "DEMO_RUNTIME_WINDOWS_PROXY_CONFIG_INVALID"
    if (type(value) is not str or not 1 <= len(value) <= 2048 or not value.isascii()
            or any(not 32 < ord(char) < 127 for char in value)):
        raise DemoRuntimeGetRefused(code)

    def address(raw):
        url = raw if "://" in raw else "http://" + raw
        try:
            parts = urlsplit(url)
            if (len(url) > 2048 or parts.scheme not in {"http", "https"} or not parts.hostname
                    or parts.username is not None or parts.password is not None
                    or parts.path not in {"", "/"} or parts.query or parts.fragment
                    or "?" in url or "#" in url or "\\" in url
                    or "%" in parts.netloc or parts.netloc.endswith(":")):
                raise DemoRuntimeGetRefused(code)
            if parts.port is not None and not 1 <= parts.port <= 65535:
                raise DemoRuntimeGetRefused(code)
        except DemoRuntimeGetRefused:
            raise
        except Exception:
            raise DemoRuntimeGetRefused(code) from None
        return url

    if "=" not in value:
        if ";" in value:
            raise DemoRuntimeGetRefused(code)
        return address(value)
    choices = {}
    for item in value.split(";"):
        if item.count("=") != 1:
            raise DemoRuntimeGetRefused(code)
        scheme, raw = item.split("=", 1)
        scheme = scheme.lower()
        if scheme not in {"http", "https"} or scheme in choices or not raw:
            raise DemoRuntimeGetRefused(code)
        choices[scheme] = address(raw)
    if "https" not in choices:
        raise DemoRuntimeGetRefused(code)
    return choices["https"]


def windows_static_proxy_url():
    """Read exactly ProxyEnable and, only if enabled, ProxyServer, query-only."""
    try:
        import winreg
        try:
            handle = winreg.OpenKey(winreg.HKEY_CURRENT_USER, WINDOWS_PROXY_KEY, 0,
                                    winreg.KEY_QUERY_VALUE)
        except FileNotFoundError:
            return None
        with handle:
            try:
                enabled, _ = winreg.QueryValueEx(handle, "ProxyEnable")
            except FileNotFoundError:
                return None
            if type(enabled) is not int or enabled not in {0, 1}:
                raise DemoRuntimeGetRefused("DEMO_RUNTIME_WINDOWS_PROXY_CONFIG_INVALID")
            if enabled == 0:
                return None
            try:
                server, _ = winreg.QueryValueEx(handle, "ProxyServer")
            except FileNotFoundError:
                raise DemoRuntimeGetRefused("DEMO_RUNTIME_WINDOWS_PROXY_CONFIG_INVALID") from None
        return parse_windows_proxy_server(server)
    except DemoRuntimeGetRefused:
        raise
    except Exception:
        raise DemoRuntimeGetRefused("DEMO_RUNTIME_WINDOWS_PROXY_READ_FAILED") from None


@contextmanager
def runtime_demo_get():
    """Keep a GET-only pool alive until the caller's safety cleanup finishes."""
    with DemoGetPool(proxy_url=selected_proxy_url()) as pool:
        yield pool.get
