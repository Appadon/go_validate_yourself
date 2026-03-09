import json
import os
import platform
import stat
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional


@dataclass
class DefaultSettings:
    """DefaultSettings stores server, binary, and request defaults for one Gvy client."""

    host: str = "127.0.0.1"
    port: int = 8080
    binary_path: str = "./gvy"
    request_timeout: Optional[float] = None
    startup_timeout: int = 30
    validate_auto_defaults: Dict[str, Any] = field(default_factory=dict)
    github_repo: str = "Appadon/go_validate_yourself"
    release_asset_name: str = "gvy"
    cache_dir: Optional[str] = None


class GvyError(RuntimeError):
    """GvyError represents SDK-level failures such as transport or process issues."""


class GvyHTTPError(GvyError):
    """GvyHTTPError represents a structured API error response from the server."""

    def __init__(self, status_code: int, payload: Dict[str, Any]):
        """__init__ stores the HTTP status code and decoded error payload."""
        self.status_code = status_code
        self.payload = payload
        message = payload.get("message", "request failed")
        super().__init__(f"HTTP {status_code}: {message}")


class Gvy:
    """Gvy provides a Python SDK for the local Gvy HTTP API and server process."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 8080,
        binary_path: str = "./gvy",
        request_timeout: Optional[float] = None,
        startup_timeout: int = 30,
        validate_auto_defaults: Optional[Dict[str, Any]] = None,
        github_repo: str = "Appadon/go_validate_yourself",
        release_asset_name: str = "gvy",
        cache_dir: Optional[str] = None,
        process: Optional[subprocess.Popen] = None,
    ) -> None:
        """__init__ creates a client instance with server defaults, binary settings, and process state."""
        self.settings = DefaultSettings(
            host=host,
            port=port,
            binary_path=binary_path,
            request_timeout=request_timeout,
            startup_timeout=startup_timeout,
            validate_auto_defaults=dict(validate_auto_defaults or {}),
            github_repo=github_repo,
            release_asset_name=release_asset_name,
            cache_dir=cache_dir,
        )
        self._process = process

    @classmethod
    def start(
        cls,
        host: str = "127.0.0.1",
        port: int = 8080,
        binary_path: str = "./gvy",
        request_timeout: Optional[float] = None,
        startup_timeout: int = 30,
        validate_auto_defaults: Optional[Dict[str, Any]] = None,
        github_repo: str = "Appadon/go_validate_yourself",
        release_asset_name: str = "gvy",
        cache_dir: Optional[str] = None,
    ) -> "Gvy":
        """start launches the local server process, waits for readiness, and returns a configured client."""
        client = cls(
            host=host,
            port=port,
            binary_path=binary_path,
            request_timeout=request_timeout,
            startup_timeout=startup_timeout,
            validate_auto_defaults=validate_auto_defaults,
            github_repo=github_repo,
            release_asset_name=release_asset_name,
            cache_dir=cache_dir,
        )
        client.start_server()
        client.wait_until_ready()
        return client

    @property
    def base_url(self) -> str:
        """base_url returns the full base URL for the configured local server."""
        return f"http://{self.settings.host}:{self.settings.port}"

    def start_server(self) -> subprocess.Popen:
        """start_server resolves or downloads the binary and launches it in server mode."""
        if self.is_process_running():
            return self._process

        binary_path = self._resolve_binary_path()
        if not binary_path.exists():
            binary_path = self.ensure_binary()

        self._process = subprocess.Popen(
            [
                str(binary_path),
                "-mode",
                "server",
                "-host",
                self.settings.host,
                "-port",
                str(self.settings.port),
            ]
            cwd=str(binary_path.parent),
        )
        return self._process

    def ensure_binary(self, force_download: bool = False) -> Path:
        """ensure_binary returns a usable binary path, downloading the Linux release asset when needed."""
        binary_path = self._resolve_binary_path()
        if binary_path.exists() and not force_download:
            self._ensure_executable(binary_path)
            return binary_path

        if not self._can_download_binary():
            raise GvyError(f"Gvy binary not found at {binary_path}")

        download_path = self._download_binary()
        self.settings.binary_path = str(download_path)
        return download_path

    def download_binary(self, force_download: bool = True) -> Path:
        """download_binary downloads the configured release asset into the local SDK cache."""
        return self.ensure_binary(force_download=force_download)

    def is_process_running(self) -> bool:
        """is_process_running reports whether this client owns a still-running server process."""
        return self._process is not None and self._process.poll() is None

    def wait_until_ready(self, timeout: Optional[int] = None, poll_interval: float = 0.5) -> Dict[str, Any]:
        """wait_until_ready polls the health endpoint until the server becomes ready or times out."""
        deadline = time.time() + (timeout or self.settings.startup_timeout)
        last_error: Optional[Exception] = None

        while time.time() < deadline:
            if self._process is not None and self._process.poll() is not None:
                raise GvyError(f"Gvy server exited early with code {self._process.returncode}")

            try:
                return self.health()
            except Exception as error:
                last_error = error
                time.sleep(poll_interval)

        raise GvyError(f"Gvy server did not become ready in time: {last_error}")

    def health(self) -> Dict[str, Any]:
        """health calls GET /health and returns the decoded JSON response."""
        return self._request("GET", "/health", timeout=5)

    def shutdown(self) -> Dict[str, Any]:
        """shutdown calls POST /shutdown and waits briefly for the owned process to exit."""
        response = self._request("POST", "/shutdown", payload={}, timeout=5)
        self.wait_for_exit(timeout=10)
        return response

    def wait_for_exit(self, timeout: int = 10) -> None:
        """wait_for_exit waits for the owned process to stop if this client started one."""
        if self._process is None:
            return
        try:
            self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired as error:
            raise GvyError("Gvy server did not exit in time") from error

    def close(self) -> None:
        """close shuts down the server if this client owns a running local process."""
        if not self.is_process_running():
            return
        try:
            self.shutdown()
        except Exception:
            self._process.terminate()
            self._process.wait(timeout=10)

    def set_validate_auto_defaults(self, **defaults: Any) -> None:
        """set_validate_auto_defaults updates reusable defaults for validate-auto requests."""
        self.settings.validate_auto_defaults.update(defaults)

    def run_validate_auto(self, **payload: Any) -> Dict[str, Any]:
        """run_validate_auto sends a POST /run/validate-auto request with merged defaults."""
        request_payload = dict(self.settings.validate_auto_defaults)
        request_payload.update(payload)
        return self._request(
            "POST",
            "/run/validate-auto",
            payload=request_payload,
            timeout=self.settings.request_timeout,
        )

    def _request(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """_request sends one JSON HTTP request and returns the decoded response body."""
        data = None
        headers = {"Content-Type": "application/json"}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")

        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )

        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as error:
            raw_body = error.read().decode("utf-8")
            try:
                payload = json.loads(raw_body)
            except json.JSONDecodeError:
                payload = {"message": raw_body or "request failed"}
            raise GvyHTTPError(error.code, payload) from error
        except urllib.error.URLError as error:
            raise GvyError(f"request failed: {error}") from error

    def _resolve_binary_path(self) -> Path:
        """_resolve_binary_path resolves the configured binary path relative to the package when needed."""
        binary_path = Path(self.settings.binary_path).expanduser()
        if binary_path.is_absolute():
            return binary_path
        return (Path(__file__).resolve().parent.parent / binary_path).resolve()

    def _can_download_binary(self) -> bool:
        """_can_download_binary enforces the current Linux-only download support."""
        return platform.system() == "Linux"

    def _download_binary(self) -> Path:
        """_download_binary downloads the configured GitHub release asset into the SDK cache directory."""
        if not self._can_download_binary():
            raise GvyError("automatic binary download is currently supported only on Linux")

        destination = self._cached_binary_path()
        destination.parent.mkdir(parents=True, exist_ok=True)
        download_url = self._binary_download_url()
        temp_file = None

        try:
            with urllib.request.urlopen(download_url, timeout=300) as response:
                with tempfile.NamedTemporaryFile(delete=False, dir=str(destination.parent)) as handle:
                    temp_file = Path(handle.name)
                    handle.write(response.read())

            temp_file.replace(destination)
            self._ensure_executable(destination)
            return destination
        except urllib.error.HTTPError as error:
            raise GvyError(f"failed downloading Gvy binary from {download_url}: HTTP {error.code}") from error
        except urllib.error.URLError as error:
            raise GvyError(f"failed downloading Gvy binary from {download_url}: {error}") from error
        finally:
            if temp_file is not None and temp_file.exists():
                temp_file.unlink(missing_ok=True)

    def _binary_download_url(self) -> str:
        """_binary_download_url returns the GitHub release download URL for the configured asset."""
        return (
            f"https://github.com/{self.settings.github_repo}/releases/latest/download/"
            f"{self.settings.release_asset_name}"
        )

    def _cached_binary_path(self) -> Path:
        """_cached_binary_path returns the cache location used for downloaded binaries."""
        cache_root = self.settings.cache_dir
        if cache_root is None:
            cache_root = os.environ.get("XDG_CACHE_HOME")
        if cache_root is None:
            cache_root = str(Path.home() / ".cache")
        return Path(cache_root) / "gvy-sdk" / self.settings.release_asset_name

    def _ensure_executable(self, binary_path: Path) -> None:
        """_ensure_executable marks the resolved binary as executable for the current user."""
        current_mode = binary_path.stat().st_mode
        binary_path.chmod(current_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    def __enter__(self) -> "Gvy":
        """__enter__ allows the client to be used as a context manager."""
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        """__exit__ closes the client when leaving a context manager scope."""
        self.close()
