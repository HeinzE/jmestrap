"""
jmestrap_client -- Python client for the JMESTrap REST API.

    from jmestrap_client import JmesTrap, Until

    trap = JmesTrap("http://127.0.0.1:9000")
    rec = trap.record(
        sources=["sensor_1"],
        until=Until.order("event == 'start'", "event == 'done'"),
    )
    rec.fetch(timeout=15)
    assert rec.completed

Suggestion for Pytest fixture pattern:

    @pytest.fixture(scope="session")
    def trap():
        t = JmesTrap("http://127.0.0.1:9000")
        yield t
        t.cleanup()

    def test_login_logout(trap, keyboard):
        rec = trap.record(until=Until.order("event=='login'", "event=='logout'"))
        keyboard.login(1743)
        rec.assert_completed(timeout=15, msg="login sequence incomplete")

Requires: requests
"""

from __future__ import annotations

import requests


# =============================================================================
# Until
# =============================================================================

class Until:
    """Recording completion pattern.

    Composes JMESPath predicates into an event pattern that determines when
    a recording finishes. Use the static constructors:

        Until.order("event == 'start'", "event == 'done'")
        Until.any_order("temp > `30`", "humidity < `20`")
    """

    __slots__ = ("_spec",)

    def __init__(self, spec: dict):
        self._spec = spec

    @staticmethod
    def order(*predicates: str) -> Until:
        """Predicates must match in sequence."""
        return Until({"type": "order", "predicates": list(predicates)})

    @staticmethod
    def any_order(*predicates: str) -> Until:
        """All predicates must match, in any order."""
        return Until({"type": "any_order", "predicates": list(predicates)})

    @property
    def spec(self) -> dict:
        return self._spec


# =============================================================================
# Recording
# =============================================================================

class Recording:
    """Handle to a recording on the JMESTrap server.

    Returned by :meth:`JmesTrap.record`.  Not instantiated directly.
    """

    def __init__(self, reference: int, client: JmesTrap):
        self._ref = reference
        self._client = client
        self._events: list = []
        self._status: str = "running"
        self._until_progress: dict | None = None

    # -- server interaction ---------------------------------------------------

    def fetch(self, timeout: float = 10.0) -> Recording:
        """Long-poll for recording completion.

        Blocks up to *timeout* seconds server-side.  Returns self for
        chaining.  Safe to call repeatedly; each call replaces local
        state from the server response.

        Raises :class:`requests.HTTPError` on 404 or other server errors.
        """
        r = self._client._get(
            f"/recordings/{self._ref}",
            params={"timeout": timeout},
            req_timeout=timeout + 5,
        )
        body = r.json()
        self._events = body["events"]
        self._status = body["status"]
        self._until_progress = body.get("until")
        return self

    def stop(self) -> Recording:
        """Ask the server to stop this recording early."""
        self._client._post(f"/recordings/{self._ref}/stop")
        return self

    def delete(self) -> None:
        """Delete this recording from the server."""
        self._client._delete(f"/recordings/{self._ref}")

    # -- assertion helper -----------------------------------------------------

    def assert_completed(self, timeout: float, msg: str = "") -> Recording:
        """Fetch and raise :class:`AssertionError` if recording did not complete.

        The error message includes the current status and predicate progress.
        """
        self.fetch(timeout)
        if self._status != "completed":
            status_line = f"Recording {self._ref} status is '{self._status}' after {timeout}s (expected 'completed')"
            detail = self._format_progress()
            parts = [s for s in (msg, status_line, detail) if s]
            raise AssertionError("\n".join(parts))
        return self

    # -- properties -----------------------------------------------------------

    @property
    def reference(self) -> int:
        return self._ref

    @property
    def status(self) -> str:
        """Recording status: ``"running"``, ``"completed"``, or ``"stopped"``."""
        return self._status

    @property
    def completed(self) -> bool:
        """True after all completion predicates have been satisfied."""
        return self._status == "completed"

    @property
    def events(self) -> list:
        """Recorded events as returned by the server."""
        return self._events

    @property
    def until_progress(self) -> dict | None:
        """Raw predicate progress from the server, or None.

        Structure (when present):
            {"type": "order"|"any_order",
             "predicates": [{"index": N, "expr": "...", "matched": bool}, ...]}
        """
        return self._until_progress

    # -- internal -------------------------------------------------------------

    def _format_progress(self) -> str:
        """Plain-text summary of predicate match state.

        Returns empty string when no completion pattern is present.
        """
        if not self._until_progress:
            return ""
        lines = ["Predicate progress:"]
        for p in self._until_progress["predicates"]:
            mark = "OK" if p["matched"] else "FAIL"
            lines.append(f"  [{p['index']:>2}] {mark:4s}  {p['expr']}")
        return "\n".join(lines)

    # -- dunder ---------------------------------------------------------------

    def __repr__(self) -> str:
        return f"<Recording ref={self._ref} {self._status} events={len(self._events)}>"

    def __str__(self) -> str:
        header = repr(self)
        progress = self._format_progress()
        return f"{header}\n{progress}" if progress else header


# =============================================================================
# JmesTrap
# =============================================================================

class JmesTrap:
    """Client for the JMESTrap REST API.

        trap = JmesTrap("http://127.0.0.1:9000")
        rec = trap.record(
            sources=["station_north"],
            until=Until.order("event=='temp'", "value > `30`"),
        )
        assert rec.fetch(timeout=15).completed
    """

    def __init__(self, base_url: str = "http://127.0.0.1:9000"):
        self._base = base_url.rstrip("/")
        self._session = requests.Session()

    def ping(self) -> dict:
        """Health check.  Raises on failure."""
        return self._get("/ping").json()

    def record(
        self,
        sources: list[str] | None = None,
        matching: str | None = None,
        until: Until | None = None,
        description: str = "",
    ) -> Recording:
        """Start a recording and return a :class:`Recording` handle.

        :param sources:     Record only from these sources (None = all).
        :param matching:    JMESPath predicate; only matching events are kept.
        :param until:       Completion condition built with :class:`Until`.
        :param description: Human-readable label stored server-side.
        """
        body: dict = {}
        if description:
            body["description"] = description
        if sources:
            body["sources"] = sources
        if matching:
            body["matching"] = matching
        if until:
            body["until"] = until.spec

        r = self._session.post(f"{self._base}/recordings", json=body)
        if r.status_code == 422:
            raise ValueError(r.json().get("error", "Invalid recording request"))
        r.raise_for_status()
        return Recording(r.json()["reference"], self)

    def inject(self, source: str, payload: dict) -> None:
        """Push a synthetic event into the server.

        Useful for test checkpoints and sequencing.
        """
        self._post(f"/events/{source}", json=payload)

    def list_recordings(self) -> list[dict]:
        """Return metadata for every recording on the server."""
        return self._get("/recordings").json()["recordings"]

    def list_sources(self) -> list[dict]:
        """Return currently observed event source names."""
        return self._get("/sources").json()["sources"]

    def cleanup(self) -> None:
        """Delete all recordings.  Suitable for fixture teardown."""
        for rec in self.list_recordings():
            self._session.delete(f"{self._base}/recordings/{rec['reference']}")

    # -- internal transport ---------------------------------------------------

    def _get(self, path: str, params: dict | None = None, req_timeout: float = 10.0):
        r = self._session.get(f"{self._base}{path}", params=params, timeout=req_timeout)
        r.raise_for_status()
        return r

    def _post(self, path: str, json: dict | None = None):
        r = self._session.post(f"{self._base}{path}", json=json)
        r.raise_for_status()
        return r

    def _delete(self, path: str):
        r = self._session.delete(f"{self._base}{path}")
        r.raise_for_status()
        return r
