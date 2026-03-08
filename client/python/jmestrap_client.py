"""
jmestrap_client — Python client library for JMESTrap REST API.

Quick start:

    from jmestrap_client import JmesTrap, Until

    trap = JmesTrap("http://127.0.0.1:9000")
    rec = trap.record(
        sources=["sensor_1"],
        until=Until.order("event == 'start'", "event == 'done'"),
    )
    result = rec.fetch(timeout=15)
    assert result.finished

Pytest fixture pattern:

    @pytest.fixture(scope="session")
    def trap():
        t = JmesTrap("http://127.0.0.1:9000")
        yield t
        t.cleanup()

    def test_login_logout(trap, keyboard):
        rec = trap.record(until=Until.order("event=='login'", "event=='logout'"))
        keyboard.login(1743)
        rec.assert_finished(timeout=15, msg="login sequence incomplete")

Requires: requests (pip install requests)
"""

import requests


# =============================================================================
# Until — recording completion condition
# =============================================================================

class Until:
    """Recording completion condition.

    Use the static constructors:

        Until.order("event == 'start'", "event == 'done'")
        Until.any_order("temp > `30`", "humidity < `20`")
    """

    def __init__(self, spec):
        self.spec = spec

    @staticmethod
    def order(*predicates):
        """Predicates must match in sequence."""
        return Until({"type": "order", "predicates": list(predicates)})

    @staticmethod
    def any_order(*predicates):
        """All predicates must match, in any order."""
        return Until({"type": "any_order", "predicates": list(predicates)})


# =============================================================================
# Recording — handle to a live or completed recording
# =============================================================================

class Recording:
    """Handle to a recording on the JMESTrap server.

    Returned by JmesTrap.record(). Use .fetch(timeout=N) to wait for
    completion, then inspect .finished, .events, .until_progress.
    """

    def __init__(self, reference, client):
        self.reference = reference
        self._client = client
        self._events = []
        self._finished = False
        self._until_progress = None

    def fetch(self, timeout=10.0):
        """Long-poll for recording completion.

        Blocks up to `timeout` seconds waiting for the recording to
        finish (all `until` predicates satisfied). Returns self for
        chaining.

        Can be called multiple times — each call refreshes the state.
        Raises requests.HTTPError on 404 (recording deleted) or other errors.
        """
        r = self._client._session.get(
            f"{self._client._base}/recordings/{self.reference}",
            params={"timeout": timeout},
            timeout=timeout + 5,
        )
        r.raise_for_status()
        data = r.json()
        self._events = data["events"]
        self._finished = data["finished"]
        self._until_progress = data.get("until")
        return self

    @property
    def finished(self):
        """True if all `until` predicates were satisfied."""
        return self._finished

    @property
    def events(self):
        """List of recorded events (envelope objects with source and payload)."""
        return self._events

    @property
    def event_count(self):
        """Number of recorded events."""
        return len(self._events)

    @property
    def until_progress(self):
        """Server-reported predicate progress, or None.

        When present, a dict with:
            {"type": "order"|"any_order",
             "predicates": [{"index": N, "expr": "...", "matched": bool}, ...]}
        """
        return self._until_progress

    @property
    def failed_predicates(self):
        """List of until predicates that have NOT matched.

        Each entry: {"index": N, "expr": "..."}.
        Empty list if all matched or no until condition.

        Example:
            >>> rec.fetch(timeout=10)
            >>> rec.failed_predicates
            [{"index": 2, "expr": "event=='missing'"}]
        """
        if not self._until_progress:
            return []
        return [
            {"index": p["index"], "expr": p["expr"]}
            for p in self._until_progress["predicates"]
            if not p["matched"]
        ]

    def format_predicate_report(self):
        """Human-readable predicate match report.

        Example output:
            [Predicate Report]
              ✅ [ 1] "event=='login'"
              🔴 [ 2] "event=='logout'"
        """
        if not self._until_progress:
            return ""
        lines = ["", "[Predicate Report]"]
        for p in self._until_progress["predicates"]:
            mark = "✅" if p["matched"] else "🔴"
            lines.append(f"  {mark} [{p['index']:>2}] {p['expr']!r}")
        return "\n".join(lines)

    def assert_finished(self, timeout, msg=None):
        """Fetch and assert the recording finished.

        On failure, raises AssertionError with a predicate report showing
        which predicates matched and which did not. Intended for pytest.

        Example:
            rec.assert_finished(timeout=15, msg="login sequence incomplete")
        """
        self.fetch(timeout)
        if not self._finished:
            report = self.format_predicate_report()
            full_msg = f"{msg}\n{report}" if msg else report
            raise AssertionError(full_msg)
        return self

    def stop(self):
        """Manually stop a recording (before until predicates complete)."""
        r = self._client._session.post(
            f"{self._client._base}/recordings/{self.reference}/stop"
        )
        r.raise_for_status()
        return self

    def delete(self):
        """Delete this recording from the server."""
        r = self._client._session.delete(
            f"{self._client._base}/recordings/{self.reference}"
        )
        r.raise_for_status()

    def __repr__(self):
        state = "finished" if self._finished else "active"
        return f"<Recording ref={self.reference} {state} events={len(self._events)}>"

    def __str__(self):
        header = f"<Recording ref={self.reference} finished={self._finished}>"
        report = self.format_predicate_report()
        return header + report if report else header


# =============================================================================
# JmesTrap — client for the JMESTrap server
# =============================================================================

class JmesTrap:
    """Client for the JMESTrap REST API.

    Typical usage:

        trap = JmesTrap("http://127.0.0.1:9000")
        rec = trap.record(
            sources=["station_north"],
            until=Until.order("event=='temp'", "value > `30`"),
        )
        assert rec.fetch(timeout=15).finished
    """

    def __init__(self, base_url="http://127.0.0.1:9000"):
        self._base = base_url.rstrip("/")
        self._session = requests.Session()

    def ping(self):
        """Health check. Raises on failure."""
        r = self._session.get(f"{self._base}/ping")
        r.raise_for_status()
        return r.json()

    def record(self, sources=None, matching=None, until=None, description=""):
        """Start a recording. Returns a Recording handle.

        Args:
            sources:     List of source names to record from.
                         Empty/None = all sources.
            matching:    JMESPath predicate — only events where this
                         evaluates to true are recorded.
            until:       Until.order(...) or Until.any_order(...) — recording
                         finishes when all predicates are satisfied.
            description: Optional human-readable label for this recording.
        """
        body = {}
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
            raise ValueError(r.json().get("error", "Invalid request"))
        r.raise_for_status()
        ref = r.json()["reference"]
        return Recording(ref, self)

    def inject(self, source, payload):
        """Inject an event directly into the server.

        Useful for test checkpoints and synthetic events.

        Example:
            trap.inject("node_1", {"event": "start", "value": 1})
        """
        r = self._session.post(
            f"{self._base}/events/{source}", json=payload
        )
        r.raise_for_status()

    def list_recordings(self):
        """List all recordings on the server."""
        r = self._session.get(f"{self._base}/recordings")
        r.raise_for_status()
        return r.json()["recordings"]

    def list_sources(self):
        """List observed event sources."""
        r = self._session.get(f"{self._base}/sources")
        r.raise_for_status()
        return r.json()["sources"]

    def cleanup(self):
        """Delete all recordings. Useful in fixture teardown."""
        for rec in self.list_recordings():
            self._session.delete(
                f"{self._base}/recordings/{rec['reference']}"
            )
