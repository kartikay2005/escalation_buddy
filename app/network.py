"""Internal event network to replace external n8n orchestration.

This module connects ingest -> ai_layer -> sheets through an internal queue,
with a background worker and a file-based inbox poller for continuous processing.
"""

from __future__ import annotations

import json
import logging
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional

try:
    from .ai_layer import process_escalation
except ImportError:
    from ai_layer import process_escalation

logger = logging.getLogger(__name__)


@dataclass
class QueueEvent:
    """Represents one event moving through the internal queue."""

    request_id: str
    payload: Dict[str, Any]
    source: str = "webhook"


class InternalEscalationNetwork:
    """Continuous in-process network that replaces n8n flow execution."""

    def __init__(self, inbox_dir: Optional[Path] = None, poll_interval_sec: int = 5):
        self._queue: queue.Queue[QueueEvent] = queue.Queue()
        self._running = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._poller_thread: Optional[threading.Thread] = None
        self._poll_interval_sec = poll_interval_sec
        self._inbox_dir = inbox_dir or (Path(__file__).resolve().parents[1] / "runtime" / "inbox")
        self._processed_dir = self._inbox_dir.parent / "processed"
        self._failed_dir = self._inbox_dir.parent / "failed"

    def start(self) -> None:
        """Start worker + inbox poller threads if not already started."""
        if self._running.is_set():
            return

        self._running.set()
        self._inbox_dir.mkdir(parents=True, exist_ok=True)
        self._processed_dir.mkdir(parents=True, exist_ok=True)
        self._failed_dir.mkdir(parents=True, exist_ok=True)

        self._worker_thread = threading.Thread(target=self._run_worker, name="internal-network-worker", daemon=True)
        self._poller_thread = threading.Thread(target=self._run_inbox_poller, name="internal-network-poller", daemon=True)

        self._worker_thread.start()
        self._poller_thread.start()
        logger.info("Internal escalation network started")

    def stop(self, timeout: float = 5.0) -> None:
        """Stop network threads gracefully.

        Args:
            timeout: Max seconds to wait for threads to finish.
        """
        if not self._running.is_set():
            return

        logger.info("Stopping internal escalation network...")
        self._running.clear()

        # Wait for threads to finish
        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=timeout)
            if self._worker_thread.is_alive():
                logger.warning("Worker thread did not stop within timeout")

        if self._poller_thread and self._poller_thread.is_alive():
            self._poller_thread.join(timeout=timeout)
            if self._poller_thread.is_alive():
                logger.warning("Poller thread did not stop within timeout")

        logger.info("Internal escalation network stopped")

    def is_running(self) -> bool:
        """Check if the network is currently running."""
        return self._running.is_set()

    def queue_size(self) -> int:
        """Return the current number of items in the queue."""
        return self._queue.qsize()

    def submit(self, payload: Dict[str, Any], source: str = "webhook") -> str:
        """Submit a new event into the internal queue.

        Args:
            payload: Incoming escalation payload.
            source: Source label for tracing.

        Returns:
            str: Internal request id.
        """
        request_id = str(uuid.uuid4())
        self._queue.put(QueueEvent(request_id=request_id, payload=payload, source=source))
        logger.info("Queued escalation request_id=%s source=%s", request_id, source)
        return request_id

    def _run_worker(self) -> None:
        """Continuously processes queued events via ai_layer."""
        while self._running.is_set():
            try:
                event = self._queue.get(timeout=1)
            except queue.Empty:
                continue

            try:
                process_escalation(event.payload)
                logger.info("Processed escalation request_id=%s source=%s", event.request_id, event.source)
            except Exception as exc:
                logger.exception("Failed processing request_id=%s: %s", event.request_id, exc)
            finally:
                self._queue.task_done()

    def _run_inbox_poller(self) -> None:
        """Polls local runtime inbox for JSON files and feeds them to queue.

        Expected file payload schema matches webhook payload.
        """
        while self._running.is_set():
            try:
                for file_path in sorted(self._inbox_dir.glob("*.json")):
                    self._handle_inbox_file(file_path)
            except Exception as exc:
                logger.exception("Inbox poller error: %s", exc)

            time.sleep(self._poll_interval_sec)

    def _handle_inbox_file(self, file_path: Path) -> None:
        """Load one inbox file and route it to processing queue."""
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
            self.submit(payload, source="inbox")
            file_path.rename(self._processed_dir / file_path.name)
        except Exception as exc:
            logger.error("Failed handling inbox file %s: %s", file_path.name, exc)
            try:
                file_path.rename(self._failed_dir / file_path.name)
            except Exception:
                logger.exception("Could not move failed inbox file %s", file_path.name)


_network_singleton: Optional[InternalEscalationNetwork] = None


def get_network() -> InternalEscalationNetwork:
    """Return singleton internal network instance."""
    global _network_singleton
    if _network_singleton is None:
        _network_singleton = InternalEscalationNetwork()
    return _network_singleton
