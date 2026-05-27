# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Consolidated thread-safe registry for operation counters and auditing."""

from datetime import datetime
import json
import logging
import os
import threading
import time
import types
from typing import Any, TextIO

logger = logging.getLogger(__name__)


class MetricsRegistry:
  """Orchestrates unified, thread-safe runtime incrementing for diagnostics."""

  def __init__(self) -> None:
    self._lock = threading.Lock()
    self.total_api_calls: int = 0
    self.total_api_latency: float = 0.0
    self.status_20x: int = 0
    self.status_4xx: int = 0
    self.status_5xx: int = 0
    self.total_root_msgs: int = 0
    self.total_reply_msgs: int = 0
    self._non_200_codes: dict[int, int] = {}
    self.audit_log: dict[str, dict[str, Any]] = {
        "status_summary": {},
        "channel_messages": {},
        "private_chat_messages": {},
    }

  def record_api_call(
      self, latency: float, status_code: int | None = None
  ) -> None:
    """Increments diagnostic counters based on observed connection metrics."""
    with self._lock:
      self.total_api_calls += 1
      self.total_api_latency += latency
      if status_code:
        if status_code != 200:
          self._non_200_codes[status_code] = (
              self._non_200_codes.get(status_code, 0) + 1
          )
        if 200 <= status_code < 300:
          self.status_20x += 1
        elif 400 <= status_code < 500:
          self.status_4xx += 1
        elif 500 <= status_code < 600:
          self.status_5xx += 1

  def increment_message_counts(self, root_count: int, reply_count: int) -> None:
    """Accumulates root and threaded sub-message tracking sums atomically."""
    with self._lock:
      self.total_root_msgs += root_count
      self.total_reply_msgs += reply_count

  def get_snapshot(self) -> dict[str, Any]:
    """Yields safe copy snapshot of cumulative performance tallies."""
    with self._lock:
      return {
          "total_api_calls": self.total_api_calls,
          "total_api_latency": self.total_api_latency,
          "status_20x": self.status_20x,
          "status_4xx": self.status_4xx,
          "status_5xx": self.status_5xx,
          "total_root_msgs": self.total_root_msgs,
          "total_reply_msgs": self.total_reply_msgs,
          "non_200_codes": self._non_200_codes.copy(),
      }


class AuditLogger:
  """Encapsulates serial trace logging with minimal locked disk I/O footprint."""

  output_dir: str
  _lock: threading.Lock
  _handle: TextIO

  def __init__(self, output_dir: str) -> None:
    self.output_dir = output_dir
    self._lock = threading.Lock()
    os.makedirs(self.output_dir, exist_ok=True)
    target_path = os.path.join(self.output_dir, "audit_events.jsonl")
    # Open stream once on instantiation to avoid repeated system syscalls.
    self._handle = open(target_path, "a", encoding="utf-8")

  def log_event(self, payload: dict[str, Any]) -> None:
    """Pushes pre-parsed serialization through persistent stream."""
    copy_payload = payload.copy()
    copy_payload["_log_time"] = str(datetime.utcnow())
    log_line = json.dumps(copy_payload) + "\n"

    with self._lock:
      # Minimum time held in contention, strictly writing text to write buffer.
      self._handle.write(log_line)
      self._handle.flush()

  def close(self) -> None:
    """Commits the remainder of stream and releases file descriptors."""
    with self._lock:
      self._handle.close()

  def __enter__(self) -> "AuditLogger":
    """Context aware execution initiator."""
    return self

  def __exit__(
      self,
      unused_exc_type: type[BaseException] | None,
      unused_exc: BaseException | None,
      unused_tb: types.TracebackType | None,
  ) -> None:
    """Guaranteed graceful closure of the logging conduit."""
    self.close()


class PerformanceMonitor:
  """Background worker tracking throughput delta performance over set windows."""

  def __init__(
      self, metrics: MetricsRegistry, interval_sec: float = 300.0
  ) -> None:
    self.metrics = metrics
    self.interval_sec = interval_sec
    self._stop_event = threading.Event()
    self._thread: threading.Thread | None = None

  def _loop(self) -> None:
    """Target iteration function running strictly in distinct background context."""
    last_snap = self.metrics.get_snapshot()
    last_time = time.monotonic()
    last_calls = last_snap["total_api_calls"]
    # Maintain independent local tracking state to precisely delineate window
    # diff.
    last_non_200 = last_snap.get("non_200_codes", {}).copy()

    while not self._stop_event.wait(self.interval_sec):
      now = time.monotonic()
      curr_snap = self.metrics.get_snapshot()
      curr_calls = curr_snap["total_api_calls"]
      curr_non_200 = curr_snap.get("non_200_codes", {})

      delta_time_seconds = now - last_time
      queries_per_second = (
          (curr_calls - last_calls) / delta_time_seconds
          if delta_time_seconds > 0
          else 0.0
      )

      window_errors = {}
      for code, total in curr_non_200.items():
        prev_count = last_non_200.get(code, 0)
        diff = total - prev_count
        if diff > 0:
          window_errors[code] = diff

      logger.info(
          "QPS: %.2f, Error Histograms: %s", queries_per_second, window_errors
      )

      last_time = now
      last_calls = curr_calls
      last_non_200 = curr_non_200.copy()

  def __enter__(self) -> "PerformanceMonitor":
    self._stop_event.clear()
    self._thread = threading.Thread(target=self._loop, daemon=True)
    self._thread.start()
    return self

  def __exit__(
      self,
      unused_exc_type: type[BaseException] | None,
      unused_exc: BaseException | None,
      unused_tb: types.TracebackType | None,
  ) -> None:
    """Terminates background processing strictly without blocking active threads."""
    self._stop_event.set()
    if self._thread:
      self._thread.join(timeout=1.0)
