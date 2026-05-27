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

"""Thread-safe rate limiting utilities."""

import asyncio
import threading
import time
import types


class HybridLimiter:
  """Rate limiter supporting both synchronous and asynchronous workflows."""

  requests_per_second: float
  lock: threading.Lock
  last_call: float

  def __init__(self, requests_per_second: float) -> None:
    if requests_per_second <= 0:
      raise ValueError("RPS must be strictly positive.")
    self.requests_per_second = requests_per_second
    self.lock = threading.Lock()
    self.last_call = 0.0

  def wait(self) -> None:
    """Synchronously wait to respect the rate limit."""
    with self.lock:
      now = time.monotonic()
      target_time = max(now, self.last_call + (1.0 / self.requests_per_second))
      self.last_call = target_time

    wait_time = target_time - now
    if wait_time > 0:
      time.sleep(wait_time)

  async def __aenter__(self) -> "HybridLimiter":
    """Enter the asynchronous rate limiter context."""
    with self.lock:
      now = time.monotonic()
      target_time = max(now, self.last_call + (1.0 / self.requests_per_second))
      self.last_call = target_time

    wait_time = target_time - now
    if wait_time > 0:
      await asyncio.sleep(wait_time)
    return self

  async def __aexit__(
      self,
      unused_exc_type: type[BaseException] | None,
      unused_exc: BaseException | None,
      unused_tb: types.TracebackType | None,
  ) -> None:
    """Exit the asynchronous rate limiter context."""
    pass
