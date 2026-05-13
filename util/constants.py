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

"""Constants for Migration Planner."""

GRAPH_BASE_URL: str = "https://graph.microsoft.com/v1.0"
TOKEN_URL_TEMPLATE: str = (
    "https://login.microsoftonline.com/{0}/oauth2/v2.0/token"
)
MAX_RETRIES: int = 10
BACKOFF: int = 2

# Chat Estimation Cost Weights (Seconds)
BLENDED_MSG_COST_SEC: float = 0.75
CHANNEL_COST_SEC: float = 10.0
MEMBERSHIP_COST_SEC: float = 1.0

# QPS Limits
CHANNEL_QPS: float = 2.0
MESSAGE_QPS: float = 26.0
MEMBERSHIP_QPS: float = 18.0
MAX_TEAMS_PER_BATCH: int = 1000
