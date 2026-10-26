#  Copyright 2025 EPAM Systems
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(order=True)
class ProcessingItem:
    priority: int
    number: int
    routing_key: str = field(compare=False)
    reply_to: Optional[str] = field(compare=False)
    log_correlation_id: str = field(compare=False)
    msg_correlation_id: str = field(compare=False)
    item: Any = field(compare=False)
    send_time: Optional[float] = field(default=None, compare=False)
    retries: int = field(default=0, compare=False)
    delivery_tag: Optional[int] = field(default=None, compare=False)
    # The AMQP channel the message arrived on, used to settle it. Not sent to the worker process.
    channel: Any = field(default=None, compare=False, repr=False)

    def __getstate__(self) -> dict[str, Any]:
        """Drop the AMQP channel when the item is pickled for the worker process."""
        state = self.__dict__.copy()
        state["channel"] = None
        return state


@dataclass
class ProcessingResult:
    item: ProcessingItem
    result: Optional[Any]
    success: bool
    error: Optional[Exception] = None
