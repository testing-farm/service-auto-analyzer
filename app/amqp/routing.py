#  Copyright 2026 EPAM Systems
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Routing key filters for AMQP request handlers."""

from typing import Callable, Optional

TRAIN_ROUTING_KEY = "train_models"

MAIN_HANDLER_SERVICES = [
    "index",
    "analyze",
    "delete",
    "clean",
    "search",
    "suggest",
    "cluster",
    "stats_info",
    "namespace_finder",
    "suggest_patterns",
    "index_suggest_info",
    "remove_suggest_info",
    "update_suggest_info",
    "remove_models",
    "get_model_info",
    "defect_update",
    "item_remove",
    "launch_remove",
    "remove_by_launch_start_time",
    "remove_by_log_time",
]


# Replies for RPC requests the main handler drops because of ANALYZER_ROUTING_KEYS. The API sends some RPC
# requests, such as "analyze", "clean" and "delete", to every analyzer exchange and waits for a reply from each,
# up to its reply timeout. The values match what the API expects from a handler that found nothing to do.
EMPTY_REPLIES: dict[str, str] = {
    "analyze": "[]",
    "search": "[]",
    "suggest": "[]",
    "defect_update": "[]",
    "clean": "0",
    "delete": "0",
    "item_remove": "0",
}


def to_list(value: Optional[str]) -> list[str]:
    """Split a comma-separated string into a list of non-empty, stripped items.

    :param value: comma-separated string
    :return: list of items
    """
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def except_train(request: str) -> bool:
    """Filters out requests that are for training models"""
    return request != TRAIN_ROUTING_KEY


def only_train(request: str) -> bool:
    """Filters out requests that are not related to training models"""
    return request == TRAIN_ROUTING_KEY


def main_routing_key_predicate(allowed_keys: list[str]) -> Callable[[str], bool]:
    """Create the routing key predicate for the main handler.

    :param allowed_keys: routing keys to process, an empty list means all keys except training
    :return: predicate that returns True for routing keys to process
    """
    if not allowed_keys:
        return except_train
    allowed = frozenset(allowed_keys)
    return lambda request: except_train(request) and request in allowed


def main_handler_services(allowed_keys: list[str]) -> list[str]:
    """Return the services the main handler initializes.

    :param allowed_keys: routing keys to process, an empty list means all keys except training
    :return: list of service names
    """
    if not allowed_keys:
        return list(MAIN_HANDLER_SERVICES)
    return [service for service in MAIN_HANDLER_SERVICES if service in allowed_keys]


def unknown_routing_keys(allowed_keys: list[str]) -> list[str]:
    """Return the routing keys the main handler does not serve.

    :param allowed_keys: routing keys to check
    :return: list of routing keys missing from the main handler services
    """
    return [key for key in allowed_keys if key not in MAIN_HANDLER_SERVICES]


def empty_reply(routing_key: str) -> Optional[str]:
    """Return the reply for a dropped RPC request.

    :param routing_key: routing key of the dropped request
    :return: reply body, or None when the handler should not reply
    """
    return EMPTY_REPLIES.get(routing_key)
