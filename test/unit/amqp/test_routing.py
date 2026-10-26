#  Copyright 2026 EPAM Systems
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

import pytest

from app.amqp.routing import (
    MAIN_HANDLER_SERVICES,
    empty_reply,
    main_handler_services,
    main_routing_key_predicate,
    to_list,
    unknown_routing_keys,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, []),
        ("", []),
        (" , ,", []),
        ("suggest", ["suggest"]),
        (" suggest, index_suggest_info ,,delete ", ["suggest", "index_suggest_info", "delete"]),
    ],
)
def test_to_list(value, expected) -> None:
    assert to_list(value) == expected


def test_predicate_without_allowlist_keeps_all_but_train() -> None:
    predicate = main_routing_key_predicate([])
    assert all(predicate(key) for key in MAIN_HANDLER_SERVICES)
    assert predicate("unknown_key")
    assert not predicate("train_models")


def test_predicate_with_allowlist() -> None:
    predicate = main_routing_key_predicate(["suggest", "delete", "train_models"])
    assert predicate("suggest")
    assert predicate("delete")
    assert not predicate("analyze")
    assert not predicate("index")
    assert not predicate("train_models")


def test_services_without_allowlist() -> None:
    assert main_handler_services([]) == MAIN_HANDLER_SERVICES


def test_services_with_allowlist() -> None:
    assert main_handler_services(["update_suggest_info", "suggest", "unknown"]) == ["suggest", "update_suggest_info"]


def test_unknown_routing_keys() -> None:
    assert unknown_routing_keys(["suggest", "sugest", "train_models"]) == ["sugest", "train_models"]


def test_empty_reply() -> None:
    assert empty_reply("analyze") == "[]"
    assert empty_reply("clean") == "0"
    assert empty_reply("delete") == "0"
    assert empty_reply("index") is None
    assert empty_reply("train_models") is None
