"""Load complete streaming modules without bootstrapping the integration."""

import importlib
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace

import pytest


@pytest.fixture(scope="session")
def streaming():
    """Import real modules and dependencies, without running Petkit's setup."""
    name = "_petkit_streaming_tests"
    package = ModuleType(name)
    package.__path__ = [
        str(Path(__file__).resolve().parents[2] / "custom_components" / "petkit")
    ]
    # Use a separate package namespace, like the standalone schedule tests.
    # No production function bodies are copied, extracted, or replaced here.
    sys.modules[name] = package
    try:
        yield SimpleNamespace(
            sdp=importlib.import_module(f"{name}.agora_sdp"),
            ws=importlib.import_module(f"{name}.agora_websocket"),
            whep=importlib.import_module(f"{name}.whep_proxy"),
        )
    finally:
        for module in list(sys.modules):
            if module == name or module.startswith(f"{name}."):
                del sys.modules[module]
