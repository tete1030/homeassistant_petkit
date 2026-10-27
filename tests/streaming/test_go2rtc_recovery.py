"""Regression tests for shared go2rtc source registration and cleanup."""

# ruff: noqa: SLF001

import asyncio
import importlib
from pathlib import Path
import sys
import types
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest


@pytest.fixture
def mod():
    """Import the complete camera module in an isolated package."""
    name = "_petkit_recovery_tests"
    p = types.ModuleType(name)
    p.__path__ = [
        str(Path(__file__).resolve().parents[2] / "custom_components" / "petkit")
    ]
    sys.modules[name] = p
    m = importlib.import_module(name + ".camera")
    yield m
    for k in list(sys.modules):
        if k == name or k.startswith(name + "."):
            del sys.modules[k]


def camera(m):
    """Create a camera with only its external dependencies mocked."""
    c = object.__new__(m.PetkitWebRTCCamera)
    c.hass = SimpleNamespace(data={})
    c.device = SimpleNamespace(id=12345678)
    c._go2rtc_registration_lock = asyncio.Lock()
    c._go2rtc_registration_active = False
    c._remove_ice_servers = None
    c.async_on_remove = Mock()
    c._async_close_stream = AsyncMock()
    return c


@pytest.mark.asyncio
async def test_registers_after_startup_unavailable_and_go2rtc_restart(mod):
    """Retry registration when go2rtc becomes available or restarts."""
    c = camera(mod)
    m = SimpleNamespace(
        is_available=Mock(return_value=False),
        async_ensure_stream=AsyncMock(),
        async_remove_stream=AsyncMock(),
    )
    with (
        patch.object(
            mod.PetkitCameraBaseEntity, "async_added_to_hass", new=AsyncMock()
        ),
        patch.object(
            mod.PetkitCameraBaseEntity, "async_will_remove_from_hass", new=AsyncMock()
        ),
        patch.object(mod, "get_go2rtc_stream_manager", return_value=m),
        patch.object(mod, "async_register_ice_servers", return_value=lambda: None),
        patch.object(
            mod, "async_track_time_interval", return_value=lambda: None
        ) as timer,
    ):
        await c.async_added_to_hass()
        m.async_ensure_stream.assert_not_awaited()
        callback = timer.call_args.args[1]
        m.is_available.return_value = True
        await callback(None)
        await callback(None)
        assert m.async_ensure_stream.await_count == 2
        await c.async_will_remove_from_hass()
        await callback(None)
        assert m.async_ensure_stream.await_count == 2
        m.async_remove_stream.assert_awaited_once_with(c)


@pytest.mark.asyncio
async def test_overlapping_retry_and_removal_are_serialized(mod):
    """Avoid overlapping retries and registration after entity removal."""
    c = camera(mod)
    c._go2rtc_registration_active = True
    started = asyncio.Event()
    release = asyncio.Event()

    async def ensure(_):
        started.set()
        await release.wait()

    m = SimpleNamespace(
        is_available=Mock(return_value=True),
        async_ensure_stream=AsyncMock(side_effect=ensure),
        async_remove_stream=AsyncMock(),
    )
    with (
        patch.object(
            mod.PetkitCameraBaseEntity, "async_will_remove_from_hass", new=AsyncMock()
        ),
        patch.object(mod, "get_go2rtc_stream_manager", return_value=m),
    ):
        c.hass.data = {"petkit": {"cameras": {"12345678": c}}}
        t = asyncio.create_task(c._async_refresh_go2rtc_stream(None))
        await started.wait()
        await c._async_refresh_go2rtc_stream(None)
        assert m.async_ensure_stream.await_count == 1
        remove = asyncio.create_task(c.async_will_remove_from_hass())
        await asyncio.sleep(0)
        m.async_remove_stream.assert_not_awaited()
        release.set()
        await t
        await remove
        m.async_remove_stream.assert_awaited_once_with(c)
        await c._async_refresh_go2rtc_stream(None)
        assert m.async_ensure_stream.await_count == 1


@pytest.mark.asyncio
async def test_registration_exception_does_not_stop_later_retry(mod):
    """A transient registration exception does not disable future attempts."""
    c = camera(mod)
    c._go2rtc_registration_active = True
    m = SimpleNamespace(
        is_available=Mock(return_value=True),
        async_ensure_stream=AsyncMock(side_effect=[RuntimeError("offline"), None]),
    )
    with patch.object(mod, "get_go2rtc_stream_manager", return_value=m):
        await c._async_refresh_go2rtc_stream(None)
        await c._async_refresh_go2rtc_stream(None)
        assert m.async_ensure_stream.await_count == 2


@pytest.mark.asyncio
async def test_remove_uses_src_and_confirms_absence(mod):
    """Delete the target using the supported parameter and verify removal."""
    module = importlib.import_module("_petkit_recovery_tests.go2rtc_stream")
    hass = SimpleNamespace(data={})
    manager = module.PetkitGo2RTCStreamManager(hass)
    device = SimpleNamespace(device=SimpleNamespace(id=12345678), coordinator=object())
    manager.configured_url = Mock(return_value="http://go2rtc/")
    existing = {"petkit_12345678": {}}

    async def api(base, method, params):
        if method == "delete" and params.get("src") == "petkit_12345678":
            existing.pop("petkit_12345678", None)
        return 200, None

    manager._async_call_api = AsyncMock(side_effect=api)
    manager._async_get_streams = AsyncMock(side_effect=lambda _: dict(existing))
    assert await manager.async_remove_stream(device)
    assert not existing
    manager._async_call_api.assert_awaited_once_with(
        "http://go2rtc/", "delete", {"src": "petkit_12345678"}
    )


@pytest.mark.asyncio
async def test_remove_does_not_treat_noop_200_as_success(mod):
    """An HTTP success without actual removal is not treated as success."""
    module = importlib.import_module("_petkit_recovery_tests.go2rtc_stream")
    manager = module.PetkitGo2RTCStreamManager(SimpleNamespace(data={}))
    device = SimpleNamespace(device=SimpleNamespace(id=12345678), coordinator=object())
    manager.configured_url = Mock(return_value="http://go2rtc/")
    manager._async_call_api = AsyncMock(return_value=(200, None))
    manager._async_get_streams = AsyncMock(return_value={"petkit_12345678": {}})
    assert not await manager.async_remove_stream(device)
