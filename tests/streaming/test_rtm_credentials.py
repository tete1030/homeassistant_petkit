"""Account token maintenance must not repeatedly request device activation."""

# ruff: noqa: SLF001

import asyncio
from http import HTTPMethod
from types import SimpleNamespace
from unittest.mock import AsyncMock

from pypetkitapi.exceptions import PypetkitError
import pytest


async def test_only_matching_sender_can_receive_new_token(streaming):
    """Do not change peer identity or start transports while updating a token."""
    rtm = streaming.whep.AgoraRTMSignaling("synthetic-app")
    rtm._app_user_id = "viewer"
    rtm._device_user_id = "feeder"
    rtm._token = "old"
    assert not await rtm.update_rtm_token("other-viewer", "new")
    for user, token in ((None, "new"), ("viewer", None), ("", "new"), ("viewer", "  ")):
        assert not await rtm.update_rtm_token(user, token)
    assert rtm._token == "old"
    assert await rtm.update_rtm_token("viewer", "new")
    assert rtm._token == "new"
    assert rtm._device_user_id == "feeder"
    assert rtm._session is None
    assert rtm._heartbeat_task is None


@pytest.mark.parametrize(
    ("responses", "expected"),
    [
        (
            [{"appRtmUserId": "viewer", "rtmToken": "fresh", "deviceRtmInfo": []}],
            "fresh",
        ),
        ([{"appRtmUserId": "other", "rtmToken": "wrong"}], "old"),
        ([{"appRtmUserId": "viewer"}], "old"),
        ([None], "old"),
        (
            [
                PypetkitError("unavailable"),
                {"appRtmUserId": "viewer", "rtmToken": "recovered"},
            ],
            "recovered",
        ),
        (
            [TimeoutError(), {"appRtmUserId": "viewer", "rtmToken": "recovered"}],
            "recovered",
        ),
    ],
)
async def test_refresh_uses_account_endpoint_without_starting_video(
    streaming, monkeypatch, responses, expected
):
    """Keep refresh cancellable and recover after a failed credential request."""
    w = streaming.whep
    rtm = w.AgoraRTMSignaling("synthetic-app")
    rtm._app_user_id = "viewer"
    rtm._device_user_id = "feeder"
    rtm._token = "old"
    request = AsyncMock(side_effect=responses)
    client = SimpleNamespace(
        req=SimpleNamespace(request=request), get_session_id=AsyncMock(return_value={})
    )
    camera = SimpleNamespace(
        coordinator=SimpleNamespace(
            config_entry=SimpleNamespace(runtime_data=SimpleNamespace(client=client))
        ),
        async_get_live_feed=AsyncMock(
            side_effect=AssertionError("must not activate video")
        ),
    )
    session = SimpleNamespace(camera=camera, agora_rtm=rtm)
    remaining = len(responses)

    async def tick(delay):
        nonlocal remaining
        assert delay == w.TOKEN_REFRESH_INTERVAL_SECONDS
        if remaining == 0:
            raise asyncio.CancelledError
        remaining -= 1

    # Replace this module's clock edge only, preserving the actual coroutine body.
    manager = w.PetkitAgoraUpstreamManager(SimpleNamespace())
    monkeypatch.setattr(w, "asyncio", SimpleNamespace(sleep=tick))
    with pytest.raises(asyncio.CancelledError):
        await manager._refresh_tokens(session)
    assert request.await_count == len(responses)
    assert all(
        call.kwargs
        == {"method": HTTPMethod.GET, "url": "device/start/rtm", "headers": {}}
        for call in request.await_args_list
    )
    camera.async_get_live_feed.assert_not_awaited()
    assert rtm._token == expected
    assert rtm._heartbeat_task is None
    assert rtm._session is None
