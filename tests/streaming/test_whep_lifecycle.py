"""Exercise the real WHEP manager and views with fake external transports."""

# ruff: noqa: SLF001

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from aiohttp import web
import pytest


def camera(device_id="42"):
    """Create a camera boundary without credentials or a live API."""
    return SimpleNamespace(
        device=SimpleNamespace(id=device_id),
        async_get_live_feed=AsyncMock(
            return_value=SimpleNamespace(rtc_token="synthetic")
        ),
        async_refresh_agora_context=AsyncMock(return_value=object()),
        filter_agora_candidates=lambda candidates, response: candidates,
    )


async def wait_until(predicate):
    """Yield until an observed lifecycle transition, with a bounded test timeout."""
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0)


@pytest.fixture
async def env(streaming, monkeypatch):
    """Replace network edges, leaving real manager, locking and HTTP code intact."""
    state = SimpleNamespace(
        rtms=[],
        handlers=[],
        tasks=[],
        events=[],
        ready=True,
        lose_during_join=False,
        join_error=None,
        start_error=None,
        start_gate=None,
        join_gate=None,
        start_ok=True,
        answer="synthetic-answer",
    )

    def create_task(coro, name=None):
        task = asyncio.create_task(coro, name=name)
        state.tasks.append(task)
        return task

    state.hass = SimpleNamespace(
        data={},
        async_create_task=create_task,
        async_create_background_task=create_task,
    )

    def make_rtm(*args):
        instance = SimpleNamespace(
            id=len(state.rtms),
            stop_gate=None,
            stop_entered=asyncio.Event(),
            stopped=False,
        )

        async def start(feed):
            state.events.append(("start", instance.id))
            if state.start_gate is not None:
                await state.start_gate.wait()
            if state.start_error is not None:
                raise state.start_error
            return state.start_ok

        async def stop(*, send_stop):
            assert send_stop
            instance.stop_entered.set()
            state.events.append(("stop_begin", instance.id))
            if instance.stop_gate is not None:
                await instance.stop_gate.wait()
            instance.stopped = True
            state.events.append(("stop_end", instance.id))

        instance.start_live = AsyncMock(side_effect=start)
        instance.stop_live = AsyncMock(side_effect=stop)
        instance.update_tokens = AsyncMock()
        state.rtms.append(instance)
        return instance

    def make_handler(**kwargs):
        instance = SimpleNamespace(
            callback=kwargs["on_connection_lost"],
            candidates=[],
            has_negotiated_video=state.ready,
            entered=asyncio.Event(),
            disconnect=AsyncMock(),
        )

        async def join(**kwargs):
            instance.entered.set()
            if state.join_gate is not None:
                await state.join_gate.wait()
            if state.join_error is not None:
                raise state.join_error
            if state.lose_during_join:
                instance.callback()
            return state.answer

        instance.connect_and_join = AsyncMock(side_effect=join)
        state.handlers.append(instance)
        return instance

    monkeypatch.setattr(streaming.whep, "AgoraRTMSignaling", make_rtm)
    monkeypatch.setattr(streaming.whep, "AgoraWebSocketHandler", make_handler)
    state.manager = streaming.whep.PetkitAgoraUpstreamManager(state.hass)
    state.hass.data["petkit"] = {
        "whep_upstream_manager": state.manager,
        "cameras": {"42": camera()},
    }
    yield state
    for gate in [state.start_gate, state.join_gate, *(r.stop_gate for r in state.rtms)]:
        if gate is not None:
            gate.set()
    await state.manager.close_all()
    await asyncio.gather(*state.tasks, return_exceptions=True)
    assert all(rtm.stopped for rtm in state.rtms)
    assert all(task.done() for task in state.tasks)


async def test_success_registered(env):
    """Register only a signaling-ready session."""
    sid, answer = await env.manager.create_session(camera(), "")
    assert answer == "synthetic-answer"
    assert env.manager._sessions["42"].session_id == sid


async def test_sdp_without_publication_not_registered(env, streaming):
    """An SDP string is insufficient evidence of a published video track."""
    env.ready = False
    with pytest.raises(streaming.ws.AgoraVideoNotPublishedError):
        await env.manager.create_session(camera(), "")
    assert not env.manager._sessions
    assert env.rtms[0].stopped
    env.handlers[0].disconnect.assert_awaited_once()
    assert not env.tasks


async def test_no_video_timeout_propagates_and_cleans(env, streaming):
    """Preserve the precise no-publication error while releasing provisional I/O."""
    env.join_error = streaming.ws.AgoraVideoNotPublishedError("no publication")
    with pytest.raises(
        streaming.ws.AgoraVideoNotPublishedError, match="no publication"
    ):
        await env.manager.create_session(camera(), "")
    assert not env.manager._sessions
    assert env.rtms[0].stopped


async def test_lost_during_join_never_registers(env, streaming):
    """The loss callback can run before a session reaches the registry."""
    env.lose_during_join = True
    with pytest.raises(streaming.ws.AgoraVideoNotPublishedError):
        await env.manager.create_session(camera(), "")
    await asyncio.gather(*env.tasks)
    assert not env.manager._sessions
    assert env.rtms[0].stopped


async def test_old_callback_cannot_close_replacement(env):
    """A late callback belongs to its original session generation."""
    await env.manager.create_session(camera(), "")
    old = env.handlers[0]
    sid, _ = await env.manager.create_session(camera(), "")
    old.callback()
    await env.tasks[-1]
    assert env.manager._sessions["42"].session_id == sid
    assert not env.rtms[1].stopped


async def test_old_delete_cannot_close_replacement(env):
    """The HTTP resource identity must reach close_session."""
    old_sid, _ = await env.manager.create_session(camera(), "")
    new_sid, _ = await env.manager.create_session(camera(), "")
    assert not await env.manager.close_session("42", session_id=old_sid)
    assert await env.manager.close_session("42", session_id=new_sid)


async def test_replacement_waits_for_teardown(env):
    """The old stop_live must finish before a replacement sends start_live."""
    await env.manager.create_session(camera(), "")
    old = env.rtms[0]
    old.stop_gate = asyncio.Event()
    close = asyncio.create_task(env.manager.close_session("42"))
    await old.stop_entered.wait()
    new = asyncio.create_task(env.manager.create_session(camera(), ""))
    await asyncio.sleep(0)
    assert len(env.rtms) == 1
    old.stop_gate.set()
    await asyncio.gather(close, new)
    assert env.events.index(("stop_end", 0)) < env.events.index(("start", 1))


async def test_concurrent_creates_leave_one_owned_session(env):
    """Serialize creation, not just dictionary writes."""
    env.join_gate = asyncio.Event()
    first = asyncio.create_task(env.manager.create_session(camera(), ""))
    await wait_until(lambda: bool(env.handlers))
    second = asyncio.create_task(env.manager.create_session(camera(), ""))
    await asyncio.sleep(0)
    assert len(env.handlers) == 1
    env.join_gate.set()
    a, b = await asyncio.gather(first, second)
    assert a[0] != b[0]
    assert env.manager._sessions["42"].session_id == b[0]
    assert env.rtms[0].stopped


async def test_other_camera_not_blocked_by_teardown(env):
    """Lifecycle serialization is per device, not integration-wide."""
    await env.manager.create_session(camera(), "")
    old = env.rtms[0]
    old.stop_gate = asyncio.Event()
    close = asyncio.create_task(env.manager.close_session("42"))
    await old.stop_entered.wait()
    async with asyncio.timeout(2):
        await env.manager.create_session(camera("99"), "")
    old.stop_gate.set()
    await close


@pytest.mark.parametrize("phase", ["start", "join"])
async def test_cancelled_creation_cleans_provisional_resources(env, phase):
    """Cancellation during activation or negotiation must not leak heartbeats."""
    setattr(env, f"{phase}_gate", asyncio.Event())
    task = asyncio.create_task(env.manager.create_session(camera(), ""))
    await wait_until(lambda: bool(env.handlers))
    if phase == "join":
        await env.handlers[0].entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not env.manager._sessions
    assert env.rtms[0].stopped
    env.handlers[0].disconnect.assert_awaited_once()


async def test_repeated_cancellation_waits_for_cleanup(env):
    """Even repeated request cancellation cannot let a new start overtake stop."""
    await env.manager.create_session(camera(), "")
    old = env.rtms[0]
    old.stop_gate = asyncio.Event()
    close = asyncio.create_task(env.manager.close_session("42"))
    await old.stop_entered.wait()
    close.cancel()
    await asyncio.sleep(0)
    close.cancel()
    new = asyncio.create_task(env.manager.create_session(camera(), ""))
    await asyncio.sleep(0)
    assert len(env.rtms) == 1
    assert not close.done()
    old.stop_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await close
    await new
    assert env.events.index(("stop_end", 0)) < env.events.index(("start", 1))


async def test_failed_refresh_does_not_skip_transport_cleanup(env):
    """A completed refresh exception must not prevent stop_live/disconnect."""
    await env.manager.create_session(camera(), "")
    session = env.manager._sessions["42"]
    session.refresh_task.cancel()
    await asyncio.gather(session.refresh_task, return_exceptions=True)

    async def fail():
        raise OSError("synthetic refresh failure")

    session.refresh_task = asyncio.create_task(fail())
    await asyncio.sleep(0)
    await env.manager.close_session("42")
    assert env.rtms[0].stopped
    env.handlers[0].disconnect.assert_awaited_once()


async def test_start_failure_cleans(env):
    """Activation failures must clean resources created before connect_and_join."""
    env.start_error = OSError("synthetic transport failure")
    with pytest.raises(OSError, match="synthetic transport failure"):
        await env.manager.create_session(camera(), "")
    assert env.rtms[0].stopped


async def test_empty_answer_cleans(env):
    """A gateway returning no answer must release provisional resources."""
    env.answer = None
    with pytest.raises(RuntimeError, match="did not return an SDP answer"):
        await env.manager.create_session(camera(), "")
    assert not env.manager._sessions
    assert env.rtms[0].stopped


async def test_activation_warning_does_not_change_readiness_policy(env):
    """Retain the existing warning-only policy for a false activation result."""
    env.start_ok = False
    await env.manager.create_session(camera(), "")
    assert await env.manager.has_session("42")


async def test_loss_while_waiting_for_registration_cleans(env, streaming):
    """Recheck publication after acquiring the registry lock."""
    env.join_gate = asyncio.Event()
    create = asyncio.create_task(env.manager.create_session(camera(), ""))
    await wait_until(lambda: bool(env.handlers))
    async with env.manager._lock:
        env.join_gate.set()
        await asyncio.sleep(0)
        env.handlers[0].callback()
    with pytest.raises(streaming.ws.AgoraVideoNotPublishedError, match="lost before"):
        await create
    await asyncio.gather(*env.tasks)
    assert not env.manager._sessions
    assert env.rtms[0].stopped


async def test_close_all_drains_pending_creation_and_rejects_new(env):
    """An unload cannot miss a session that is still negotiating."""
    env.join_gate = asyncio.Event()
    create = asyncio.create_task(env.manager.create_session(camera(), ""))
    await wait_until(lambda: bool(env.handlers))
    close = asyncio.create_task(env.manager.close_all())
    await asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="manager is closing"):
        await env.manager.create_session(camera("99"), "")
    env.join_gate.set()
    with pytest.raises(RuntimeError, match="manager is closing"):
        await create
    await close
    assert not env.manager._sessions
    assert env.rtms[0].stopped


async def test_shutdown_also_rejects_already_queued_create(env):
    """A create waiting for the device lock must recheck the shutdown flag."""
    env.join_gate = asyncio.Event()
    first = asyncio.create_task(env.manager.create_session(camera(), ""))
    await wait_until(lambda: bool(env.handlers))
    queued = asyncio.create_task(env.manager.create_session(camera(), ""))
    await asyncio.sleep(0)
    close = asyncio.create_task(env.manager.close_all())
    await asyncio.sleep(0)
    env.join_gate.set()
    for task in (first, queued):
        with pytest.raises(RuntimeError, match="manager is closing"):
            await task
    await close
    assert len(env.rtms) == 1
    assert not env.manager._sessions


def request(env):
    """Use real aiohttp responses and the real authenticated view code."""
    result = Mock(spec=web.Request)
    result.app = {"hass": env.hass}
    result.path = "/api/petkit/whep_upstream/42"
    result.get.return_value = object()  # Already-authenticated Home Assistant user.
    result.text = AsyncMock(return_value="synthetic-offer")
    return result


@pytest.mark.parametrize("ready", [False, True])
async def test_http_created_only_with_announced_video(env, streaming, ready):
    """The actual WHEP view returns 502 rather than a false-success 201."""
    env.ready = ready
    response = await streaming.whep.PetkitUpstreamWhepView().post(request(env), "42")
    assert response.status == (201 if ready else 502)
    if ready:
        sid = env.manager._sessions["42"].session_id
        assert response.headers["Location"].endswith("/" + sid)
    else:
        assert "no active announced video" in response.text
        assert not env.manager._sessions


async def test_http_stale_delete_preserves_new_session(env, streaming):
    """Check behavior at the real DELETE view, not its source syntax."""
    old_sid, _ = await env.manager.create_session(camera(), "")
    new_sid, _ = await env.manager.create_session(camera(), "")
    view = streaming.whep.PetkitUpstreamWhepSessionView()
    response = await view.delete(request(env), "42", old_sid)
    assert response.status == 404
    assert env.manager._sessions["42"].session_id == new_sid
    response = await view.delete(request(env), "42", new_sid)
    assert response.status == 200
