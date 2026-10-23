"""Publication-driven negotiation, using real SDP and synthetic signaling."""

# These tests intentionally inspect signaling state, not private network data.
# ruff: noqa: SLF001

import asyncio
import copy
from importlib import import_module
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

UID = 42
SSRC = 123456
FINGERPRINT = ":".join(["AA"] * 32)
OFFER = "\r\n".join(
    [
        "v=0",
        "o=- 1 1 IN IP4 127.0.0.1",
        "s=-",
        "t=0 0",
        "a=group:BUNDLE 0",
        "m=video 9 UDP/TLS/RTP/SAVPF 96",
        "c=IN IP4 127.0.0.1",
        "a=mid:0",
        "a=recvonly",
        "a=rtcp-mux",
        "a=ice-ufrag:test",
        "a=ice-pwd:synthetic-offer-password",
        f"a=fingerprint:sha-256 {FINGERPRINT}",
        "a=setup:actpass",
        "a=rtpmap:96 H264/90000",
        "a=fmtp:96 packetization-mode=1;profile-level-id=42001f",
        "a=rtcp-fb:96 nack",
        "a=rtcp-fb:96 nack pli",
        "a=rtcp-fb:96 ccm fir",
        "",
    ]
)


def publication(uid=UID, ssrc=SSRC):
    """Return a synthetic real-shape publication notification."""
    return {
        "_type": "on_add_video_stream",
        "_message": {"uid": uid, "video": True, "ssrcId": ssrc, "rtxSsrcId": SSRC + 1},
    }


def removal(uid=UID):
    """Withdrawal has no invented reason or offline flag."""
    return {"_type": "on_remove_stream", "_message": {"uid": uid}}


class Socket:
    """An async signaling transport with no network access."""

    def __init__(self, messages=(), *, timeout=False):
        """Store scripted messages and outgoing commands."""
        self.messages = iter(messages)
        self.timeout = timeout
        self.sent = []
        self.closed = False

    def __aiter__(self):
        """Iterate queued JSON messages."""
        return self

    async def __anext__(self):
        """End or time out once the scripted messages are consumed."""
        try:
            return json.dumps(next(self.messages))
        except StopIteration:
            if self.timeout:
                raise TimeoutError from None
            raise StopAsyncIteration from None

    async def send(self, raw):
        """Record the command that the real handler produced."""
        self.sent.append(json.loads(raw))

    async def close(self):
        """Record transport cleanup."""
        self.closed = True


@pytest.fixture
async def handler(streaming):
    """Create a real strict WHEP handler with only its transport replaced."""
    instance = streaming.ws.AgoraWebSocketHandler(
        declare_remote_video_ssrc=True, on_connection_lost=Mock()
    )
    instance._websocket = Socket()
    instance._connection_state = "CONNECTED"
    yield instance
    if instance._disconnect_task is not None:
        await instance._disconnect_task
    await instance.disconnect()


async def join(handler, *, streams=()):
    """Pass a synthetic join response through actual ORTC and SDP code."""
    offer = handler._parse_offer_sdp(OFFER)
    module = import_module(type(handler).__module__)
    ortc = module.parse_offer_to_ortc(OFFER)
    ortc["dtlsParameters"] = {
        "fingerprints": [{"hashFunction": "sha-256", "fingerprint": FINGERPRINT}]
    }
    ortc["iceParameters"] = {
        "iceUfrag": "remote",
        "icePwd": "synthetic-answer-password",
        "candidates": [],
    }
    return await handler._handle_join_success(
        {
            "_result": "success",
            "_message": {"ortc": copy.deepcopy(ortc), "streams": streams},
        },
        offer,
        SimpleNamespace(get_gateway_addresses=list, addresses=[]),
    )


async def negotiate(handler):
    """Announce a real-shape publication after joining."""
    assert await join(handler) is None
    answer = await handler._handle_add_video_stream(publication())
    assert answer is not None
    return answer


async def test_online_is_not_publication(handler):
    """Joining and seeing a user do not authorize an invented subscription."""
    await handler._handle_user_online({"_message": {"uid": UID}})
    assert await join(handler) is None
    assert not handler.has_negotiated_video
    assert not any(
        message["_type"] == "subscribe" for message in handler._websocket.sent
    )


async def test_real_answer_preserves_ssrc_rtx_and_feedback(handler):
    """Announced metadata and feedback reach the generated answer together."""
    answer = await negotiate(handler)
    assert handler._validate_sdp(answer)
    for line in (
        f"a=ssrc:{SSRC} cname:agora",
        f"a=ssrc-group:FID {SSRC} {SSRC + 1}",
        "a=rtcp-fb:96 nack",
        "a=rtcp-fb:96 nack pli",
        "a=rtcp-fb:96 ccm fir",
    ):
        assert line in answer
    assert handler.has_negotiated_video


async def test_join_snapshot_publication_is_accepted(handler):
    """Already-published streams need no second announcement."""
    assert await join(handler, streams=[publication()["_message"]])
    assert handler.has_negotiated_video


async def test_announcement_before_join_is_accepted(handler):
    """Publication and join delivery order do not bypass or deadlock the gate."""
    assert await handler._handle_add_video_stream(publication()) is None
    assert await join(handler)
    assert handler.has_negotiated_video


async def test_remove_invalidates_negotiated_stream_once(handler):
    """Withdrawal clears cached media and SDP but is not an offline notice."""
    await negotiate(handler)
    handler._online_users.add(UID)
    callback = handler._on_connection_lost
    assert "on_remove_stream" in handler._message_handlers
    await handler._message_handlers["on_remove_stream"](removal())
    await handler._message_handlers["on_remove_stream"](removal())
    assert not handler.has_negotiated_video
    assert handler._answer_sdp is None
    assert handler._video_streams == {}
    assert handler._subscribed_video_streams == set()
    assert UID in handler._online_users
    callback.assert_called_once_with()
    await handler._disconnect_task


async def test_unrelated_removal_keeps_active_video(handler):
    """Removing another publisher must not close this negotiated stream."""
    await negotiate(handler)
    await handler._handle_add_video_stream(publication(uid=99, ssrc=999))
    await handler._handle_remove_stream(removal(99))
    assert handler.has_negotiated_video
    assert handler._subscribed_video_streams == {(UID, SSRC)}
    handler._on_connection_lost.assert_not_called()


async def test_offline_clears_presence_too(handler):
    """An explicit offline message removes both presence and publication."""
    await negotiate(handler)
    handler._online_users.add(UID)
    await handler._message_handlers["on_user_offline"](removal())
    assert UID not in handler._online_users
    assert not handler.has_negotiated_video


@pytest.mark.parametrize("new_ssrc", [SSRC, SSRC + 2])
async def test_remove_before_answer_allows_republication(handler, new_ssrc):
    """Do not retain subscription deduplication after a withdrawn publication."""
    await handler._handle_add_video_stream(publication())
    await handler._handle_remove_stream(removal())
    assert handler._disconnect_task is None
    assert await join(handler) is None
    assert await handler._handle_add_video_stream(publication(ssrc=new_ssrc))
    subscriptions = [m for m in handler._websocket.sent if m["_type"] == "subscribe"]
    assert len(subscriptions) == 2
    assert handler._subscribed_video_streams == {(UID, new_ssrc)}


@pytest.mark.parametrize("message", [None, {}, [1], {"uid": "42"}, {"uid": True}])
async def test_invalid_removal_does_not_close_video(handler, message):
    """Null, malformed and non-integer identities do not match a publisher."""
    await negotiate(handler)
    await handler._handle_remove_stream({"_message": message})
    assert handler.has_negotiated_video


async def test_malformed_announcement_and_offline_ignored(handler):
    """Malformed message containers must not mutate the active publication."""
    await negotiate(handler)
    await handler._handle_add_video_stream({"_message": [1]})
    await handler._handle_user_offline({"_message": [1]})
    assert handler.has_negotiated_video


@pytest.mark.parametrize("ssrc", [None, "123456", 0, -1, True, 2**32])
async def test_invalid_ssrc_never_subscribed(handler, ssrc):
    """A user ID or invalid SSRC is not a usable media announcement."""
    assert await join(handler) is None
    assert await handler._handle_add_video_stream(publication(ssrc=ssrc)) is None
    assert not handler.has_negotiated_video
    assert not any(m["_type"] == "subscribe" for m in handler._websocket.sent)


async def test_no_publication_timeout_is_not_sdp_success(handler, streaming):
    """Joined-without-video raises a distinct failure, not an early answer."""
    assert await join(handler) is None
    with pytest.raises(
        streaming.ws.AgoraVideoNotPublishedError, match="publication timeout"
    ):
        await handler._wait_for_join_response(Socket(timeout=True), None, None)
    assert handler._answer_sdp is None
    assert not handler._joined


async def test_no_join_timeout_is_not_mislabeled(handler):
    """No join/ORTC is different from joined-without-publication."""
    assert (
        await handler._wait_for_join_response(Socket(timeout=True), None, None) is None
    )


async def test_direct_answer_waits_for_publication(handler):
    """A direct SDP message cannot bypass the strict publication gate."""
    assert (
        await handler._handle_answer({"_message": {"sdp": "synthetic-answer"}}) is None
    )
    assert not handler.has_negotiated_video
    assert await handler._handle_add_video_stream(publication()) == "synthetic-answer"
    assert handler.has_negotiated_video


async def test_non_whep_negotiation_remains_immediate(handler):
    """Keep the ordinary browser path independent of fixed-SSRC WHEP gating."""
    handler._declare_remote_video_ssrc = False
    assert await join(handler)


async def test_disconnect_clears_state_idempotently(handler):
    """Concurrent cleanup closes the socket and forgets all publication state."""
    await negotiate(handler)
    socket = handler._websocket
    handler._online_users.add(UID)
    await asyncio.gather(handler.disconnect(), handler.disconnect())
    assert socket.closed
    assert not handler._online_users
    assert not handler._video_streams
    assert not handler._subscribed_video_streams
    assert handler._answer_video_uid is None


async def test_cancelled_socket_close_can_be_retried(handler):
    """Keep ownership of a socket until its close operation finishes."""
    entered, release = asyncio.Event(), asyncio.Event()

    class SlowCloseSocket(Socket):
        """Expose cancellation during the transport close handshake."""

        async def close(self):
            entered.set()
            await release.wait()
            self.closed = True

    socket = handler._websocket = SlowCloseSocket()
    close = asyncio.create_task(handler.disconnect())
    async with asyncio.timeout(2):
        await entered.wait()
    close.cancel()
    with pytest.raises(asyncio.CancelledError):
        await close
    assert handler._websocket is socket
    release.set()
    await handler.disconnect()
    assert socket.closed
    assert handler._websocket is None


async def test_publication_timeout_closes_socket_without_retry(
    handler, streaming, monkeypatch
):
    """Do not turn one no-video outcome into joins against every gateway."""
    await join(handler)
    socket = Socket(timeout=True)
    connect = AsyncMock(return_value=socket)
    monkeypatch.setattr(streaming.ws, "connect", connect)
    monkeypatch.setattr(
        handler, "_create_join_message", lambda **kwargs: {"_type": "join_v3"}
    )
    edge = SimpleNamespace(ip="127.0.0.1", port=1)
    edges = SimpleNamespace(get_gateway_addresses=lambda: [edge, edge])
    with pytest.raises(streaming.ws.AgoraVideoNotPublishedError):
        await handler.connect_and_join(
            SimpleNamespace(rtc_token="synthetic"), OFFER, "session", "app", edges
        )
    connect.assert_awaited_once()
    assert socket.closed
    assert not handler.is_connected
    assert handler._pending_answer_ortc is None


async def test_cancel_during_websocket_join_closes_transport(
    handler, streaming, monkeypatch
):
    """Exercise cancellation in connect_and_join with the real receive loop."""
    entered = asyncio.Event()

    class BlockingSocket(Socket):
        """Wait indefinitely for a message until the request is cancelled."""

        async def __anext__(self):
            entered.set()
            await asyncio.Event().wait()

    socket = BlockingSocket()
    monkeypatch.setattr(streaming.ws, "connect", AsyncMock(return_value=socket))
    monkeypatch.setattr(
        handler, "_create_join_message", lambda **kwargs: {"_type": "join_v3"}
    )
    edge = SimpleNamespace(ip="127.0.0.1", port=1)
    task = asyncio.create_task(
        handler.connect_and_join(
            SimpleNamespace(rtc_token="synthetic"),
            OFFER,
            "session",
            "app",
            SimpleNamespace(get_gateway_addresses=lambda: [edge]),
        )
    )
    async with asyncio.timeout(2):
        await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert socket.closed
    assert not handler.is_connected
    assert handler._message_loop_task is None
    assert handler._ping_task is None


async def test_failed_gateway_clears_pending_negotiation(
    handler, streaming, monkeypatch
):
    """A later gateway must not reuse the previous attempt's deferred answer."""
    await join(handler)
    sockets = [Socket(), Socket(timeout=True)]
    monkeypatch.setattr(streaming.ws, "connect", AsyncMock(side_effect=sockets))
    monkeypatch.setattr(
        handler, "_create_join_message", lambda **kwargs: {"_type": "join_v3"}
    )
    edge = SimpleNamespace(ip="127.0.0.1", port=1)
    assert (
        await handler.connect_and_join(
            SimpleNamespace(rtc_token="synthetic"),
            OFFER,
            "session",
            "app",
            SimpleNamespace(get_gateway_addresses=lambda: [edge, edge]),
        )
        is None
    )
    assert all(socket.closed for socket in sockets)
    assert handler._pending_answer_ortc is None
    assert not handler.is_connected
