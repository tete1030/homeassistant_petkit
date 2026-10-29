# Agora feedback and publication lifecycle

These changes address two independent signaling defects. They do not add a
media jitter buffer, H.264 repair, polling, guessed subscriptions, or another
reconnect loop.

## Preserve negotiated RTCP feedback

`SDPParser` initialized `rtcpFb`, but never parsed `a=rtcp-fb`. Consequently
`parse_offer_to_ortc` advertised an empty feedback list even when the consumer
offered NACK, PLI and FIR.

The parser now preserves feedback type and parameter, including an empty
parameter for plain `nack`. Payload-specific attributes remain scoped to their
payload; `*` applies to all codecs in the same media section.

In a controlled investigation, repairing this conversion changed the actual
SDP/ORTC exchange and enabled NACK request packets. This is not a claim that
feedback alone repairs every video problem: late retransmissions can still be
rejected by the media receiver's independent SRTP replay window. That receiver
configuration belongs in go2rtc/Pion, not this integration.

## Withdrawal must invalidate publication state

A naturally occurring interruption was observed on 2026-10-23:

1. Inbound target RTP and the HA receive track stopped progressing.
2. About ten seconds later, the signaling channel delivered
   `{"_type":"on_remove_stream","_message":{"uid":42}}`
   (the identifier here is synthetic).
3. The component had no handler for that message, leaving publication and
   subscription-deduplication state behind.
4. Nine subsequent attempts received ORTC parameters and user-online events,
   but no video publication announcements. The old timeout fallback still
   returned an early SDP answer; the first eight attempts subsequently failed.

The captured removal message contained **no reason, cause or details**. It does
not explain why the publisher stopped. HTTP acceptance of heartbeat commands
also does not prove that the device executed them. The observed stream later
resumed before this lifecycle patch was deployed, so that recovery is not
evidence of this patch's effectiveness.

The component now handles removal and offline notifications separately:

- Withdrawal clears that UID's publication and subscription records.
- Withdrawal alone does not mean that the user left the channel.
- Removing the negotiated WHEP video invalidates its cached answer and invokes
  the existing owner cleanup callback, once.
- Removing an unrelated publisher does not close the negotiated stream.
- Removal before initial negotiation permits a later real publication to
  complete negotiation, including reuse of the same SSRC.

## WHEP readiness requires publication, not just membership

In the existing fixed-remote-SSRC WHEP mode, an answer requires an actual video
publication with a valid SSRC and a sent subscription. A publication supplied
in the join response is sufficient; a separate later notification is not
mandatory. Direct SDP answers cannot bypass the publication gate.

The existing 15-second join-response deadline is retained. Joined-without-video
raises `AgoraVideoNotPublishedError` instead of returning an early SDP answer.
WHEP returns its existing HTTP 502 error response and does not register the
provisional session. Missing join/ORTC remains a distinct negotiation failure.
The ordinary non-fixed-SSRC browser mode retains its existing readiness path.

`has_negotiated_video` is deliberately a **signaling** predicate. It does not
mean subscription acknowledgement, authenticated RTP, a decoded first frame,
or acceptable picture quality.

## Token renewal requires a confirmed reply

The Agora SDK emits `on_token_privilege_will_expire` before expiry and expects
the new token to be accepted by the signaling connection. The previous handler
sent `renew_token` and discarded its response. `on_token_privilege_did_expire`
only wrote an error log, so a rejected or lost renewal could leave the WHEP
session alive until the server withdrew the remote publication.

The handler now:

- sends `renew_token` in a background task so the WebSocket receive loop can
  still read the reply;
- records the request ID and waits for the matching reply with a bounded
  timeout and one bounded retry;
- accepts only an explicit successful result;
- treats provider failure, send failure, timeout, a rejected request, or
  `on_token_privilege_did_expire` as a reason to invalidate the current WHEP
  session.

Invalidating the session deliberately reuses the existing owner cleanup and
consumer retry path. It does not attempt an in-place `join` under an already
negotiated fixed-SSRC WHEP answer, and it does not add blind pre-subscriptions.
The next WHEP request must obtain a fresh token and receive a real video
publication before it can return success.

The observed removal messages do not contain a rejection reason. A renewal
reply is therefore not invented as the cause of every withdrawal; this patch
only removes the silent-success behavior and makes an unconfirmed renewal
fail closed.

## Safe replacement and teardown

Per-device lifecycle serialization completes old teardown before new activation.
Callbacks and WHEP DELETE requests are session-scoped, so a stale request cannot
stop a replacement. Different devices do not share the lifecycle lock.

Cancellation during activation or joining cleans provisional RTC and RTM
resources. Cancellation during teardown is propagated only after cleanup
finishes. A failed token-refresh task cannot skip transport cleanup.
Manager shutdown rejects new creates and drains in-flight creation as well as
registered sessions.

These are locally reproduced lifecycle hazards; not all were proven to cause
the captured natural interruption. In particular, the investigated go2rtc WHEP
client did not issue DELETE requests, so stale DELETE is not presented as that
incident's trigger.

## Reference behavior and limits

The [Agora Web SDK release bundle](https://download.agora.io/sdk/release/AgoraRTC_N.js)
examined during the investigation identified itself as
`4.24.9-0-g2319458dd` (SHA-256
`63a04a823e6e589a6dd4384e2c175a8e6f70c313f6f58aa88fd92e436ced55fe`).
Its ordinary removal handler clears publication/SSRC/ORTC state and emits
`user-unpublished` while retaining the user. Offline handling additionally
removes presence. Its ordinary subscribe path checks publication metadata.
Optional pre-subscribe paths exist; this patch does not enable them.

The official
[Web SDK example](https://github.com/AgoraIO-Community/AgoraWebSDK-NG/blob/eb8a5b2ef20a5cc4498f66caaaba9f5e2f65d043/Demo/basicVideoCall/basicVideoCall.js)
also subscribes in response to publication and handles withdrawal separately.

Static inspection of a Petkit Android 13.4.3 sample found separate callbacks in
`RtcService` for joining, user presence, remote video state/reason, and the first
remote video frame. `stop` stops the live heartbeat, sends the stop command,
and leaves the channel. **The app's own `isLive`/`initP2PSuccess` occurs on user
join, not necessarily the first frame.** The sample's signature was not
independently authenticated; static inspection is not a live behavior test.
Sample SHA-256:
`3b0baf66cc6785b63cd3bd1fe5bbf03c4517bcc850b5d1af651a220009ef5f86`.
No APK, proprietary SDK code, private capture or credentials are included here.

The SDK can update media bindings inside an existing connection. This WHEP
adapter instead has a fixed negotiated SSRC and uses its existing consumer
recovery path to negotiate afresh after withdrawal. This is an adaptation,
not a claim that the official SDK always reconnects on unpublication.
Changing a negotiated SSRC without a withdrawal notification is outside this
patch's scope.

The app sends its activation command after joining, whereas this integration
currently sends it before joining. That difference was not proven to cause the
withdrawal and is intentionally unchanged. The patch does not promise to make
an unpublished device publish, or solve every long-running interruption.

## Shared stream registration after startup

The camera previously attempted registration only when the entity was added.
If go2rtc was unavailable at that moment, or restarted later and lost its
in-memory stream registry, an external RTSP consumer could keep retrying a
missing source indefinitely. The camera now retries registration every 30
seconds while it exists. Retries are serialized with entity removal, and
cleanup confirms the source was actually removed: some go2rtc versions return
HTTP 200 for a DELETE request that omitted the required `src` parameter.

A controlled deployment test removed the named Petkit source, observed it
absent, and saw it automatically reappear after approximately 27 seconds.
The exact reason for an earlier failed startup registration was not recorded.
Successful registration does not guarantee that a device will publish video.

## Reproduce the tests

With Python 3.14.2 or newer:

```sh
python -m venv .venv
.venv/bin/python -m pip install -r requirements-test.txt
.venv/bin/python -m pytest tests
```

The streaming tests import complete source modules and real dependencies in an
isolated package namespace, without bootstrapping the whole integration.
Network edges are fake; no Home Assistant instance, credentials, camera or
media connection is used. Tests cover actual SDP generation, real WHEP view
responses, malformed/absent publication, removal and re-publication, stale
callbacks/DELETE, token-renewal acknowledgement/timeout, concurrent creation,
cancellation, and shutdown.

These are offline regression tests, not deployment or picture-quality acceptance.

## Keep RTM credential refresh separate from device activation

The periodic shared-WHEP RTM refresh previously used `get_live_feed()`, which
calls the device's `start/live` endpoint. This is an activation request, not a
read-only token endpoint. The Android control implementation instead obtains
account RTM credentials from `device/start/rtm`.

The background RTM refresh now uses that account endpoint and applies its token
only when `appRtmUserId` matches the existing RTM sender. An empty device list is
valid for this token-only operation; it does not replace the current device peer,
channel, RTC token or publication state. Missing/mismatched responses leave the
current token intact. Expected request failures do not terminate the refresh
loop, and cancellation still terminates it. RTC renewal retains its separate,
acknowledgement-tracked provider.

A six-hour observation recorded six successful RTC renewals and one subsequent
withdrawal. Failed recovery attempts had valid join/ORTC replies but RTM
`message_offline` responses and no device publication. Recovery followed a later
`message_delivered` response and a real video announcement. The withdrawal
notification contained no reason. Its proximity to a periodic `start/live`
request is a reason to remove that unnecessary activation from maintenance,
not proof that the request caused the withdrawal. This change does not claim to
explain device-side RTM logout or reproduce the complete native SDK lifecycle.

Tests exercise the actual refresh coroutine and RTM token updater, including
wrong sender, missing fields, empty device list, transient failures, cancellation,
and the invariant that background RTM maintenance never requests video activation.
