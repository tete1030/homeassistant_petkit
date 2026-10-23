"""SDP feedback survives the actual SDP-to-ORTC conversion."""

import copy

import pytest

BASE = (
    "v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96 97\r\na=recvonly\r\n"
    "a=rtpmap:96 H264/90000\r\na=rtpmap:97 H264/90000\r\n"
    "a=fmtp:96 packetization-mode=1;profile-level-id=42001f\r\n"
)
FEEDBACK = "a=rtcp-fb:96 nack\r\na=rtcp-fb:96 nack pli\r\na=rtcp-fb:96 ccm fir\r\n"


def video_codecs(result):
    """Return the advertised receive codecs."""
    return result["rtpCapabilities"]["recv"]["videoCodecs"]


def test_feedback_preserved_and_payload_scoped(streaming):
    """Preserve parameterless NACK, PLI and FIR only for their payload."""
    codecs = video_codecs(streaming.sdp.parse_offer_to_ortc(BASE + FEEDBACK))
    assert codecs[0]["rtcpFeedbacks"] == [
        {"type": "nack", "parameter": ""},
        {"type": "nack", "parameter": "pli"},
        {"type": "ccm", "parameter": "fir"},
    ]
    assert codecs[1]["rtcpFeedbacks"] == []


def test_wildcard_is_media_scoped(streaming):
    """Wildcard feedback applies to all codecs in its media, not other media."""
    sdp = BASE + "a=rtcp-fb:* transport-cc\r\n"
    sdp += "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\n"
    result = streaming.sdp.parse_offer_to_ortc(sdp)
    assert all(
        codec["rtcpFeedbacks"] == [{"type": "transport-cc", "parameter": ""}]
        for codec in video_codecs(result)
    )
    assert result["rtpCapabilities"]["recv"]["audioCodecs"][0]["rtcpFeedbacks"] == []


def test_feedback_does_not_change_other_capabilities(streaming):
    """Adding feedback leaves codec, ICE and other ORTC data intact."""
    baseline = streaming.sdp.parse_offer_to_ortc(BASE)
    actual = copy.deepcopy(streaming.sdp.parse_offer_to_ortc(BASE + FEEDBACK))
    for codec in video_codecs(actual):
        codec["rtcpFeedbacks"] = []
    assert actual == baseline


@pytest.mark.parametrize("value", ["", "96", "x nack", "-1 nack", "\u00b2 nack"])
def test_malformed_feedback_ignored(streaming, value):
    """Invalid feedback attributes do not crash or invent capabilities."""
    result = streaming.sdp.parse_offer_to_ortc(BASE + f"a=rtcp-fb:{value}\r\n")
    assert all(codec["rtcpFeedbacks"] == [] for codec in video_codecs(result))
