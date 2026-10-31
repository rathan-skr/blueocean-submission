"""Deterministic tests for the BOSP framing, the per-channel sequencer and credit accounting.
No sockets, no clocks: time is passed in explicitly."""
import json

import pytest

from settle.bosp import ACCEPT, DUP, GAP, ChannelSequencer, CreditLedger, FrameDecoder, FrameError, encode


# -- framing ------------------------------------------------------------------------------------
def test_decoder_handles_any_split_including_inside_length_prefix():
    frames = [{"type": "DATA", "seq": i, "pad": "x" * (i * 37 % 200)} for i in range(1, 30)]
    wire = b"".join(encode(f) for f in frames)
    for step in (1, 2, 3, 5, 7, 64, 4096):
        d, got = FrameDecoder(), []
        for i in range(0, len(wire), step):
            got += d.feed(wire[i:i + step])
        assert [json.loads(b) for b in got] == frames
        assert d.buffered == 0


def test_decoder_several_frames_in_one_chunk_and_partial_tail():
    a, b = encode({"type": "HEARTBEAT"}), encode({"type": "TEST_REQUEST", "test_req_id": "T1"})
    d = FrameDecoder()
    out = d.feed(a + b + b[:3])
    assert len(out) == 2 and d.buffered == 3
    d.reset()                                  # connection dropped mid-frame: discard the partial
    assert d.buffered == 0
    assert d.feed(a) == [a[4:]]


def test_decoder_rejects_oversize_and_zero_length():
    with pytest.raises(FrameError):
        FrameDecoder().feed((70_000).to_bytes(4, "big"))
    with pytest.raises(FrameError):
        FrameDecoder().feed((0).to_bytes(4, "big"))


# -- sequencer ----------------------------------------------------------------------------------
def seqr(expected=1):
    return ChannelSequencer(expected, resend_timeout=2.0, idle_probe=4.0, last_frame_at=0.0, last_probe_at=0.0)


def test_in_order_accept():
    s = seqr()
    for i in range(1, 6):
        assert s.on_frame("DATA", i, None, 0.1) == (ACCEPT, None)
    assert s.expected == 6


def test_duplicates_ignored_regardless_of_poss_dup():
    s = seqr(10)
    assert s.on_frame("DATA", 9, None, 0) == (DUP, None)      # wire duplicate with poss_dup:false
    assert s.on_frame("NOTICE", 3, None, 0) == (DUP, None)
    assert s.expected == 10


def test_gap_requests_resend_once_then_discards_until_filled():
    s = seqr()
    s.on_frame("DATA", 1, None, 0)
    assert s.on_frame("DATA", 3, None, 0.1) == (GAP, 2)         # 2 lost
    assert s.on_frame("DATA", 4, None, 0.2) == (GAP, None)      # resend already outstanding
    # go-back-N: the server rewinds to 2 and re-sends 2, 3, 4 (poss_dup)
    assert s.on_frame("DATA", 2, None, 0.3)[0] == ACCEPT
    assert s.resend_from is None
    assert s.on_frame("DATA", 3, None, 0.3)[0] == ACCEPT
    assert s.on_frame("DATA", 4, None, 0.3)[0] == ACCEPT
    assert s.expected == 5


def test_lost_resend_is_detected_and_reissued():
    s = seqr()
    s.on_frame("DATA", 1, None, 0)
    assert s.on_frame("DATA", 3, None, 0.0) == (GAP, 2)
    assert s.tick(1.0) is None                                  # still within timeout
    assert s.tick(2.5) == 2                                     # our RESEND (or its answer) was lost
    assert s.resends == 2
    assert s.on_frame("DATA", 5, None, 2.6) == (GAP, None)      # newer resend outstanding
    assert s.on_frame("DATA", 6, None, 5.0) == (GAP, 2)         # and that one timed out too


def test_gap_fill_advances_and_overlapping_gap_fill():
    s = seqr(43)
    assert s.on_frame("GAP_FILL", 43, 45, 0)[0] == ACCEPT       # 43, 44 were NOTICEs
    assert s.expected == 45
    assert s.on_frame("GAP_FILL", 40, 45, 0)[0] == DUP          # entirely behind us
    s2 = seqr(44)
    assert s2.on_frame("GAP_FILL", 43, 47, 0)[0] == ACCEPT      # straddles expected
    assert s2.expected == 47
    s3 = seqr(40)
    assert s3.on_frame("GAP_FILL", 43, 47, 0) == (GAP, 40)      # starts beyond expected: a real gap


def test_gap_closed_by_gap_fill():
    s = seqr(10)
    assert s.on_frame("DATA", 13, None, 0) == (GAP, 10)
    assert s.on_frame("GAP_FILL", 10, 13, 0.5)[0] == ACCEPT
    assert s.resend_from is None
    assert s.on_frame("DATA", 13, None, 0.5)[0] == ACCEPT


def test_tail_loss_idle_probe():
    s = seqr()
    for i in range(1, 4):
        s.on_frame("DATA", i, None, 1.0)
    # frame 4 (the channel's last) was dropped: nothing will ever arrive to reveal the gap
    assert s.tick(3.0) is None
    assert s.tick(5.1) == 4                                      # probe: RESEND from expected
    assert s.tick(6.0) is None                                   # rate-limited
    assert s.tick(9.2) == 4


def test_reconnect_resumes_from_applied_and_drops_replay():
    # after a reconnect the sequencer starts at last applied + 1; anything older is a duplicate
    s = seqr(101)
    assert s.on_frame("DATA", 100, None, 0)[0] == DUP
    assert s.on_frame("DATA", 101, None, 0)[0] == ACCEPT


# -- credit -------------------------------------------------------------------------------------
def test_credit_never_exceeds_window_and_is_returned_only_when_settled():
    c = CreditLedger(window=1000, refill_at=250)
    outstanding = 1000
    given = 0
    for i in range(5000):
        c.received()
        outstanding -= 1
        assert outstanding >= 0
        g = c.settle()
        outstanding += g
        given += g
        assert outstanding <= 1000
    assert c.unreturned < 250


def test_unsettled_frames_hold_their_credit():
    c = CreditLedger(window=1000, refill_at=250)
    for _ in range(400):
        c.received()                       # published, PubAck not yet received
    assert c.unreturned == 400 and c.settle(0) == 0
