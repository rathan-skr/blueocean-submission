"""R6 - events silently lost after any wire drop. incumbent/session_reader.py:101-103 discards every
frame with poss_dup=true ("already delivered to us earlier"), but a RESENT frame always carries
poss_dup=true - including the one that was lost and never delivered. And :107-113 publishes the
out-of-order frame and moves the checkpoint past the gap anyway. So every dropped frame (~0.2 %
of traffic) is lost for good, and the frame after it is published out of order.

The incumbent's per-frame decision (lines 99-113) is extracted verbatim below; ours is the
ChannelSequencer the reader uses. Same wire, both run to completion.
Property: everything published, in channel order, exactly once.
"""
import pytest

from settle.bosp import ACCEPT, ChannelSequencer
from tests.incumbent.conftest import IMPLS

# what the gateway writes on one channel: frame 3 is dropped on the wire; the reader asks for a
# resend; go-back-N re-sends 3.. with poss_dup=true (gateway.py:226: poss_dup = seq <= seen)
WIRE = [("DATA", 1, False), ("DATA", 2, False), ("DATA", 4, False),
        ("DATA", 3, True), ("DATA", 4, True), ("DATA", 5, False), ("DATA", 5, False)]   # + a wire duplicate


def incumbent(frames):
    expected, published = 1, []
    for t, seq, poss_dup in frames:            # session_reader.py:98-113
        if poss_dup:
            pass
        elif t == "GAP_FILL":
            pass
        else:
            if seq != expected:
                pass                            # RESEND requested ... but it carries on
            if t == "DATA":
                published.append(seq)
            expected = seq + 1
    return published


def fixed(frames):
    s, published = ChannelSequencer(1), []
    for t, seq, _ in frames:
        verdict, _resend = s.on_frame(t, seq, None, 0.0)
        if verdict == ACCEPT:
            published.append(seq)
    return published


@pytest.mark.parametrize("impl", IMPLS)
def test_resent_frames_are_published_in_order_once(impl):
    assert (incumbent if impl == "incumbent" else fixed)(WIRE) == [1, 2, 3, 4, 5]
