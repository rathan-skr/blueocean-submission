"""Our rules vs the harness reference (ledger_events.apply_event) on generated channels.

The generator deliberately emits over-refunds, double captures, captures of expired auths,
mid-stream MERCHANT_CONFIG changes and unknown event types, so a few thousand frames per channel
exercise every branch.
"""
import ledger_events as le

from settle.ledger import Book, apply_event


def run_channel(seed, mid, n):
    ref_book, ref_plat, ref_pay = le.MerchantBook(mid), {}, {}
    book, auths, caps = Book(mid), {}, {}
    results = set()
    for f in le.channel_frames(seed, mid, n):
        if f["kind"] != "DATA":
            continue
        ev = f["event"]
        exp = le.apply_event(ref_book, ev, ref_plat, ref_pay)
        got, payout = apply_event(book, ev, auths, caps)
        results.add(got.split(":")[0] + ":" + ev["type"])
        assert got == exp, (ev, got, exp)
        assert (book.settled, book.held, book.available) == (ref_book.settled, ref_book.held, ref_book.available), ev
        if ev["type"] == "PAYOUT_REQUEST":
            want = ref_pay[ev["event_id"]]
            assert payout["status"] == ("rejected" if want["status"] == "rejected" else "pending")
            assert (payout["account"], payout["currency"], payout["amount"]) == \
                   (want["account"], want["currency"], want["amount_minor"])
    assert book.fees == sum(ref_plat.values())
    return results


def test_rules_match_reference_across_channels():
    seen = set()
    for i in range(1, 41):
        seen |= run_channel(42, f"M-{i:04d}", 3000)
    # every interesting branch was hit at least once
    for must in ("applied:CAPTURE", "rejected:CAPTURE", "applied:REFUND", "rejected:REFUND",
                 "applied:CHARGEBACK", "applied:AUTH_EXPIRY", "applied:PAYOUT_REQUEST",
                 "rejected:PAYOUT_REQUEST", "ignored:LOYALTY_ACCRUAL", "applied:MERCHANT_CONFIG"):
        assert must in seen, must


def test_ref_to_another_merchants_auth_is_rejected():
    a, b = Book("M-0001"), Book("M-0002")
    auths, caps = {}, {}
    apply_event(a, {"event_id": "M-0001:2", "type": "AUTH", "amount_minor": 1000}, auths, caps)
    res, _ = apply_event(b, {"event_id": "M-0002:2", "type": "CAPTURE", "ref": "M-0001:2",
                             "amount_minor": 1000, "fee_minor": 59}, auths, caps)
    assert res == "rejected:auth_not_open"
    assert (b.settled, b.held, b.fees) == (0, 0, 0)


def test_partial_capture_releases_whole_hold():
    m = Book("M-1")
    auths, caps = {}, {}
    apply_event(m, {"event_id": "M-1:2", "type": "AUTH", "amount_minor": 10_000}, auths, caps)
    apply_event(m, {"event_id": "M-1:3", "type": "CAPTURE", "ref": "M-1:2", "amount_minor": 4_000,
                    "fee_minor": 146}, auths, caps)
    assert (m.held, m.settled, m.fees) == (0, 3_854, 146)
