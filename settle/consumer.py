"""Ledger consumer. Run >= 4 processes.

Partition ownership: the stream has PARTITIONS subjects. Live consumers (heartbeating in
consumer_members) split them deterministically (sorted members, round robin); each partition is
held through a lease row in `partitions` with an owner_epoch that increments on every takeover.

Exactly-once effect, per batch, ONE transaction:
  1. SELECT the partition row FOR UPDATE WHERE owner = me AND owner_epoch = mine  (fence)
  2. apply the frames (ledger.apply_batch: per-merchant last_seq guard)
  3. UPDATE partitions SET stream_seq = <last broker seq in batch>          (offset, same txn)
  4. COMMIT
The broker's own ack state is never used: a new owner resumes from stream_seq + 1 (an ephemeral,
ack-none JetStream consumer started at that sequence). A consumer killed mid-batch leaves nothing
behind (its transaction rolls back); its successor re-reads the same messages and the guard makes
any re-delivery a no-op.
"""
from __future__ import annotations

import asyncio
import logging
import signal
import time

import psycopg
from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

from . import broker, config, db, ledger
from .log import setup as setup_logging
from .partition import subject

log = logging.getLogger("consumer")


class Resync(Exception):
    pass


class Owned:
    def __init__(self, partition: int, epoch: int, stream_seq: int):
        self.partition, self.epoch, self.stream_seq = partition, epoch, stream_seq
        self.sub = None
        self.task: asyncio.Task | None = None
        self.release = False


class Consumer:
    def __init__(self, js):
        self.js = js
        self.me = config.INSTANCE_ID
        self.owned: dict[int, Owned] = {}
        self.stop = asyncio.Event()

    # -- membership + assignment (one small autocommit connection) --------------------------
    async def coordinate(self):
        """Membership + partition assignment, every second. Live members sorted -> partition p belongs
        to members[p % n]. Renews leases we keep, flags partitions to release, and acquires free or
        expired ones (owner_epoch + 1 = the fencing token for the apply transaction)."""
        conn = None
        while not self.stop.is_set():
            try:
                if conn is None or conn.closed:
                    conn = await db.aconnect(app="consumer-coord")
                await conn.execute("INSERT INTO consumer_members (member_id, heartbeat) VALUES (%s, now()) "
                                   "ON CONFLICT (member_id) DO UPDATE SET heartbeat = now()", (self.me,))
                cur = await conn.execute(
                    "SELECT member_id FROM consumer_members WHERE heartbeat > now() - make_interval(secs => %s) "
                    "ORDER BY member_id", (config.MEMBER_TTL_S,))
                members = [r[0] for r in await cur.fetchall()]
                if self.me not in members:
                    members = sorted(members + [self.me])
                target = {p for p in range(config.PARTITIONS) if members[p % len(members)] == self.me}
                # give up partitions that now belong to someone else (after the in-flight batch)
                for p, o in list(self.owned.items()):
                    if p not in target:
                        o.release = True
                # renew what we keep, take what is ours and free
                for p in sorted(target):
                    o = self.owned.get(p)
                    if o and not o.release:
                        cur = await conn.execute(
                            "UPDATE partitions SET lease_until = now() + make_interval(secs => %s) "
                            "WHERE partition = %s AND owner = %s AND owner_epoch = %s",
                            (config.PARTITION_LEASE_S, p, self.me, o.epoch))
                        if cur.rowcount != 1:
                            log.warning("partition lease lost", extra={"partition": p})
                            self.drop(p)
                        continue
                    if o is None:
                        cur = await conn.execute(
                            "UPDATE partitions SET owner = %s, owner_epoch = owner_epoch + 1, "
                            "lease_until = now() + make_interval(secs => %s) "
                            "WHERE partition = %s AND (owner IS NULL OR lease_until < now()) "
                            "RETURNING owner_epoch, stream_seq", (self.me, config.PARTITION_LEASE_S, p))
                        row = await cur.fetchone()
                        if row:
                            self.start(p, row[0], row[1])
            except (psycopg.Error, OSError) as e:
                log.warning("coordination error", extra={"error": repr(e)[:200]})
                conn = None
                # without the database we cannot prove ownership: stop applying (apply txns would fail anyway)
            await asyncio.sleep(1.0)

    def start(self, p: int, epoch: int, stream_seq: int):
        o = Owned(p, epoch, stream_seq)
        self.owned[p] = o
        o.task = asyncio.create_task(self.run_partition(o))
        log.info("partition acquired", extra={"partition": p, "owner_epoch": epoch, "from_stream_seq": stream_seq + 1})

    def drop(self, p: int):
        # never cancel a partition task mid-transaction: flag it; its next apply is fenced anyway
        o = self.owned.get(p)
        if o:
            o.release = True

    # -- one partition ------------------------------------------------------------------------
    async def run_partition(self, o: Owned):
        """Fetch -> apply loop for one owned partition. Reads NATS from our committed offset + 1 with
        an ephemeral, ack-none consumer; any error resets the subscription to the committed offset."""
        conn = None
        sub = None
        try:
            while not self.stop.is_set():
                if o.release:
                    await self.release(o)
                    return
                try:
                    if sub is None:
                        sub = await self.js.pull_subscribe(
                            subject(o.partition), durable=None, stream=config.STREAM,
                            config=ConsumerConfig(deliver_policy=DeliverPolicy.BY_START_SEQUENCE,
                                                  opt_start_seq=o.stream_seq + 1, ack_policy=AckPolicy.NONE,
                                                  inactive_threshold=60.0, max_waiting=8))
                    try:
                        msgs = await sub.fetch(config.CONSUMER_BATCH, timeout=1.0)
                    except asyncio.TimeoutError:
                        continue
                    msgs = [m for m in msgs if m.metadata.sequence.stream > o.stream_seq]
                    if not msgs:
                        continue
                    if conn is None or conn.closed:
                        conn = await asyncio.to_thread(db.connect, False, "consumer-apply")
                        await asyncio.to_thread(self._session_setup, conn)
                    ok = await asyncio.to_thread(self.apply, conn, o, msgs)
                    if not ok:
                        log.warning("fenced: partition owned by someone else", extra={"partition": o.partition,
                                                                                      "owner_epoch": o.epoch})
                        self.owned.pop(o.partition, None)
                        return
                except Resync:
                    log.info("offset resync", extra={"partition": o.partition, "stream_seq": o.stream_seq})
                    sub = await self._reset_sub(sub)
                except (psycopg.OperationalError, psycopg.InterfaceError) as e:
                    log.warning("db error, will retry batch", extra={"partition": o.partition, "error": str(e)[:200]})
                    conn = None
                    sub = await self._reset_sub(sub)        # re-read from the committed offset
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    log.warning("partition loop error", extra={"partition": o.partition, "error": repr(e)[:300]})
                    sub = await self._reset_sub(sub)
                    await asyncio.sleep(1)
        finally:
            await self._reset_sub(sub)
            if conn is not None:
                conn.close()

    async def _reset_sub(self, sub):
        if sub is not None:
            try:
                await sub.unsubscribe()
            except Exception:
                pass
        return None

    @staticmethod
    def _session_setup(conn):
        # a paused/stuck owner must not hold the partition row lock forever
        conn.execute("SET idle_in_transaction_session_timeout = '15s'")
        conn.execute("SET statement_timeout = '30s'")
        conn.commit()

    def apply(self, conn: psycopg.Connection, o: Owned, msgs) -> bool:
        """One batch, one transaction. Returns False if we no longer own the partition."""
        frames = [ledger.decode(m.data) for m in msgs]
        last = msgs[-1].metadata.sequence.stream
        lag = msgs[-1].metadata.num_pending
        t0 = time.perf_counter()
        with conn.transaction():
            cur = conn.cursor()
            cur.execute("SELECT stream_seq FROM partitions WHERE partition = %s AND owner = %s AND owner_epoch = %s "
                        "FOR UPDATE", (o.partition, self.me, o.epoch))
            row = cur.fetchone()
            if row is None:
                return False
            if row[0] != o.stream_seq:
                # e.g. our previous COMMIT succeeded but the connection died before we heard back.
                # We still own the partition, so the committed offset is the truth: re-read from it.
                o.stream_seq = row[0]
                raise Resync()
            st = ledger.apply_batch(cur, frames)
            cur.execute("UPDATE partitions SET stream_seq = %s, lag = %s, applied = applied + %s, "
                        "dup_skipped = dup_skipped + %s, gap_dropped = gap_dropped + %s, "
                        "lease_until = greatest(lease_until, now() + make_interval(secs => %s)) "
                        "WHERE partition = %s", (last, lag, st.applied, st.duplicates, st.gaps,
                                                 config.PARTITION_LEASE_S, o.partition))
        o.stream_seq = last
        if st.gaps:
            log.info("dropped out-of-order frames (will be re-published)",
                     extra={"partition": o.partition, "count": st.gaps})
        log.debug("batch applied", extra={"partition": o.partition, "frames": len(frames), "applied": st.applied,
                                          "dups": st.duplicates, "ms": round((time.perf_counter() - t0) * 1000, 1)})
        return True

    async def release(self, o: Owned):
        """Give a partition back (fenced on owner + epoch) so its new owner can take it immediately."""
        try:
            conn = await db.aconnect(app="consumer-release")
            await conn.execute("UPDATE partitions SET owner = NULL, lease_until = now() WHERE partition = %s "
                               "AND owner = %s AND owner_epoch = %s", (o.partition, self.me, o.epoch))
            await conn.close()
            log.info("partition released", extra={"partition": o.partition})
        finally:
            self.owned.pop(o.partition, None)

    async def shutdown(self):
        for o in list(self.owned.values()):
            o.release = True
        for _ in range(50):
            if not self.owned:
                break
            await asyncio.sleep(0.1)
        try:
            conn = await db.aconnect(app="consumer-bye")
            await conn.execute("DELETE FROM consumer_members WHERE member_id = %s", (self.me,))
            await conn.close()
        except Exception:
            pass


async def main():
    setup_logging("consumer")
    await asyncio.to_thread(db.bootstrap)
    nc, js = await broker.connect("consumer")
    await broker.ensure_stream(js)
    c = Consumer(js)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, c.stop.set)
    coord = asyncio.create_task(c.coordinate())
    await c.stop.wait()
    coord.cancel()
    await c.shutdown()
    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
