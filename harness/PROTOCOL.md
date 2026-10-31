# BOSP/1: Blue Ocean Session Protocol

BOSP/1 is the wire protocol of the acquirer gateway (`gateway.py`), which is the only source of ledger events.

The protocol is session-oriented and multiplexed. It is modelled on real financial session protocols such as FIX and on multiplexed transports such as HTTP/2 and AMQP. If you have worked with those, it will look familiar. If you haven't, read this page twice.

> **Default ports.** BOSP is on TCP `7100`. The read-only admin/status endpoint is `GET http://<host>:7101/status`. That endpoint is for tooling and the verifier only; your pipeline must not depend on it.

---

## 1. Framing

The protocol runs over a single TCP connection, and both directions use the same framing:

```
+-------------------------+----------------------------------+
| length: uint32, big end | body: `length` bytes, UTF-8 JSON |
+-------------------------+----------------------------------+
```

- The maximum body size is 65,536 bytes. A larger frame is a protocol error.
- Every body is a JSON object with a `type` field.
- TCP is a byte stream. A single read may return part of a frame, several frames, or a frame split anywhere, including inside the length prefix.
- The server may close the connection in the middle of a frame. Discard any partial frame you are holding when the connection drops.

---

## 2. Sessions

### 2.1 Logon

The first frame on a connection must be `LOGON`, sent within 5 seconds:

```json
{"type": "LOGON", "client_id": "bo-ledger", "secret": "bo-dev-secret",
 "resume": {"M-0001": 1532, "M-0002": 977}}
```

- **`resume`** gives, for each channel, the **last sequence number you have fully processed**. A channel you leave out is treated as `0`, so it is resent from `1`.
- **`LOGON_ACK`.** On success the server replies:

  ```json
  {"type": "LOGON_ACK", "session_epoch": 1793184412, "heartbeat_ms": 2000,
   "channels": ["M-0001", "M-0002", "..."], "max_outstanding_credit": 10000,
   "server_time": "2026-11-01T09:00:00.000Z"}
  ```

- **`LOGON_REJECT`.** On failure, `{"type": "LOGON_REJECT", "reason": "..."}` is sent and the connection is closed. The reasons are:

  | Reason | Meaning |
  |---|---|
  | `bad_credentials` | The secret is wrong. |
  | `throttled` | More than one logon per second for this `client_id`. Wait `retry_after_ms`. |
  | `expected_logon`, `bad_client_id`, `bad_resume` | The `LOGON` frame is malformed. |

- **`session_epoch`.** This is strictly increasing across all logons the server has accepted. Two sessions never share an epoch, and a later logon always gets a larger one.

### 2.2 One live session per `client_id`

**Only one session per `client_id` can be live.** A successful `LOGON` for a `client_id` that already has a live session immediately ends the older one:

- the old connection receives `{"type": "LOGOUT", "reason": "superseded"}`;
- it is then closed.

Anything the old session was still doing after that point (processing, publishing, writing) is happening on behalf of a session that no longer exists.

### 2.3 Liveness

- **The server's heartbeat.** If the server has sent nothing for `heartbeat_ms`, it sends `{"type": "HEARTBEAT"}`.
- **Your heartbeat.** You must send something at least every `heartbeat_ms`. Any frame counts; if you have nothing else, send `{"type": "HEARTBEAT"}`.
- **`TEST_REQUEST`.** If the server hears nothing from you for 1.5 × `heartbeat_ms`, it sends `{"type": "TEST_REQUEST", "test_req_id": "T…"}`. You must answer with `{"type": "HEARTBEAT", "test_req_id": "<the same id>"}`. A plain heartbeat does not answer a `TEST_REQUEST`.
- **Timeouts.** The server ends the session with `LOGOUT {"reason": "heartbeat_timeout"}` in either case:
  - it hears nothing from you for 2.5 × `heartbeat_ms`;
  - a `TEST_REQUEST` goes unanswered for 2 × `heartbeat_ms`.

  A process that blocks its I/O loop (a synchronous database call, a long GC pause, a CPU-bound batch) will be disconnected.
- **Your side.** Treat 2 × `heartbeat_ms` of silence from the server as a dead connection.

### 2.4 Logout

- **You** may send `{"type": "LOGOUT"}` to end the session cleanly.
- **The server** sends `LOGOUT` with one of these reasons, then closes the connection:
  - `superseded`
  - `heartbeat_timeout`
  - `flow_control_violation`
  - `protocol_error`
  - `client_logout`

The server also drops connections without any `LOGOUT`. This happens at random, roughly every 1–3 minutes, and sometimes in the middle of a frame.

---

## 3. Channels (multiplexing)

**One channel per merchant.** Every merchant is a channel named by its merchant ID (`M-0001` …). All channels share the one TCP session.

**Sequence numbers.** Each channel is an independent, ordered stream of frames numbered `1, 2, 3, …`, with no gaps in the content. Sequence numbers are per channel, not per session:
- `M-0001` seq 7 and `M-0002` seq 7 are unrelated;
- there is no ordering between channels.

**Frame types.** Three frame types carry channel sequence numbers:

| Frame | Meaning |
|---|---|
| `DATA` | `{"type":"DATA","channel":"M-0001","seq":42,"poss_dup":false,"event":{…}}`. One ledger event (see `ASSIGNMENT.md` for the event schema and rules). |
| `NOTICE` | `{"type":"NOTICE","channel":"M-0001","seq":43,"poss_dup":false,"text":"…"}`. An informational message with **no ledger effect**. It still occupies its sequence number. |
| `GAP_FILL` | `{"type":"GAP_FILL","channel":"M-0001","seq":43,"new_seq":45,"poss_dup":true}`. Sequence numbers `seq … new_seq-1` contain nothing you need, and the next frame you should expect on this channel is `new_seq`. It is only sent when the server **re-sends** a range that consisted of `NOTICE`s. |

### 3.1 Your processing obligations, per channel

- **Order.** Process a channel's frames strictly in sequence order.
- **Contiguity.** You may only consider seq `n` processed once `1 … n-1` are processed. A `GAP_FILL` counts as processing its range.
- **Duplicates.** A frame whose `seq` is at or below what you have already processed is a duplicate. Ignore it. This can happen with `poss_dup: true` *or* `poss_dup: false`, because the wire duplicates frames too.
- **Gaps.** A frame whose `seq` is above the next one you expect means frames were lost. Request a resend (§3.2).
- **What `poss_dup` means.** `poss_dup: true` means the server may have sent this sequence number before, in this session or an earlier one. It is a hint, not a guarantee in either direction.

### 3.2 Resend

```json
{"type": "RESEND", "channel": "M-0001", "from_seq": 40}
```

- **The rewind.** The server rewinds that channel to `from_seq` and continues sending from there. This is go-back-N: frames after the gap that you already received will arrive again with `poss_dup: true`.
- **Resent `NOTICE`s.** A run of `NOTICE`s that you were previously sent comes back as one `GAP_FILL`.
- **Credit.** Resent frames consume credit like any other frame.
- **Lost resends.** A resend can itself be lost on the wire. Detecting that, and asking again, is your job.

### 3.3 Flow control (credit)

**Each channel starts every session with 0 credit.** The server sends nothing on a channel until you grant credit:

```json
{"type": "CREDIT", "channel": "M-0001", "n": 500}
```

| Rule | Detail |
|---|---|
| What consumes credit | Every `DATA` / `NOTICE` / `GAP_FILL` frame the server **writes to the socket** on that channel consumes 1 credit. Frames lost on the wire do **not**. |
| Credit is additive | Outstanding (granted but unused) credit may not exceed `max_outstanding_credit` (10,000) per channel. Exceeding it ends the session with `LOGOUT flow_control_violation`. |
| Credit does not survive a session | A new session starts at 0 everywhere. |

Credit is how you apply backpressure. Granting it is a statement that you have room to take more of that channel's frames. When you choose to grant it, relative to what you have made durable, is a design decision you will be asked to defend.

### 3.4 What the server does to you (chaos)

All of the following happen with default settings. The probabilities are configurable in `gateway.py`; reviewers turn them up.

- Frames are **lost** (~0.2%), which produces sequence gaps.
- Frames are **duplicated** (~0.1%), sometimes with `poss_dup: false`.
- The connection is **dropped** every 1–3 minutes, about a third of the time after writing half of a frame.
- Sending **stalls** for 1–3 s.
- Event release is **bursty**: by default 1,000 frames/s, rising to 4× for 10 s out of every 90 s.
- Traffic is **skewed**: a handful of merchants carry most of the volume.

---

## 4. Client → server frame summary

| `type` | Fields | Notes |
|---|---|---|
| `LOGON` | `client_id`, `secret`, `resume` | The first frame only. |
| `CREDIT` | `channel`, `n` (int > 0) | |
| `RESEND` | `channel`, `from_seq` (int ≥ 1) | Go-back-N from `from_seq`. |
| `HEARTBEAT` | `test_req_id` (optional) | Must echo the id when answering `TEST_REQUEST`. |
| `LOGOUT` | — | |

Any other frame type, or a malformed field, ends the session with `LOGOUT protocol_error`.

## 5. Server → client frame summary

| `type` | Notes |
|---|---|
| `LOGON_ACK` / `LOGON_REJECT` | §2.1 |
| `DATA` / `NOTICE` / `GAP_FILL` | §3; channel-sequenced. |
| `HEARTBEAT` / `TEST_REQUEST` | §2.3 |
| `LOGOUT` | §2.4 |
