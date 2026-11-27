<div align="center">
<h1>fairlane</h1>
</div>
<div align="center">

A bounded multi-lane MPMC channel: a bound per lane, an anti-starvation policy, batched
receives, and both awaited and blocking doors.

[<img alt="github" src="https://img.shields.io/badge/github-al8n/fairlane-8da0cb?style=for-the-badge&logo=Github" height="22">][Github-url]
[<img alt="Build" src="https://img.shields.io/github/actions/workflow/status/al8n/fairlane/ci.yml?logo=Github-Actions&style=for-the-badge" height="22">][CI-url]
[<img alt="codecov" src="https://img.shields.io/codecov/c/gh/al8n/fairlane?style=for-the-badge&logo=codecov" height="22">][codecov-url]

[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-fairlane-66c2a5?style=for-the-badge&labelColor=555555&logo=data:image/svg+xml;base64,PHN2ZyByb2xlPSJpbWciIHhtbG5zPSJodHRwOi8vd3d3LnczLm9yZy8yMDAwL3N2ZyIgdmlld0JveD0iMCAwIDUxMiA1MTIiPjxwYXRoIGZpbGw9IiNmNWY1ZjUiIGQ9Ik00ODguNiAyNTAuMkwzOTIgMjE0VjEwNS41YzAtMTUtOS4zLTI4LjQtMjMuNC0zMy43bC0xMDAtMzcuNWMtOC4xLTMuMS0xNy4xLTMuMS0yNS4zIDBsLTEwMCAzNy41Yy0xNC4xIDUuMy0yMy40IDE4LjctMjMuNCAzMy43VjIxNGwtOTYuNiAzNi4yQzkuMyAyNTUuNSAwIDI2OC45IDAgMjgzLjlWMzk0YzAgMTMuNiA3LjcgMjYuMSAxOS45IDMyLjJsMTAwIDUwYzEwLjEgNS4xIDIyLjEgNS4xIDMyLjIgMGwxMDMuOS01MiAxMDMuOSA1MmMxMC4xIDUuMSAyMi4xIDUuMSAzMi4yIDBsMTAwLTUwYzEyLjItNi4xIDE5LjktMTguNiAxOS45LTMyLjJWMjgzLjljMC0xNS05LjMtMjguNC0yMy40LTMzLjd6TTM1OCAyMTQuOGwtODUgMzEuOXYtNjguMmw4NS0zN3Y3My4zek0xNTQgMTA0LjFsMTAyLTM4LjIgMTAyIDM4LjJ2LjZsLTEwMiA0MS40LTEwMi00MS40di0uNnptODQgMjkxLjFsLTg1IDQyLjV2LTc5LjFsODUtMzguOHY3NS40em0wLTExMmwtMTAyIDQxLjQtMTAyLTQxLjR2LS42bDEwMi0zOC4yIDEwMiAzOC4ydi42em0yNDAgMTEybC04NSA0Mi41di03OS4xbDg1LTM4Ljh2NzUuNHptMC0xMTJsLTEwMiA0MS40LTEwMi00MS40di0uNmwxMDItMzguMiAxMDIgMzguMnYuNnoiPjwvcGF0aD48L3N2Zz4K" height="20">][doc-url]
[<img alt="crates.io" src="https://img.shields.io/crates/v/fairlane?style=for-the-badge&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBlbmNvZGluZz0iaXNvLTg4NTktMSI/Pg0KPCEtLSBHZW5lcmF0b3I6IEFkb2JlIElsbHVzdHJhdG9yIDE5LjAuMCwgU1ZHIEV4cG9ydCBQbHVnLUluIC4gU1ZHIFZlcnNpb246IDYuMDAgQnVpbGQgMCkgIC0tPg0KPHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCINCgkgdmlld0JveD0iMCAwIDUxMiA1MTIiIHhtbDpzcGFjZT0icHJlc2VydmUiPg0KPGc+DQoJPGc+DQoJCTxwYXRoIGQ9Ik0yNTYsMEwzMS41MjgsMTEyLjIzNnYyODcuNTI4TDI1Niw1MTJsMjI0LjQ3Mi0xMTIuMjM2VjExMi4yMzZMMjU2LDB6IE0yMzQuMjc3LDQ1Mi41NjRMNzQuOTc0LDM3Mi45MTNWMTYwLjgxDQoJCQlsMTU5LjMwMyw3OS42NTFWNDUyLjU2NHogTTEwMS44MjYsMTI1LjY2MkwyNTYsNDguNTc2bDE1NC4xNzQsNzcuMDg3TDI1NiwyMDIuNzQ5TDEwMS44MjYsMTI1LjY2MnogTTQzNy4wMjYsMzcyLjkxMw0KCQkJbC0xNTkuMzAzLDc5LjY1MVYyNDAuNDYxbDE1OS4zMDMtNzkuNjUxVjM3Mi45MTN6IiBmaWxsPSIjRkZGIi8+DQoJPC9nPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPGc+DQo8L2c+DQo8Zz4NCjwvZz4NCjxnPg0KPC9nPg0KPC9zdmc+DQo=" height="22">][crates-url]
[<img alt="crates.io" src="https://img.shields.io/crates/d/fairlane?color=critical&logo=data:image/svg+xml;base64,PD94bWwgdmVyc2lvbj0iMS4wIiBzdGFuZGFsb25lPSJubyI/PjwhRE9DVFlQRSBzdmcgUFVCTElDICItLy9XM0MvL0RURCBTVkcgMS4xLy9FTiIgImh0dHA6Ly93d3cudzMub3JnL0dyYXBoaWNzL1NWRy8xLjEvRFREL3N2ZzExLmR0ZCI+PHN2ZyB0PSIxNjQ1MTE3MzMyOTU5IiBjbGFzcz0iaWNvbiIgdmlld0JveD0iMCAwIDEwMjQgMTAyNCIgdmVyc2lvbj0iMS4xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHAtaWQ9IjM0MjEiIGRhdGEtc3BtLWFuY2hvci1pZD0iYTMxM3guNzc4MTA2OS4wLmkzIiB3aWR0aD0iNDgiIGhlaWdodD0iNDgiIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIj48ZGVmcz48c3R5bGUgdHlwZT0idGV4dC9jc3MiPjwvc3R5bGU+PC9kZWZzPjxwYXRoIGQ9Ik00NjkuMzEyIDU3MC4yNHYtMjU2aDg1LjM3NnYyNTZoMTI4TDUxMiA3NTYuMjg4IDM0MS4zMTIgNTcwLjI0aDEyOHpNMTAyNCA2NDAuMTI4QzEwMjQgNzgyLjkxMiA5MTkuODcyIDg5NiA3ODcuNjQ4IDg5NmgtNTEyQzEyMy45MDQgODk2IDAgNzYxLjYgMCA1OTcuNTA0IDAgNDUxLjk2OCA5NC42NTYgMzMxLjUyIDIyNi40MzIgMzAyLjk3NiAyODQuMTYgMTk1LjQ1NiAzOTEuODA4IDEyOCA1MTIgMTI4YzE1Mi4zMiAwIDI4Mi4xMTIgMTA4LjQxNiAzMjMuMzkyIDI2MS4xMkM5NDEuODg4IDQxMy40NCAxMDI0IDUxOS4wNCAxMDI0IDY0MC4xOTJ6IG0tMjU5LjItMjA1LjMxMmMtMjQuNDQ4LTEyOS4wMjQtMTI4Ljg5Ni0yMjIuNzItMjUyLjgtMjIyLjcyLTk3LjI4IDAtMTgzLjA0IDU3LjM0NC0yMjQuNjQgMTQ3LjQ1NmwtOS4yOCAyMC4yMjQtMjAuOTI4IDIuOTQ0Yy0xMDMuMzYgMTQuNC0xNzguMzY4IDEwNC4zMi0xNzguMzY4IDIxNC43MiAwIDExNy45NTIgODguODMyIDIxNC40IDE5Ni45MjggMjE0LjRoNTEyYzg4LjMyIDAgMTU3LjUwNC03NS4xMzYgMTU3LjUwNC0xNzEuNzEyIDAtODguMDY0LTY1LjkyLTE2NC45MjgtMTQ0Ljk2LTE3MS43NzZsLTI5LjUwNC0yLjU2LTUuODg4LTMwLjk3NnoiIGZpbGw9IiNmZmZmZmYiIHAtaWQ9IjM0MjIiIGRhdGEtc3BtLWFuY2hvci1pZD0iYTMxM3guNzc4MTA2OS4wLmkwIiBjbGFzcz0iIj48L3BhdGg+PC9zdmc+&style=for-the-badge" height="22">][crates-url]
<img alt="license" src="https://img.shields.io/badge/License-Apache%202.0/MIT-blue.svg?style=for-the-badge&fontColor=white&logoColor=f5c076&logo=data:image/svg+xml;base64,PCFET0NUWVBFIHN2ZyBQVUJMSUMgIi0vL1czQy8vRFREIFNWRyAxLjEvL0VOIiAiaHR0cDovL3d3dy53My5vcmcvR3JhcGhpY3MvU1ZHLzEuMS9EVEQvc3ZnMTEuZHRkIj4KDTwhLS0gVXBsb2FkZWQgdG86IFNWRyBSZXBvLCB3d3cuc3ZncmVwby5jb20sIFRyYW5zZm9ybWVkIGJ5OiBTVkcgUmVwbyBNaXhlciBUb29scyAtLT4KPHN2ZyBmaWxsPSIjZmZmZmZmIiBoZWlnaHQ9IjgwMHB4IiB3aWR0aD0iODAwcHgiIHZlcnNpb249IjEuMSIgaWQ9IkNhcGFfMSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIiB4bWxuczp4bGluaz0iaHR0cDovL3d3dy53My5vcmcvMTk5OS94bGluayIgdmlld0JveD0iMCAwIDI3Ni43MTUgMjc2LjcxNSIgeG1sOnNwYWNlPSJwcmVzZXJ2ZSIgc3Ryb2tlPSIjZmZmZmZmIj4KDTxnIGlkPSJTVkdSZXBvX2JnQ2FycmllciIgc3Ryb2tlLXdpZHRoPSIwIi8+Cg08ZyBpZD0iU1ZHUmVwb190cmFjZXJDYXJyaWVyIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiLz4KDTxnIGlkPSJTVkdSZXBvX2ljb25DYXJyaWVyIj4gPGc+IDxwYXRoIGQ9Ik0xMzguMzU3LDBDNjIuMDY2LDAsMCw2Mi4wNjYsMCwxMzguMzU3czYyLjA2NiwxMzguMzU3LDEzOC4zNTcsMTM4LjM1N3MxMzguMzU3LTYyLjA2NiwxMzguMzU3LTEzOC4zNTcgUzIxNC42NDgsMCwxMzguMzU3LDB6IE0xMzguMzU3LDI1OC43MTVDNzEuOTkyLDI1OC43MTUsMTgsMjA0LjcyMywxOCwxMzguMzU3UzcxLjk5MiwxOCwxMzguMzU3LDE4IHMxMjAuMzU3LDUzLjk5MiwxMjAuMzU3LDEyMC4zNTdTMjA0LjcyMywyNTguNzE1LDEzOC4zNTcsMjU4LjcxNXoiLz4gPHBhdGggZD0iTTE5NC43OTgsMTYwLjkwM2MtNC4xODgtMi42NzctOS43NTMtMS40NTQtMTIuNDMyLDIuNzMyYy04LjY5NCwxMy41OTMtMjMuNTAzLDIxLjcwOC0zOS42MTQsMjEuNzA4IGMtMjUuOTA4LDAtNDYuOTg1LTIxLjA3OC00Ni45ODUtNDYuOTg2czIxLjA3Ny00Ni45ODYsNDYuOTg1LTQ2Ljk4NmMxNS42MzMsMCwzMC4yLDcuNzQ3LDM4Ljk2OCwyMC43MjMgYzIuNzgyLDQuMTE3LDguMzc1LDUuMjAxLDEyLjQ5NiwyLjQxOGM0LjExOC0yLjc4Miw1LjIwMS04LjM3NywyLjQxOC0xMi40OTZjLTEyLjExOC0xNy45MzctMzIuMjYyLTI4LjY0NS01My44ODItMjguNjQ1IGMtMzUuODMzLDAtNjQuOTg1LDI5LjE1Mi02NC45ODUsNjQuOTg2czI5LjE1Miw2NC45ODYsNjQuOTg1LDY0Ljk4NmMyMi4yODEsMCw0Mi43NTktMTEuMjE4LDU0Ljc3OC0zMC4wMDkgQzIwMC4yMDgsMTY5LjE0NywxOTguOTg1LDE2My41ODIsMTk0Ljc5OCwxNjAuOTAzeiIvPiA8L2c+IDwvZz4KDTwvc3ZnPg==" height="22">

</div>

## Introduction

`fairlane` is a bounded MPMC channel whose capacity is divided into lanes. A lane is a priority
class with its own queue and its own bound, so a full lane never holds a sender of another lane.

- **Lanes are your own enum.** `#[derive(Lane)]` numbers them in declaration order; lane 0 is
  served first.
- **Every lane has a bound,** and a capacity has no default: it is a required decision.
- **Express lanes** come first in the enum and are always served first; the policy never sees
  them.
- **A policy against starvation.** The default owes a lane service once it has been passed over 8
  times, or once its oldest item has waited 2 seconds, whichever comes first. Both numbers can be
  changed per lane while the channel runs.
- **Batched receives.** `recv_many` takes up to a limit in one call, choosing lane by lane for
  each item, with an optional limit per lane and a range of lanes it admits.
- **Every wait has three forms:** awaited (`send`, `reserve`, `recv`, `recv_many`), blocking
  (`send_blocking`, …), and one that never waits (`try_send`, …). Any runtime will do, or none.
- **Two steps where they help.** `reserve` waits for room and hands back a permit; the item is
  built and committed only once the room is there.

## Installation

```toml
[dependencies]
fairlane = { version = "0.1", features = ["derive"] }
```

## Quick start

```rust
use core::num::NonZeroUsize;

use fairlane::{Lane, LaneOptions, Options, Quota};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Lane)]
enum Urgency {
  #[fairlane(express)]
  Control,
  Interactive,
  Batch,
}

let options = Options::from_fn(|lane| match lane {
  Urgency::Control => LaneOptions::new(NonZeroUsize::new(8).unwrap()),
  Urgency::Interactive => LaneOptions::new(NonZeroUsize::new(64).unwrap()),
  Urgency::Batch => LaneOptions::new(NonZeroUsize::new(1024).unwrap()),
});
let (tx, rx) = fairlane::channel::<&str, Urgency>(options).unwrap();

tx.send_blocking(Urgency::Batch, "reindex").unwrap();
tx.send_blocking(Urgency::Interactive, "search").unwrap();
tx.send_blocking(Urgency::Control, "pause").unwrap();

let mut batch = Vec::new();
rx.recv_many_blocking(&mut batch, Quota::up_to(8)).unwrap();
let items: Vec<&str> = batch.into_iter().map(|received| received.into_item()).collect();
assert_eq!(items, ["pause", "search", "reindex"]);
```

A channel of one lane is `bounded`. The awaited forms, and a permit:

```rust
use fairlane::{Receiver, Sender, Single};

async fn produce(tx: Sender<u64>) {
  for item in 0..100 {
    // Waits for room; the item is committed only once a unit of room is held.
    let Ok(permit) = tx.reserve(Single).await else { return };
    if permit.send(item).is_err() {
      return;
    }
  }
}

async fn consume(rx: Receiver<u64>) -> u64 {
  let mut batch = Vec::new();
  let mut sum = 0;
  while rx.recv_many(&mut batch, 32).await.is_ok() {
    sum += batch.drain(..).map(|received| received.into_item()).sum::<u64>();
  }
  sum
}
```

Each received item carries its lane and its ordinal, its place in that lane: 0, 1, 2 and on, with
no gap.

## Policies

| Policy | A lane is owed service |
| --- | --- |
| `Strict` | never: strict priority |
| `PassLimit` | once it has been passed over a number of times |
| `Elapsed` | once its oldest item has waited longer than a duration, by the policy's clock |
| `FirstOf<A, B>` | once either `A` or `B` says so |
| `DefaultPolicy` | `FirstOf<PassLimit, Elapsed>`, with `DEFAULT_PASS_LIMIT` (8) and `DEFAULT_MAX_WAIT` (2 s) |

`channel_with(options, policy)` takes any of them, or your own `Policy`. A policy is pure: it reads
a view of the waiting lanes and names the one owed, or none.

## Wakers, policies and items

A channel runs three kinds of your code while it works: the `Waker`s of the tasks that wait on it,
its `Policy`, and the destructors of its items. It has no executor of its own, so all of it runs on
the thread of the call that set it off.

- **A `Waker` must not panic, must not block on the channel or wait for another party's progress
  through it, and must not call a fairlane channel operation,** from `wake`, `wake_by_ref`, `clone`
  or `drop`: no send, receive or close, and no drop of a fairlane handle or future. A wake should
  be prompt and only schedule its task, as executors expect. A channel runs every waker outside its
  locks, once the state changes the wake stands for are made.
  - Fairlane's own waker, for its blocking calls, is std's `Thread::unpark`. On some targets
    (Windows 7's keyed events, the pthread parker) an unpark waits for the parked thread to reach
    its wait.
  - That thread needs no channel lock to get there: every queue guard is let go before it parks.
    So the unpark can delay the wakes after it, but never stop them.
- **A `Policy` must not block, and must not call any channel operation, on any channel.** Its
  `stamp` and `owed`, and the clock they read, run while a channel holds its choice lock, or while
  a send holds room it has not committed. A call into a channel there may deadlock, as any callback
  run under a lock may. Where it is cheap to tell, the call is refused by name instead: a receive,
  or a reserve that must wait, made on the same thread inside the choice of the same channel,
  panics with the cause.
- **An item's `Drop` must not block on anyone who waits on the same channel.** A disconnect drops
  the items it lets go, and a refund can run beside an item's drop; a destructor that waits for a
  sender or a receiver of the channel may deadlock. A close runs the wakes it resolves before the
  destructors of the items it drops, but a wake that a concurrent close is delivering may run
  later, so the order is no promise a destructor can wait on.
- **A `Policy::Stamp` should be small,** a few words: a receive's view of the lanes holds one
  reading per lane, each with a stamp, on the stack.

**What a breach still guarantees.** There is no undefined behaviour, and no unit of capacity is
lost. Others are not promised a prompt wake: a notification or a grant given to a task whose waker
panicked stays with that task's future until the future is polled or dropped. A panic unwinds out of
the call that ran the code. The value that call was handing back on that thread may be dropped by
the unwind: the item of a single receive (`recv`, `try_recv`, `recv_blocking`), or a send's item,
which it was given by value. A batched receive keeps every item it took in its `Vec`. The panic is
not swallowed: that would hide a bug in the executor or the policy.

**Fairlane allocates as std does.** During a call, an allocation failure aborts the process, as
`Vec::push` does. At construction, a capacity that cannot be laid out or allocated is refused as
`Malformed::Capacity`, since it comes from your options.

## Features

| Feature | Default | What it adds |
| --- | --- | --- |
| `derive` | no | `#[derive(Lane)]`, re-exported beside the trait |
| `serde` | no | `Serialize` and `Deserialize` for `LaneOptions`, `Options` and `PerLane`: a map keyed by each lane's name, every lane required |
| `either` | no | `Policy` for `either::Either`: one policy of two, chosen at run time |
| `test-util` | no | `ManualClock`, a clock a test sets |

The crate needs the standard library: it parks threads. It needs 64-bit atomics. Rust 1.85 is the
MSRV.

## Verification

- Every law is checked against a sequential model of the channel: the unit laws, and random
  walks that compare the channel with the model step by step.
- Concurrent histories on real threads are checked for linearizability against the same model.
- The waits and wakes are modelled under [loom](https://crates.io/crates/loom), every actor its
  own thread.
- The ring, the only `unsafe` code, runs under Miri with both Tree Borrows and Stacked Borrows.
- The derive's refusals are compile-fail tests.

## Benchmarks

`fairlane-bench`, a member of the workspace that is never published, compares fairlane with
flume, crossbeam-channel, async-channel, kanal and tokio's mpsc:

```bash
cargo bench -p fairlane-bench
```

#### License

`fairlane` is under the terms of both the MIT license and the
Apache License (Version 2.0).

See [LICENSE-APACHE](LICENSE-APACHE), [LICENSE-MIT](LICENSE-MIT) for details.

Copyright (c) 2026 Al Liu.

[Github-url]: https://github.com/al8n/fairlane/
[CI-url]: https://github.com/al8n/fairlane/actions/workflows/ci.yml
[doc-url]: https://docs.rs/fairlane
[crates-url]: https://crates.io/crates/fairlane
[codecov-url]: https://app.codecov.io/gh/al8n/fairlane/
