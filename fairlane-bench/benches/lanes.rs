//! S5, S6 and S8: several lanes, batches, and the wait of an urgent item behind a saturated lane.
//! No law asserts a time: these inform a release and gate nothing.

use std::{
  num::NonZeroUsize,
  sync::mpsc,
  thread,
  time::{Duration, Instant},
};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fairlane::{Lane, LaneOptions, Options, Quota, Receiver, Sender, Single, Strict};

const ITEMS: u64 = 100_000;
const CAPACITY: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Lane)]
enum Two {
  High,
  Low,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Lane)]
enum Three {
  High,
  Mid,
  Low,
}

fn capacity() -> NonZeroUsize {
  NonZeroUsize::new(CAPACITY).unwrap()
}

fn options<L: Lane>() -> Options<L> {
  Options::from_fn(|_| LaneOptions::new(capacity()))
}

/// `senders` threads send `ITEMS` in all, round robin over `lanes`, so that every lane stays
/// saturated; `receivers` threads take them until the channel ends.
fn saturate<L: Lane, Y: fairlane::Policy<L>>(
  (tx, rx): (Sender<u64, L, Y>, Receiver<u64, L, Y>),
  senders: u64,
  receivers: usize,
  lanes: &[L],
) where
  Sender<u64, L, Y>: Send,
  Receiver<u64, L, Y>: Send,
  L: Sync,
{
  thread::scope(|scope| {
    for _ in 0..senders {
      let tx = tx.clone();
      scope.spawn(move || {
        for item in 0..ITEMS / senders {
          let lane = lanes[(item % lanes.len() as u64) as usize];
          tx.send_blocking(lane, item).unwrap();
        }
      });
    }
    drop(tx);
    for _ in 0..receivers {
      let rx = rx.clone();
      scope.spawn(move || while rx.recv_blocking().is_ok() {});
    }
  });
}

/// S5: two and three lanes, N:1 and N:N, every lane saturated; `Strict` and the default policy.
fn s5(c: &mut Criterion) {
  let mut group = c.benchmark_group("s5_lanes_saturated");
  group.throughput(Throughput::Elements(ITEMS));
  for (senders, receivers) in [(4, 1), (4, 4)] {
    let shape = format!("{senders}:{receivers}");
    group.bench_with_input(BenchmarkId::new("two/strict", &shape), &(), |b, ()| {
      b.iter(|| {
        let halves = fairlane::channel_with::<u64, Two, _>(options(), Strict).unwrap();
        saturate(halves, senders, receivers, &[Two::High, Two::Low]);
      })
    });
    group.bench_with_input(BenchmarkId::new("two/default", &shape), &(), |b, ()| {
      b.iter(|| {
        let halves = fairlane::channel::<u64, Two>(options()).unwrap();
        saturate(halves, senders, receivers, &[Two::High, Two::Low]);
      })
    });
    group.bench_with_input(BenchmarkId::new("three/strict", &shape), &(), |b, ()| {
      b.iter(|| {
        let halves = fairlane::channel_with::<u64, Three, _>(options(), Strict).unwrap();
        saturate(
          halves,
          senders,
          receivers,
          &[Three::High, Three::Mid, Three::Low],
        );
      })
    });
    group.bench_with_input(BenchmarkId::new("three/default", &shape), &(), |b, ()| {
      b.iter(|| {
        let halves = fairlane::channel::<u64, Three>(options()).unwrap();
        saturate(
          halves,
          senders,
          receivers,
          &[Three::High, Three::Mid, Three::Low],
        );
      })
    });
  }
  group.finish();
}

/// One sender thread sends `ITEMS`; this thread takes them in batches of up to `batch`, or one at
/// a time where `batch` is 1.
fn batches(batch: usize) {
  let (tx, rx) = fairlane::bounded::<u64>(capacity()).unwrap();
  let sender = thread::spawn(move || {
    for item in 0..ITEMS {
      tx.send_blocking(Single, item).unwrap();
    }
  });
  if batch == 1 {
    while rx.recv_blocking().is_ok() {}
  } else {
    let mut into = Vec::with_capacity(batch);
    while rx
      .recv_many_blocking(&mut into, Quota::up_to(batch))
      .is_ok()
    {
      into.clear();
    }
  }
  sender.join().unwrap();
}

/// S6: `recv_many` of 32 against 32 single receives.
fn s6(c: &mut Criterion) {
  let mut group = c.benchmark_group("s6_batches");
  group.throughput(Throughput::Elements(ITEMS));
  group.bench_function("recv_many_32", |b| b.iter(|| batches(32)));
  group.bench_function("recv", |b| b.iter(|| batches(1)));
  group.finish();
}

/// Four threads keep the low lane full; `iters` urgent items are sent one at a time, each after
/// the last was received, and the wait of each, from its send to its receipt, is summed.
fn urgent_fairlane(iters: u64) -> Duration {
  let options = Options::from_fn(|lane| {
    let options = LaneOptions::new(capacity());
    if lane == Two::High {
      options.with_express(true)
    } else {
      options
    }
  });
  let (tx, rx) = fairlane::channel::<Option<Instant>, Two>(options).unwrap();
  let (waits, waited) = mpsc::channel();
  let receiver = thread::spawn(move || {
    while let Ok(received) = rx.recv_blocking() {
      if let Some(sent) = received.into_item() {
        waits.send(sent.elapsed()).unwrap();
      }
    }
  });
  let floods: Vec<_> = (0..4)
    .map(|_| {
      let tx = tx.clone();
      thread::spawn(move || while tx.send_blocking(Two::Low, None).is_ok() {})
    })
    .collect();
  let mut total = Duration::ZERO;
  for _ in 0..iters {
    tx.send_blocking(Two::High, Some(Instant::now())).unwrap();
    total += waited.recv().unwrap();
  }
  tx.close();
  for flood in floods {
    flood.join().unwrap();
  }
  receiver.join().unwrap();
  total
}

/// The same with two flume channels: the receiver tries the urgent one first, then selects over
/// both.
fn urgent_flume(iters: u64) -> Duration {
  let (urgent_tx, urgent_rx) = flume::bounded::<Option<Instant>>(CAPACITY);
  let (low_tx, low_rx) = flume::bounded::<Option<Instant>>(CAPACITY);
  let (waits, waited) = mpsc::channel();
  let receiver = thread::spawn(move || {
    loop {
      let next = match urgent_rx.try_recv() {
        Ok(item) => Ok(item),
        Err(flume::TryRecvError::Disconnected) => break,
        Err(flume::TryRecvError::Empty) => flume::Selector::new()
          .recv(&urgent_rx, |item| item)
          .recv(&low_rx, |item| item)
          .wait(),
      };
      match next {
        Ok(Some(sent)) => waits.send(sent.elapsed()).unwrap(),
        Ok(None) => {}
        Err(_) => break,
      }
    }
  });
  let floods: Vec<_> = (0..4)
    .map(|_| {
      let low_tx = low_tx.clone();
      thread::spawn(move || while low_tx.send(None).is_ok() {})
    })
    .collect();
  drop(low_tx);
  let mut total = Duration::ZERO;
  for _ in 0..iters {
    urgent_tx.send(Some(Instant::now())).unwrap();
    total += waited.recv().unwrap();
  }
  drop(urgent_tx);
  receiver.join().unwrap();
  for flood in floods {
    flood.join().unwrap();
  }
  total
}

/// The same with two async-channels and a select biased to the urgent one.
fn urgent_async_channel(iters: u64) -> Duration {
  let (urgent_tx, urgent_rx) = async_channel::bounded::<Option<Instant>>(CAPACITY);
  let (low_tx, low_rx) = async_channel::bounded::<Option<Instant>>(CAPACITY);
  let (waits, waited) = mpsc::channel();
  let receiver = thread::spawn(move || {
    futures_lite::future::block_on(async {
      loop {
        let next = futures_lite::future::or(urgent_rx.recv(), low_rx.recv()).await;
        match next {
          Ok(Some(sent)) => waits.send(sent.elapsed()).unwrap(),
          Ok(None) => {}
          Err(_) => break,
        }
      }
    });
    urgent_rx.close();
    low_rx.close();
  });
  let floods: Vec<_> = (0..4)
    .map(|_| {
      let low_tx = low_tx.clone();
      thread::spawn(move || while low_tx.send_blocking(None).is_ok() {})
    })
    .collect();
  drop(low_tx);
  let mut total = Duration::ZERO;
  for _ in 0..iters {
    urgent_tx.send_blocking(Some(Instant::now())).unwrap();
    total += waited.recv().unwrap();
  }
  drop(urgent_tx);
  receiver.join().unwrap();
  for flood in floods {
    flood.join().unwrap();
  }
  total
}

/// S8: the wait of an urgent item behind a saturated lane, against two channels and a selector.
fn s8(c: &mut Criterion) {
  let mut group = c.benchmark_group("s8_urgent_behind_a_flood");
  group.bench_function("fairlane/express", |b| b.iter_custom(urgent_fairlane));
  group.bench_function("flume/selector", |b| b.iter_custom(urgent_flume));
  group.bench_function("async-channel/biased_or", |b| {
    b.iter_custom(urgent_async_channel)
  });
  group.finish();
}

criterion_group!(benches, s5, s6, s8);
criterion_main!(benches);
