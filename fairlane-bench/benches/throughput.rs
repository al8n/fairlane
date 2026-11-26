//! S1, S2, S3 and S7: one lane, against flume, crossbeam-channel, async-channel, kanal and tokio's
//! mpsc, each bounded alike. No law asserts a time: these inform a release and gate nothing.

use std::{num::NonZeroUsize, thread};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use fairlane::Single;

const ITEMS: u64 = 100_000;
const CAPACITY: usize = 1024;

fn capacity() -> NonZeroUsize {
  NonZeroUsize::new(CAPACITY).unwrap()
}

fn runtime() -> tokio::runtime::Runtime {
  tokio::runtime::Builder::new_multi_thread()
    .worker_threads(4)
    .build()
    .unwrap()
}

/// `senders` threads send `ITEMS` in all; `receivers` threads take them until the channel ends.
fn fairlane_blocking(senders: u64, receivers: usize, reserve_first: bool) {
  let (tx, rx) = fairlane::bounded::<u64>(capacity()).unwrap();
  thread::scope(|scope| {
    for _ in 0..senders {
      let tx = tx.clone();
      scope.spawn(move || {
        for item in 0..ITEMS / senders {
          if reserve_first {
            tx.reserve_blocking(Single).unwrap().send(item).unwrap();
          } else {
            tx.send_blocking(Single, item).unwrap();
          }
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

fn crossbeam_blocking(senders: u64, receivers: usize) {
  let (tx, rx) = crossbeam_channel::bounded::<u64>(CAPACITY);
  thread::scope(|scope| {
    for _ in 0..senders {
      let tx = tx.clone();
      scope.spawn(move || {
        for item in 0..ITEMS / senders {
          tx.send(item).unwrap();
        }
      });
    }
    drop(tx);
    for _ in 0..receivers {
      let rx = rx.clone();
      scope.spawn(move || while rx.recv().is_ok() {});
    }
  });
}

fn flume_blocking(senders: u64, receivers: usize) {
  let (tx, rx) = flume::bounded::<u64>(CAPACITY);
  thread::scope(|scope| {
    for _ in 0..senders {
      let tx = tx.clone();
      scope.spawn(move || {
        for item in 0..ITEMS / senders {
          tx.send(item).unwrap();
        }
      });
    }
    drop(tx);
    for _ in 0..receivers {
      let rx = rx.clone();
      scope.spawn(move || while rx.recv().is_ok() {});
    }
  });
}

fn kanal_blocking(senders: u64, receivers: usize) {
  let (tx, rx) = kanal::bounded::<u64>(CAPACITY);
  thread::scope(|scope| {
    for _ in 0..senders {
      let tx = tx.clone();
      scope.spawn(move || {
        for item in 0..ITEMS / senders {
          tx.send(item).unwrap();
        }
      });
    }
    drop(tx);
    for _ in 0..receivers {
      let rx = rx.clone();
      scope.spawn(move || while rx.recv().is_ok() {});
    }
  });
}

async fn fairlane_awaited(senders: u64, receivers: usize) {
  let (tx, rx) = fairlane::bounded::<u64>(capacity()).unwrap();
  let mut tasks = Vec::new();
  for _ in 0..senders {
    let tx = tx.clone();
    tasks.push(tokio::spawn(async move {
      for item in 0..ITEMS / senders {
        tx.send(Single, item).await.unwrap();
      }
    }));
  }
  drop(tx);
  for _ in 0..receivers {
    let rx = rx.clone();
    tasks.push(tokio::spawn(
      async move { while rx.recv().await.is_ok() {} },
    ));
  }
  drop(rx);
  for task in tasks {
    task.await.unwrap();
  }
}

async fn tokio_awaited(senders: u64) {
  let (tx, mut rx) = tokio::sync::mpsc::channel::<u64>(CAPACITY);
  let mut tasks = Vec::new();
  for _ in 0..senders {
    let tx = tx.clone();
    tasks.push(tokio::spawn(async move {
      for item in 0..ITEMS / senders {
        tx.send(item).await.unwrap();
      }
    }));
  }
  drop(tx);
  tasks.push(tokio::spawn(
    async move { while rx.recv().await.is_some() {} },
  ));
  for task in tasks {
    task.await.unwrap();
  }
}

async fn async_channel_awaited(senders: u64, receivers: usize) {
  let (tx, rx) = async_channel::bounded::<u64>(CAPACITY);
  let mut tasks = Vec::new();
  for _ in 0..senders {
    let tx = tx.clone();
    tasks.push(tokio::spawn(async move {
      for item in 0..ITEMS / senders {
        tx.send(item).await.unwrap();
      }
    }));
  }
  drop(tx);
  for _ in 0..receivers {
    let rx = rx.clone();
    tasks.push(tokio::spawn(
      async move { while rx.recv().await.is_ok() {} },
    ));
  }
  drop(rx);
  for task in tasks {
    task.await.unwrap();
  }
}

async fn flume_awaited(senders: u64, receivers: usize) {
  let (tx, rx) = flume::bounded::<u64>(CAPACITY);
  let mut tasks = Vec::new();
  for _ in 0..senders {
    let tx = tx.clone();
    tasks.push(tokio::spawn(async move {
      for item in 0..ITEMS / senders {
        tx.send_async(item).await.unwrap();
      }
    }));
  }
  drop(tx);
  for _ in 0..receivers {
    let rx = rx.clone();
    tasks.push(tokio::spawn(async move {
      while rx.recv_async().await.is_ok() {}
    }));
  }
  drop(rx);
  for task in tasks {
    task.await.unwrap();
  }
}

async fn kanal_awaited(senders: u64, receivers: usize) {
  let (tx, rx) = kanal::bounded_async::<u64>(CAPACITY);
  let mut tasks = Vec::new();
  for _ in 0..senders {
    let tx = tx.clone();
    tasks.push(tokio::spawn(async move {
      for item in 0..ITEMS / senders {
        tx.send(item).await.unwrap();
      }
    }));
  }
  drop(tx);
  for _ in 0..receivers {
    let rx = rx.clone();
    tasks.push(tokio::spawn(
      async move { while rx.recv().await.is_ok() {} },
    ));
  }
  drop(rx);
  for task in tasks {
    task.await.unwrap();
  }
}

/// An awaited sender task into a blocking receiver thread.
fn fairlane_awaited_into_blocking(runtime: &tokio::runtime::Runtime) {
  let (tx, rx) = fairlane::bounded::<u64>(capacity()).unwrap();
  let receiver = thread::spawn(move || while rx.recv_blocking().is_ok() {});
  runtime.block_on(async move {
    for item in 0..ITEMS {
      tx.send(Single, item).await.unwrap();
    }
  });
  receiver.join().unwrap();
}

/// S1: 1:1, one lane.
fn s1(c: &mut Criterion) {
  let runtime = runtime();
  let mut group = c.benchmark_group("s1_one_to_one");
  group.throughput(Throughput::Elements(ITEMS));
  group.bench_function("fairlane/blocking", |b| {
    b.iter(|| fairlane_blocking(1, 1, false))
  });
  group.bench_function("crossbeam-channel/blocking", |b| {
    b.iter(|| crossbeam_blocking(1, 1))
  });
  group.bench_function("flume/blocking", |b| b.iter(|| flume_blocking(1, 1)));
  group.bench_function("kanal/blocking", |b| b.iter(|| kanal_blocking(1, 1)));
  group.bench_function("fairlane/awaited", |b| {
    b.iter(|| runtime.block_on(fairlane_awaited(1, 1)))
  });
  group.bench_function("tokio-mpsc/awaited", |b| {
    b.iter(|| runtime.block_on(tokio_awaited(1)))
  });
  group.bench_function("async-channel/awaited", |b| {
    b.iter(|| runtime.block_on(async_channel_awaited(1, 1)))
  });
  group.bench_function("flume/awaited", |b| {
    b.iter(|| runtime.block_on(flume_awaited(1, 1)))
  });
  group.bench_function("kanal/awaited", |b| {
    b.iter(|| runtime.block_on(kanal_awaited(1, 1)))
  });
  group.bench_function("fairlane/awaited_into_blocking", |b| {
    b.iter(|| fairlane_awaited_into_blocking(&runtime))
  });
  group.finish();
}

/// S2: N:1, one lane, 4 and 16 senders.
fn s2(c: &mut Criterion) {
  let runtime = runtime();
  let mut group = c.benchmark_group("s2_many_to_one");
  group.throughput(Throughput::Elements(ITEMS));
  for senders in [4, 16] {
    group.bench_with_input(BenchmarkId::new("fairlane", senders), &senders, |b, &n| {
      b.iter(|| fairlane_blocking(n, 1, false))
    });
    group.bench_with_input(
      BenchmarkId::new("crossbeam-channel", senders),
      &senders,
      |b, &n| b.iter(|| crossbeam_blocking(n, 1)),
    );
    group.bench_with_input(BenchmarkId::new("flume", senders), &senders, |b, &n| {
      b.iter(|| flume_blocking(n, 1))
    });
    group.bench_with_input(BenchmarkId::new("kanal", senders), &senders, |b, &n| {
      b.iter(|| kanal_blocking(n, 1))
    });
    group.bench_with_input(
      BenchmarkId::new("tokio-mpsc", senders),
      &senders,
      |b, &n| b.iter(|| runtime.block_on(tokio_awaited(n))),
    );
  }
  group.finish();
}

/// S3: 1:N and N:N, one lane.
fn s3(c: &mut Criterion) {
  let runtime = runtime();
  let mut group = c.benchmark_group("s3_many_receivers");
  group.throughput(Throughput::Elements(ITEMS));
  for (senders, receivers) in [(1, 4), (4, 4), (16, 16)] {
    let shape = format!("{senders}:{receivers}");
    group.bench_with_input(BenchmarkId::new("fairlane", &shape), &(), |b, ()| {
      b.iter(|| fairlane_blocking(senders, receivers, false))
    });
    group.bench_with_input(
      BenchmarkId::new("crossbeam-channel", &shape),
      &(),
      |b, ()| b.iter(|| crossbeam_blocking(senders, receivers)),
    );
    group.bench_with_input(BenchmarkId::new("flume", &shape), &(), |b, ()| {
      b.iter(|| flume_blocking(senders, receivers))
    });
    group.bench_with_input(BenchmarkId::new("kanal", &shape), &(), |b, ()| {
      b.iter(|| kanal_blocking(senders, receivers))
    });
    group.bench_with_input(BenchmarkId::new("async-channel", &shape), &(), |b, ()| {
      b.iter(|| runtime.block_on(async_channel_awaited(senders, receivers)))
    });
  }
  group.finish();
}

/// S7: `reserve` then `send`, against `send`.
fn s7(c: &mut Criterion) {
  let mut group = c.benchmark_group("s7_two_steps");
  group.throughput(Throughput::Elements(ITEMS));
  group.bench_function("send", |b| b.iter(|| fairlane_blocking(1, 1, false)));
  group.bench_function("reserve_then_send", |b| {
    b.iter(|| fairlane_blocking(1, 1, true))
  });
  group.finish();
}

criterion_group!(benches, s1, s2, s3, s7);
criterion_main!(benches);
