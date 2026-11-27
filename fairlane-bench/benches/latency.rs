//! Capacity 1, one item at a time back and forth between two threads, so that every receive
//! parks and every send wakes. No law asserts a time: these inform a release and gate nothing.

use std::{num::NonZeroUsize, thread};

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use fairlane::Single;

const ROUNDS: u64 = 10_000;

fn fairlane_ping_pong() {
  let (ping_tx, ping_rx) = fairlane::bounded::<u64>(NonZeroUsize::MIN).unwrap();
  let (pong_tx, pong_rx) = fairlane::bounded::<u64>(NonZeroUsize::MIN).unwrap();
  let echo = thread::spawn(move || {
    while let Ok(received) = ping_rx.recv_blocking() {
      pong_tx.send_blocking(Single, received.into_item()).unwrap();
    }
  });
  for round in 0..ROUNDS {
    ping_tx.send_blocking(Single, round).unwrap();
    pong_rx.recv_blocking().unwrap();
  }
  drop(ping_tx);
  echo.join().unwrap();
}

fn async_channel_ping_pong() {
  let (ping_tx, ping_rx) = async_channel::bounded::<u64>(1);
  let (pong_tx, pong_rx) = async_channel::bounded::<u64>(1);
  let echo = thread::spawn(move || {
    while let Ok(item) = ping_rx.recv_blocking() {
      pong_tx.send_blocking(item).unwrap();
    }
  });
  for round in 0..ROUNDS {
    ping_tx.send_blocking(round).unwrap();
    pong_rx.recv_blocking().unwrap();
  }
  drop(ping_tx);
  echo.join().unwrap();
}

fn crossbeam_ping_pong() {
  let (ping_tx, ping_rx) = crossbeam_channel::bounded::<u64>(1);
  let (pong_tx, pong_rx) = crossbeam_channel::bounded::<u64>(1);
  let echo = thread::spawn(move || {
    while let Ok(item) = ping_rx.recv() {
      pong_tx.send(item).unwrap();
    }
  });
  for round in 0..ROUNDS {
    ping_tx.send(round).unwrap();
    pong_rx.recv().unwrap();
  }
  drop(ping_tx);
  echo.join().unwrap();
}

fn kanal_ping_pong() {
  let (ping_tx, ping_rx) = kanal::bounded::<u64>(1);
  let (pong_tx, pong_rx) = kanal::bounded::<u64>(1);
  let echo = thread::spawn(move || {
    while let Ok(item) = ping_rx.recv() {
      pong_tx.send(item).unwrap();
    }
  });
  for round in 0..ROUNDS {
    ping_tx.send(round).unwrap();
    pong_rx.recv().unwrap();
  }
  drop(ping_tx);
  echo.join().unwrap();
}

fn flume_ping_pong() {
  let (ping_tx, ping_rx) = flume::bounded::<u64>(1);
  let (pong_tx, pong_rx) = flume::bounded::<u64>(1);
  let echo = thread::spawn(move || {
    while let Ok(item) = ping_rx.recv() {
      pong_tx.send(item).unwrap();
    }
  });
  for round in 0..ROUNDS {
    ping_tx.send(round).unwrap();
    pong_rx.recv().unwrap();
  }
  drop(ping_tx);
  echo.join().unwrap();
}

/// One item at a time back and forth, capacity 1.
fn s4(c: &mut Criterion) {
  let mut group = c.benchmark_group("s4_ping_pong");
  group.throughput(Throughput::Elements(ROUNDS));
  group.bench_function("fairlane", |b| b.iter(fairlane_ping_pong));
  group.bench_function("async-channel", |b| b.iter(async_channel_ping_pong));
  group.bench_function("crossbeam-channel", |b| b.iter(crossbeam_ping_pong));
  group.bench_function("kanal", |b| b.iter(kanal_ping_pong));
  group.bench_function("flume", |b| b.iter(flume_ping_pong));
  group.finish();
}

criterion_group!(benches, s4);
criterion_main!(benches);
