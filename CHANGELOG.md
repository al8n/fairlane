# Changelog

All notable changes to this crate are documented here. The project follows semantic versioning;
before 1.0, a minor bump (0.x to 0.y) signals a breaking change.

New entries go under `## Unreleased`. A release renames that section to `## X.Y.Z (YYYY-MM-DD)` and
opens a new, empty `## Unreleased` above it. `ci/changelog.sh` checks this structure on every pull
request, and `release.yml` publishes the released section as the GitHub Release notes.

## Unreleased

- The first release: `fairlane`, a bounded MPMC channel of several lanes, and `fairlane-derive`,
  its `#[derive(Lane)]`.
  - A bound per lane, express lanes, and a policy against starvation: `Strict`, `PassLimit`,
    `Elapsed`, `FirstOf`, and `DefaultPolicy` (a count and a time, whichever comes first).
  - `recv_many` with a `Quota`: a limit, a limit per lane, and a range of lanes.
  - Every wait awaited, blocking, or not at all; permits, borrowed and owned; per-lane ordinals; a
    `Snapshot` of the queues.
