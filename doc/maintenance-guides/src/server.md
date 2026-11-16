# Status-server profiling maintenance notes

The release-8.5 status server keeps pprof for CPU sampling and protobuf output.
SVG flamegraphs are rendered from pprof's public stack samples using inferno.

- Keep pprof's `flamegraph` feature disabled in both normal and development
  dependencies. Enabling it restores an outdated inferno/quick-xml chain.
- Preserve the existing pprof versions, including the 0.13 development dependency
  in `components/compact-log-backup`.
- Keep thread/frame ordering and sample counts when converting samples into
  inferno input, and keep XML escaping enabled for SVG output.
- Preserve the single-active-profile guard and release it when profiling ends or
  is cancelled. HTTP routing and protocol support are unchanged.

Read `src/server/status_server/profile.rs` for sampling, output conversion, and
regression tests covering SVG escaping, concurrency, and cancellation.
