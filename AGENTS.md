# cbroker

`cbroker` provides brokers for Erlang/OTP: processes enqueue on either of two
lanes (`left` or `right`), meet, and swap offers, as in worker pools and other
producer-consumer setups. There is no broker process; matching is done
concurrently by a NIF, on whichever scheduler the asking process runs. See
`README.md` for the API and `INTERNALS.md` for the algorithm.

## Architecture

`cbroker` is a library application: it has no `application` callback module and
no supervision tree of its own.

- `cbroker` is the public API module. A broker is a NIF resource, returned by
  `new/0,1` as a reference.
- `cbroker_nif` holds the NIF stubs and loads `priv/cbroker`. The algorithm
  lives in `c_src/cbroker_nif.c`; `c_src/cbroker_omap.c` is the ordered map it
  keeps batches in.
- `cbroker_persistent` is the `gen_server` behind named brokers
  (`cbroker:child_spec/1,2`). It creates the broker, keeps it in
  `persistent_term`, and is the process users place in their own supervision
  tree.
- `cbroker_utils` holds small helpers.

## Build, test, check

```bash
make compile         # compile
make test            # eunit + CT (+ coverage) + PropEr
make proper          # PropEr only; `make proper PROPER_NUMTESTS=1000` for a longer run
make stress          # the stress cases alone, sized up (see below)
make test-sanitized  # CT suites against an ASan+UBSan build of the NIF
make check           # check-fast + check-slow
make check-fast      # format check + xref + dead-code (hank) + lint (elvis)
make check-slow      # dialyzer
make format          # auto-format sources (erlfmt, clang-format, prettier)
make doc             # build docs with ex_doc (downloads the ex_doc escript to tmp/)
make doc-watch       # rebuild docs on every save (needs inotifywait; Linux only)
make shell           # interactive REPL with the app started
make bench-shell     # same, plus the benchmarks in bench/
```

Benchmarks live in `bench/` (`cbroker_bench`, plus the `cbroker_simple` baseline
it compares against), compiled only by the `bench` profile together with their
bench-only deps (`xb5`). Run e.g.
`cbroker_bench:bench1(cbroker, smallest, 1_000_000, 64)` from
`make bench-shell`; each run sets up and tears down what it measures.

Tests come in three layers: `cbroker_tests_SUITE` (worked examples per API
family), `prop_cbroker` plus `prop_cbroker_statem` (stateless properties and a
single-process model), and `cbroker_stress_SUITE` (concurrency invariants —
matches pair up, the cancel-versus-match race never loses a reply, dead
processes' requests are reclaimed). The stress cases are sized by
`CBROKER_STRESS_PROCS_PER_LANE` and `CBROKER_STRESS_ITERATIONS`, small enough by
default to run inside `make test`; `make stress` (`STRESS_PROCS_PER_LANE`,
`STRESS_ITERATIONS`) runs them far longer. That suite is also the workload to
run against a sanitizer build of the NIF: it found both a live `assert` abort
and a lost-reply hang in the cancellation path.

`make test-sanitized` rebuilds the NIF with `SANITIZE=address,undefined` (see
`c_src/Makefile`) and runs the CT suites against it. Three things make that
work:

- The sanitizer runtime is `LD_PRELOAD`ed, because it must initialize before the
  instrumented library is `dlopen`ed.
- `SANITIZE` is exported so rebar3's compile hook relinks with the same flags.
  The library depends on the always-regenerated `compile_flags.txt`, so it _is_
  relinked, and without the flag it ends up missing the ASan symbols and fails
  to load, which shows up as `undef` on `cbroker_nif:new/1`.
- `ERL_FLAGS="+Mea min"` routes `erts_alloc` through libc `malloc`. Without it
  ASan cannot see `enif_alloc`ed memory: overflowing such a block corrupts an
  ERTS allocator and surfaces as a bare SEGV elsewhere (verified by injecting
  one), whereas with it the report names the offending line.

Leak detection stays off. Even with `+Mea min`, LSan would mostly report the
emulator's own allocations, and suppressions can't separate them from ours
because both reach `malloc` through `erts_alloc`. Instead, the same target
builds with `COUNT_ALLOCS=1` (`-DCBROKER_COUNT_ALLOCS`), which turns the
`cbroker_alloc*` wrappers into counting ones and makes
`cbroker_nif:alloc_perfcounters/0` report live blocks, envs, brokers, tickets
and retries; the `brokers_leave_nothing_allocated` case asserts they return to
baseline. Without that define the wrappers are macros for the plain ERTS calls,
`alloc_perfcounters/0` answers `unavailable`, and the case skips itself. Note
`cbroker_omap.c` allocates through `CBROKER_OMAP_ALLOC` directly, so its blocks
are not counted. The NIF is cleaned afterwards, as these flags don't mix with
the normal `-O3` build.

All checks run sequentially (`.NOTPARALLEL`). CI runs `make check-fast`,
`make test`, and `make check-slow` over OTP 24–29 on Linux, plus a Windows job
that only builds and tests (`rebar3 compile` + `rebar3 do 'eunit,ct'`) over OTP
28–29.

## Compiler flags

`warn_export_vars`, `warn_missing_spec`, `warn_unused_import`, and
`warnings_as_errors` are always on — every exported function needs a `-spec`.
The `test` and `shell` profiles relax `warn_missing_spec` and
`warnings_as_errors`.

## The NIF build

The NIF is built by `pre_hooks`/`post_hooks` in `rebar.config`, selected by a
regex over rebar3's `OTP_RELEASE-SYSTEM_ARCHITECTURE-WORDSIZE` string:

- **Unix** (`linux|darwin|solaris`, and `freebsd` through `gmake`):
  `c_src/Makefile` builds `priv/cbroker.so` with `cc`/`gcc`, globbing
  `c_src/*.c`. Warning flags are
  `-Wall -Wmissing-prototypes -Wsign-compare -Wconversion`; the last two were
  added because `-Wall` alone hides narrowing and signed/unsigned comparisons
  that MSVC reports at `/W3`.
- **Windows** (`windows`, matching e.g. `28-x86_64-pc-windows-64`, not `win32`):
  `c_src/Makefile.win` builds `priv/cbroker.dll` with nmake and MSVC, and must
  be run from a Visual Studio developer prompt. `.c` files are listed explicitly
  there because nmake cannot glob, so **new sources must be added to it by
  hand**. `/std:c11` is needed for `_Thread_local`, `/experimental:c11atomics`
  for `<stdatomic.h>` (VS 2022 17.5+), and `/MD` shares ERTS's C runtime, so
  that e.g. the `stderr` handed to `enif_fprintf` is the same one. The ERTS
  headers come from `ERLANG_ROOT_DIR`/`ERLANG_ERTS_VER`, which rebar3 exports to
  hooks.

MSVC portability rules for the C sources: no POSIX-only types (`ptrdiff_t`, not
`ssize_t`) and no compiler-specific atomics extensions — `memory_order_acq_rel`
is mapped to `memory_order_seq_cst` on MSVC in `cbroker_nif.c`.

The Windows CI job lists `priv/cbroker.dll` after compiling, because a hook
whose regex doesn't match fails silently and only surfaces later as a NIF load
error.

## Documentation (EEP-48)

Docs are EEP-48 native, rendered by the standalone `ex_doc` escript (not the
`rebar3_ex_doc` plugin):

- `make doc` runs `rebar3 edoc` (top-level `edoc_opts` emit chunks into
  `_build/docs/lib/cbroker/ebin`) then the `ex_doc` escript over that ebin,
  configured by `ex_doc.config`. README is the main page.
- Doc attributes are guarded by `-ifdef(E48). … -endif.` so sources still
  compile on OTP < 27. Public: `-moduledoc "…"` / `-doc "…"`. **Hide internals
  with `-moduledoc false` / `-doc false`, NOT `%% @private`** — ex_doc ignores
  `@private` and would leak the exported functions of any `-moduledoc`'d module.

## Conventions

- Erlang is formatted with `erlfmt`, C with `clang-format` (config in
  `.clang-format`) and Markdown with `prettier` (config in `.prettierrc`); run
  `make format` before committing. `make check-formatted` verifies all three and
  is part of `check-fast`. Each is skipped with a warning when the tool is
  unavailable, so CI never hard-fails on a missing formatter. A bulk reformat
  should be its own commit, whose full SHA is appended to
  `.git-blame-ignore-revs`.
- Blocks whose alignment carries meaning (such as the `ATOM_LIST` X-macro table
  in `cbroker_nif.c`) are fenced with `/* clang-format off */` and
  `/* clang-format on */`. The marker comment must contain nothing else.
- Public API functions that aren't called internally carry `-ignore_xref([…])`
  to satisfy the `exports_not_used` xref check.
- Documented `elvis`/`hank` exceptions live inline in `elvis.config` /
  `rebar.config`.
- Markdown docs (`README.md`, `INTERNALS.md`) explain concepts, not code: no
  function or field names, atomics or memory orders, call-site tables, or
  formulas. Lead with what a feature does and which options control it, then the
  mechanism in a sentence or two. Leave edge cases to the code and tests. A
  small feature gets one section of short paragraphs, with no subsections.

## OTP-version gating

`rebar.config.script` drops dev-only plugins on old OTP releases: `erlfmt` +
`rebar3_hank` + `rebar3_lint` on OTP ≤ 25, and `erlfmt` alone on OTP 26 (its
`-doc` triple-quoted strings break erlfmt/katana there). Whenever the gated
plugin set changes, bump the `_build` cache prefix in
`.github/workflows/ci.yml`.

## Releasing

`make publish` runs `rebar3 hex publish --doc-dir=doc`. Versioning is SemVer;
history is in `CHANGELOG.md` (Keep a Changelog format).
