# cbroker

FIXME: one-paragraph description of what `cbroker` is and does.

## Architecture

`cbroker` is an OTP application. `cbroker_app` is the `application`
callback module (started via the `mod` entry in `cbroker.app.src`); it starts
`cbroker_sup`, the top-level `one_for_one` supervisor. Add child specs in
`cbroker_sup:child_specs/0`. Both modules are internal (`-moduledoc false`);
`cbroker` is the public API module.

## Build, test, check

```bash
make compile         # compile
make test            # eunit + CT (+ coverage)
make check           # check-fast + check-slow
make check-fast      # format check (erlfmt) + xref + dead-code (hank) + lint (elvis)
make check-slow      # dialyzer
make format          # auto-format sources with erlfmt
make doc             # build docs with ex_doc (downloads the ex_doc escript to tmp/)
make shell           # interactive REPL with the app started
make bench-shell     # same, plus the benchmarks in bench/
```

Benchmarks live in `bench/` (`cbroker_bench`, plus the `cbroker_simple` baseline it
compares against), compiled only by the `bench` profile together with their
bench-only deps (`xb5`). Run e.g. `cbroker_bench:bench1(cbroker, smallest, 1_000_000, 64)`
from `make bench-shell`; each run sets up and tears down what it measures.

All checks run sequentially (`.NOTPARALLEL`). CI runs `make check-fast`, `make test`,
and `make check-slow` over OTP 24–29 on Linux, plus a Windows job that only builds
and tests (`rebar3 compile` + `rebar3 do 'eunit,ct'`) over OTP 28–29.

## Compiler flags

`warn_export_vars`, `warn_missing_spec`, `warn_unused_import`, and `warnings_as_errors`
are always on — every exported function needs a `-spec`. The `test` and `shell`
profiles relax `warn_missing_spec` and `warnings_as_errors`.

## The NIF build

The NIF is built by `pre_hooks`/`post_hooks` in `rebar.config`, selected by a
regex over rebar3's `OTP_RELEASE-SYSTEM_ARCHITECTURE-WORDSIZE` string:

- **Unix** (`linux|darwin|solaris`, and `freebsd` through `gmake`): `c_src/Makefile`
  builds `priv/cbroker.so` with `cc`/`gcc`, globbing `c_src/*.c`. Warning flags are
  `-Wall -Wmissing-prototypes -Wsign-compare -Wconversion`; the last two were added
  because `-Wall` alone hides narrowing and signed/unsigned comparisons that MSVC
  reports at `/W3`.
- **Windows** (`windows`, matching e.g. `28-x86_64-pc-windows-64`, not `win32`):
  `c_src/Makefile.win` builds `priv/cbroker.dll` with nmake and MSVC, and must be
  run from a Visual Studio developer prompt. `.c` files are listed explicitly there
  because nmake cannot glob, so **new sources must be added to it by hand**.
  `/std:c11` is needed for `_Thread_local`, `/experimental:c11atomics` for
  `<stdatomic.h>` (VS 2022 17.5+), and `/MD` shares ERTS's C runtime, so that e.g.
  the `stderr` handed to `enif_fprintf` is the same one. The ERTS headers come from
  `ERLANG_ROOT_DIR`/`ERLANG_ERTS_VER`, which rebar3 exports to hooks.

MSVC portability rules for the C sources: no POSIX-only types (`ptrdiff_t`, not
`ssize_t`) and no compiler-specific atomics extensions — `memory_order_acq_rel` is
mapped to `memory_order_seq_cst` on MSVC in `cbroker_nif.c`.

The Windows CI job lists `priv/cbroker.dll` after compiling, because a hook whose
regex doesn't match fails silently and only surfaces later as a NIF load error.

## Documentation (EEP-48)

Docs are EEP-48 native, rendered by the standalone `ex_doc` escript (not the
`rebar3_ex_doc` plugin):

- `make doc` runs `rebar3 edoc` (top-level `edoc_opts` emit chunks into
  `_build/docs/lib/cbroker/ebin`) then the `ex_doc` escript over that ebin,
  configured by `ex_doc.config`. README is the main page.
- Doc attributes are guarded by `-ifdef(E48). … -endif.` so sources still compile
  on OTP < 27. Public: `-moduledoc "…"` / `-doc "…"`. **Hide internals with
  `-moduledoc false` / `-doc false`, NOT `%% @private`** — ex_doc ignores
  `@private` and would leak the exported functions of any `-moduledoc`'d module.

## Conventions

- Erlang is formatted with `erlfmt` and C with `clang-format` (config in
  `.clang-format`); run `make format` before committing. `make check-formatted`
  verifies both and is part of `check-fast`. Both are skipped with a warning
  when the tool is unavailable, so CI never hard-fails on a missing formatter.
  A bulk reformat should be its own commit, whose full SHA is appended to
  `.git-blame-ignore-revs`.
- Blocks whose alignment carries meaning (such as the `ATOM_LIST` X-macro table
  in `cbroker_nif.c`) are fenced with `/* clang-format off */` and
  `/* clang-format on */`. The marker comment must contain nothing else.
- Public API functions that aren't called internally carry `-ignore_xref([…])`
  to satisfy the `exports_not_used` xref check.
- Documented `elvis`/`hank` exceptions live inline in `elvis.config` /
  `rebar.config`.

## OTP-version gating

`rebar.config.script` drops dev-only plugins on old OTP releases:
`erlfmt` + `rebar3_hank` + `rebar3_lint` on OTP ≤ 25, and `erlfmt` alone on
OTP 26 (its `-doc` triple-quoted strings break erlfmt/katana there). Whenever the
gated plugin set changes, bump the `_build` cache prefix in `.github/workflows/ci.yml`.

## Releasing

`make publish` runs `rebar3 hex publish --doc-dir=doc`. Versioning is SemVer;
history is in `CHANGELOG.md` (Keep a Changelog format).
