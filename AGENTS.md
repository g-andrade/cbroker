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
```

All checks run sequentially (`.NOTPARALLEL`). CI runs `make check-fast`, `make test`,
and `make check-slow` over OTP 24–29 on Linux.

## Compiler flags

`warn_export_vars`, `warn_missing_spec`, `warn_unused_import`, and `warnings_as_errors`
are always on — every exported function needs a `-spec`. The `test` and `shell`
profiles relax `warn_missing_spec` and `warnings_as_errors`.

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

- Code is formatted with `erlfmt`; run `make format` before committing. A bulk
  reformat should be its own commit, whose full SHA is appended to
  `.git-blame-ignore-revs`.
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
