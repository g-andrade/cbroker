SHELL := bash
.ONESHELL:
.SHELLFLAGS := -euc
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

## General Rules

all: compile
.PHONY: all
.NOTPARALLEL: all

compile:
	@rebar3 compile
.PHONY: compile

clean:
	@rebar3 clean -a
.PHONY: clean

check: check-fast check-slow
.NOTPARALLEL: check
.PHONY: check

check-fast: check-formatted xref hank-dead-code-cleaner elvis-linter
.NOTPARALLEL: check-fast
.PHONY: check-fast

check-slow: dialyzer
.NOTPARALLEL: check-slow
.PHONY: check-slow

test: eunit ct proper
.NOTPARALLEL: test
.PHONY: test

C_SOURCES := $(wildcard c_src/*.c c_src/*.h test/c/*.c test/c/*.h)

format: format-erl format-c
.NOTPARALLEL: format
.PHONY: format

format-erl:
	@rebar3 fmt
.PHONY: format-erl

format-c:
	@if command -v clang-format >/dev/null 2>&1; then \
		clang-format -i $(C_SOURCES); \
	else \
		echo >&2 "WARN: skipping clang-format"; \
	fi
.PHONY: format-c

## Tests

ct:
	@rebar3 do ct
.PHONY: ct

eunit:
	@rebar3 eunit
.PHONY: eunit

# PROPER_NUMTESTS: raise it for a longer run, e.g. `make proper PROPER_NUMTESTS=1000`
PROPER_NUMTESTS ?= 100

proper:
	@rebar3 proper -n $(PROPER_NUMTESTS)
.PHONY: proper

cover: ct eunit proper
	@rebar3 cover
.PHONY: cover

# The stress cases also run as part of `ct`, but with small defaults; this runs
# them for much longer. Override either variable to size it differently.
STRESS_PROCS_PER_LANE ?= 32
STRESS_ITERATIONS ?= 20000

stress: export CBROKER_STRESS_PROCS_PER_LANE = $(STRESS_PROCS_PER_LANE)
stress: export CBROKER_STRESS_ITERATIONS = $(STRESS_ITERATIONS)
stress:
	@rebar3 ct --suite=test/cbroker_stress_SUITE
.PHONY: stress

# Runs the CT suites against an instrumented NIF: ASan+UBSan plus the allocation
# counters, which only exist in this build. The sanitizer runtime has to
# be preloaded into the emulator, since it must initialize before the library
# that needs it is dlopen'd. `+Mea min` routes erts_alloc through libc malloc:
# without it ASan cannot see `enif_alloc`ed memory, so overflowing such a block
# corrupts an ERTS allocator and surfaces as a bare SEGV somewhere else instead
# of an attributed report. Leak detection is off: even with `+Mea min` it would
# mostly report the emulator's own allocations, which can't be told apart from
# ours by suppression, since both reach malloc through erts_alloc. The NIF is
# rebuilt (and cleaned afterwards) because these flags are incompatible with the
# normal -O3 build.
SANITIZERS ?= address,undefined
SANITIZER_PRELOAD ?= $(shell $(or $(CC),cc) -print-file-name=libasan.so)
ASAN_OPTIONS ?= detect_leaks=0
UBSAN_OPTIONS ?= print_stacktrace=1

test-sanitized:
	@$(MAKE) -C c_src clean
	@$(MAKE) -C c_src SANITIZE=$(SANITIZERS) COUNT_ALLOCS=1
	@status=0; \
	SANITIZE=$(SANITIZERS) COUNT_ALLOCS=1 \
	ERL_FLAGS="+Mea min" \
	LD_PRELOAD=$(SANITIZER_PRELOAD) \
	ASAN_OPTIONS=$(ASAN_OPTIONS) \
	UBSAN_OPTIONS=$(UBSAN_OPTIONS) \
	rebar3 ct || status=$$?; \
	$(MAKE) -C c_src clean; \
	exit $$status
.NOTPARALLEL: test-sanitized
.PHONY: test-sanitized

## Checks

check-formatted: check-formatted-erl check-formatted-c
.NOTPARALLEL: check-formatted
.PHONY: check-formatted

check-formatted-erl:
	@if rebar3 plugins list | grep '^erlfmt\>' >/dev/null; then \
		rebar3 fmt --check; \
	else \
		echo >&2 "WARN: skipping rebar3 erlfmt check"; \
	fi
.PHONY: check-formatted-erl

check-formatted-c:
	@if command -v clang-format >/dev/null 2>&1; then \
		clang-format --dry-run --Werror $(C_SOURCES); \
	else \
		echo >&2 "WARN: skipping clang-format check"; \
	fi
.PHONY: check-formatted-c

xref:
	@rebar3 xref
.PHONY: xref

hank-dead-code-cleaner:
	@if rebar3 plugins list | grep '^rebar3_hank\>' >/dev/null; then \
		rebar3 hank; \
	else \
		echo >&2 "WARN: skipping rebar3_hank check"; \
	fi
.NOTPARALLEL: hank-dead-code-cleaner
.PHONY: hank-dead-code-cleaner

elvis-linter:
	@if rebar3 plugins list | grep '^rebar3_lint\>' >/dev/null; then \
		rebar3 lint; \
	else \
		echo >&2 "WARN: skipping rebar3_lint check"; \
	fi
.NOTPARALLEL: elvis-linter
.PHONY: elvis-linter

dialyzer:
	@rebar3 dialyzer
.PHONY: dialyzer

## Shell, docs and publication

publish: doc
publish:
	@rebar3 hex publish --doc-dir=doc
.NOTPARALLEL: publish
.PHONY: publish

shell: export ERL_FLAGS = +pc unicode
shell:
	@rebar3 as shell shell
.PHONY: shell

bench-shell: export ERL_FLAGS = +pc unicode
bench-shell:
	@rebar3 as shell,bench shell
.PHONY: bench-shell

doc: SOURCE_REF := $(shell git describe --tags --exact-match 2>/dev/null || git rev-parse --short HEAD)
doc: tmp/ex_doc
doc:
	rebar3 edoc; \
		./tmp/ex_doc "cbroker" "${SOURCE_REF}" \
		_build/docs/lib/cbroker/ebin \
		-c ex_doc.config \
		--source-ref "${SOURCE_REF}";
.PHONY: doc

# Rebuilds the docs whenever a source of them is saved; reload the browser to see it.
# Watches directories, not files: editors that save by renaming over the file would
# otherwise remove the watch. Runs one monitor (-m) rather than one inotifywait per
# save, so that events arriving while it would be restarting aren't lost.
doc-watch: doc
	@inotifywait -mqe close_write,moved_to --format %w%f . src | while read -r f; do \
		case "$$f" in \
		./*.md|./ex_doc.config|src/*.erl) $(MAKE) --no-print-directory doc;; \
		esac; \
	done
.PHONY: doc-watch

tmp/ex_doc: EX_DOC_VER=0.40.2
tmp/ex_doc: OTP_VER := $(shell erl -noshell -eval 'io:fwrite("~s", [erlang:system_info(otp_release)]), init:stop().')
tmp/ex_doc: | tmp
tmp/ex_doc:
	curl -fL -o tmp/ex_doc \
		"https://github.com/elixir-lang/ex_doc/releases/download/v${EX_DOC_VER}/ex_doc_otp_${OTP_VER}"; \
		chmod a+x tmp/ex_doc

tmp:
	mkdir tmp
