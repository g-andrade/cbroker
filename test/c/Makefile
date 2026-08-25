# Standalone build of the cbroker_omap tests. Deliberately outside c_src/,
# whose Makefile globs *.c recursively and would otherwise link this (main and
# all) into the NIF shared object.
#
# cbroker_omap.c is compiled here without erl_nif.h: CBROKER_OMAP_ALLOC/FREE are
# pointed at the harness's instrumented allocator, which counts allocations and
# can be made to fail on demand.

CURDIR := $(shell pwd)
C_SRC_DIR := $(abspath $(CURDIR)/../../c_src)

CC ?= gcc
CFLAGS ?= -O1 -g -std=c11 -Wall -Wextra -Wmissing-prototypes -Wshadow \
          -Wconversion -Wsign-conversion
CFLAGS += -I $(C_SRC_DIR) -I $(CURDIR)
CFLAGS += -DCBROKER_OMAP_DEBUG_CHECKS

SANITIZERS ?= -fsanitize=address,undefined -fno-omit-frame-pointer

OMAP_CFLAGS = -include omap_test_alloc.h \
              -DCBROKER_OMAP_ALLOC=omap_test_alloc \
              -DCBROKER_OMAP_FREE=omap_test_free

SEED ?= 0x9E3779B97F4A7C15
ITERATIONS ?= 200000

all: cbroker_omap_test
.PHONY: all

cbroker_omap_test: cbroker_omap_test.c $(C_SRC_DIR)/cbroker_omap.c $(C_SRC_DIR)/cbroker_omap.h omap_test_alloc.h
	$(CC) $(CFLAGS) $(SANITIZERS) $(OMAP_CFLAGS) \
		cbroker_omap_test.c $(C_SRC_DIR)/cbroker_omap.c -o $@

test: cbroker_omap_test
	./cbroker_omap_test $(SEED) $(ITERATIONS)
.PHONY: test

# A different pseudo-random seed on every run, for the occasional wider sweep.
test-fuzz: cbroker_omap_test
	@for i in $$(seq 1 20); do \
		seed=$$(( (RANDOM << 16) | RANDOM | 1 )); \
		echo "seed $$seed"; \
		./cbroker_omap_test $$seed 50000 || exit 1; \
	done
.PHONY: test-fuzz

clean:
	@rm -f cbroker_omap_test
	@rm -rf cbroker_omap_test.dSYM
.PHONY: clean
