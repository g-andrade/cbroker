/* Standalone test harness for cbroker_omap.
 *
 *     make -C test/c && ./test/c/cbroker_omap_test [seed] [iterations]
 *
 * Two halves: hand-written cases for the edges, then a randomised run that
 * compares the map against a naive sorted-array model after every operation.
 */

#include "cbroker_omap.h"
#include "omap_test_alloc.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*********************************************************************/
/* Harness                                                           */
/*********************************************************************/

static unsigned long checks_run = 0;
static unsigned long failures = 0;
static const char* current_case = "";

#define CHECK(cond)                                                                  \
    do {                                                                             \
        checks_run++;                                                                \
        if (!(cond)) {                                                               \
            failures++;                                                              \
            fprintf(stderr, "FAIL %s:%d [%s]: %s\n", __FILE__, __LINE__,             \
                    current_case, #cond);                                            \
            if (failures > 20) {                                                      \
                fprintf(stderr, "too many failures, giving up\n");                   \
                exit(1);                                                             \
            }                                                                        \
        }                                                                            \
    } while (0)

#define CASE(name)                                                                   \
    do {                                                                             \
        current_case = (name);                                                       \
    } while (0)

/*********************************************************************/
/* Instrumented allocator                                            */
/*********************************************************************/

static long alloc_budget = -1; /* < 0: unlimited */
static size_t live_allocations = 0;
static unsigned long total_allocations = 0;

void* omap_test_alloc(size_t size) {
    void* ptr;

    if (alloc_budget == 0) {
        return NULL;
    }
    if (alloc_budget > 0) {
        alloc_budget--;
    }

    ptr = malloc(size);
    if (ptr != NULL) {
        live_allocations++;
        total_allocations++;
    }
    return ptr;
}

void omap_test_free(void* ptr) {
    if (ptr != NULL) {
        live_allocations--;
    }
    free(ptr);
}

/*********************************************************************/
/* Model: the same map, implemented the obvious slow way             */
/*********************************************************************/

#define MODEL_CAPACITY 8192

static uint64_t model_keys[MODEL_CAPACITY];
static void* model_values[MODEL_CAPACITY];
static size_t model_count = 0;

static void model_reset(void) {
    model_count = 0;
}

static bool model_find(uint64_t key, size_t* idx_out) {
    size_t i;

    for (i = 0; i < model_count && model_keys[i] < key; i++) {
    }
    *idx_out = i;
    return (i < model_count && model_keys[i] == key);
}

static bool model_insert(uint64_t key, void* value) {
    size_t idx;

    if (model_find(key, &idx)) {
        return false;
    }
    if (model_count == MODEL_CAPACITY) {
        fprintf(stderr, "model overflow; raise MODEL_CAPACITY\n");
        exit(1);
    }

    memmove(&model_keys[idx + 1], &model_keys[idx],
            (model_count - idx) * sizeof(model_keys[0]));
    memmove(&model_values[idx + 1], &model_values[idx],
            (model_count - idx) * sizeof(model_values[0]));
    model_keys[idx] = key;
    model_values[idx] = value;
    model_count++;
    return true;
}

static bool model_delete_and_next(uint64_t key,
                                  bool* has_next,
                                  uint64_t* next_key,
                                  void** next_value) {
    size_t idx;

    if (!model_find(key, &idx)) {
        return false;
    }

    *has_next = (idx + 1 < model_count);
    if (*has_next) {
        *next_key = model_keys[idx + 1];
        *next_value = model_values[idx + 1];
    }

    memmove(&model_keys[idx], &model_keys[idx + 1],
            (model_count - idx - 1) * sizeof(model_keys[0]));
    memmove(&model_values[idx], &model_values[idx + 1],
            (model_count - idx - 1) * sizeof(model_values[0]));
    model_count--;
    return true;
}

/* Walks the map with first()/next() and compares it to the model entry by
 * entry -- so this also exercises the iteration pair. */
static void check_matches_model(const cbroker_omap_t* map) {
    uint64_t key;
    void* value;
    size_t i = 0;
    bool more;

    CHECK(cbroker_omap_size(map) == model_count);

    if (model_count == 0) {
        CHECK(!cbroker_omap_last(map, &key, &value));
    } else {
        CHECK(cbroker_omap_last(map, &key, &value));
        CHECK(key == model_keys[model_count - 1]);
        CHECK(value == model_values[model_count - 1]);
    }

    more = cbroker_omap_first(map, &key, &value);
    while (more) {
        if (i >= model_count) {
            CHECK(i < model_count); /* map has entries the model does not */
            return;
        }
        CHECK(key == model_keys[i]);
        CHECK(value == model_values[i]);
        i++;
        more = cbroker_omap_next(map, key, &key, &value);
    }
    CHECK(i == model_count);
}

/*********************************************************************/
/* Edge cases                                                        */
/*********************************************************************/

static uint64_t value_seen_keys[MODEL_CAPACITY];
static size_t value_seen_count = 0;

static void record_freed_value(uint64_t key, void* value, void* ctx) {
    CHECK(ctx == (void*)0xC0FFEE);
    CHECK(value == (void*)(uintptr_t)(key + 1));
    value_seen_keys[value_seen_count++] = key;
}

static void test_empty(void) {
    cbroker_omap_t* map;
    uint64_t key = 12345;
    void* value = (void*)0xDEAD;
    bool has_next = true;

    CASE("empty map");
    map = cbroker_omap_new();
    CHECK(map != NULL);

    CHECK(cbroker_omap_size(map) == 0);
    CHECK(!cbroker_omap_lookup(map, 0, &value));
    CHECK(!cbroker_omap_lookup(map, UINT64_MAX, &value));
    CHECK(!cbroker_omap_first(map, &key, &value));
    CHECK(!cbroker_omap_last(map, &key, &value));
    CHECK(!cbroker_omap_next(map, 0, &key, &value));
    CHECK(!cbroker_omap_delete_and_next(map, 7, &has_next, &key, &value));

    /* A failed call writes nothing. */
    CHECK(key == 12345);
    CHECK(value == (void*)0xDEAD);
    CHECK(has_next == true);

    cbroker_omap_destroy(map, NULL, NULL);
    cbroker_omap_destroy(NULL, NULL, NULL); /* tolerated */
}

static void test_single_entry(void) {
    cbroker_omap_t* map = cbroker_omap_new();
    uint64_t key = 0;
    void* value = NULL;
    bool has_next = true;

    CASE("single entry");
    CHECK(cbroker_omap_insert(map, 42, (void*)0xAB) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_size(map) == 1);
    CHECK(cbroker_omap_lookup(map, 42, &value));
    CHECK(value == (void*)0xAB);
    CHECK(!cbroker_omap_lookup(map, 41, &value));
    CHECK(!cbroker_omap_lookup(map, 43, &value));
    CHECK(cbroker_omap_first(map, &key, &value) && key == 42 && value == (void*)0xAB);
    CHECK(cbroker_omap_last(map, &key, &value) && key == 42 && value == (void*)0xAB);
    CHECK(!cbroker_omap_next(map, 42, &key, &value));

    CHECK(cbroker_omap_delete_and_next(map, 42, &has_next, &key, &value));
    CHECK(!has_next);
    CHECK(cbroker_omap_size(map) == 0);

    /* Reusable after draining. */
    CHECK(cbroker_omap_insert(map, 42, (void*)0xCD) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_lookup(map, 42, &value) && value == (void*)0xCD);
    cbroker_omap_destroy(map, NULL, NULL);
}

static void test_duplicate_and_null_value(void) {
    cbroker_omap_t* map = cbroker_omap_new();
    void* value = (void*)0xFF;

    CASE("duplicate insert / NULL value");
    CHECK(cbroker_omap_insert(map, 1, (void*)0x11) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_insert(map, 1, (void*)0x22) == CBROKER_OMAP_DUPLICATE);
    CHECK(cbroker_omap_size(map) == 1);
    CHECK(cbroker_omap_lookup(map, 1, &value) && value == (void*)0x11);

    /* A stored NULL is a value, not an absence. */
    CHECK(cbroker_omap_insert(map, 2, NULL) == CBROKER_OMAP_OK);
    value = (void*)0xFF;
    CHECK(cbroker_omap_lookup(map, 2, &value));
    CHECK(value == NULL);
    CHECK(cbroker_omap_insert(map, 2, (void*)0x33) == CBROKER_OMAP_DUPLICATE);

    cbroker_omap_destroy(map, NULL, NULL);
}

static void test_delete_and_next_positions(void) {
    cbroker_omap_t* map = cbroker_omap_new();
    uint64_t key = 0;
    void* value = NULL;
    bool has_next = false;

    CASE("delete_and_next at each position");
    CHECK(cbroker_omap_insert(map, 10, (void*)0x10) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_insert(map, 20, (void*)0x20) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_insert(map, 30, (void*)0x30) == CBROKER_OMAP_OK);

    /* Middle. */
    CHECK(cbroker_omap_delete_and_next(map, 20, &has_next, &key, &value));
    CHECK(has_next && key == 30 && value == (void*)0x30);
    CHECK(cbroker_omap_size(map) == 2);
    CHECK(!cbroker_omap_lookup(map, 20, &value));

    /* Smallest. */
    CHECK(cbroker_omap_delete_and_next(map, 10, &has_next, &key, &value));
    CHECK(has_next && key == 30 && value == (void*)0x30);

    /* Largest / last one standing. */
    has_next = true;
    CHECK(cbroker_omap_delete_and_next(map, 30, &has_next, &key, &value));
    CHECK(!has_next);
    CHECK(cbroker_omap_size(map) == 0);

    /* Absent key: no removal, no writes. */
    CASE("delete_and_next on absent key");
    CHECK(cbroker_omap_insert(map, 5, (void*)0x5) == CBROKER_OMAP_OK);
    key = 999;
    value = (void*)0x999;
    has_next = true;
    CHECK(!cbroker_omap_delete_and_next(map, 4, &has_next, &key, &value));
    CHECK(!cbroker_omap_delete_and_next(map, 6, &has_next, &key, &value));
    CHECK(key == 999 && value == (void*)0x999 && has_next == true);
    CHECK(cbroker_omap_size(map) == 1);

    /* Every out param is optional. */
    CHECK(cbroker_omap_delete_and_next(map, 5, NULL, NULL, NULL));
    CHECK(cbroker_omap_size(map) == 0);

    cbroker_omap_destroy(map, NULL, NULL);
}

static void test_extremes(void) {
    cbroker_omap_t* map = cbroker_omap_new();
    uint64_t key = 0;
    void* value = NULL;
    bool has_next = true;

    CASE("key extremes");
    CHECK(cbroker_omap_insert(map, 0, (void*)0x1) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_insert(map, UINT64_MAX, (void*)0x2) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_lookup(map, 0, &value) && value == (void*)0x1);
    CHECK(cbroker_omap_lookup(map, UINT64_MAX, &value) && value == (void*)0x2);
    CHECK(cbroker_omap_next(map, 0, &key, &value) && key == UINT64_MAX);
    CHECK(!cbroker_omap_next(map, UINT64_MAX, &key, &value));
    CHECK(cbroker_omap_delete_and_next(map, 0, &has_next, &key, &value));
    CHECK(has_next && key == UINT64_MAX);
    cbroker_omap_destroy(map, NULL, NULL);
}

/* omap_search short-circuits at both endpoints and cbroker_omap_next
 * short-circuits at the largest key. Those branches carry the lower-bound
 * contract that insert() depends on, so pin them down explicitly rather than
 * leaving them to the randomised run. */
static void test_endpoint_fast_paths(void) {
    cbroker_omap_t* map = cbroker_omap_new();
    uint64_t key = 0;
    void* value = NULL;

    CASE("endpoint fast paths");

    /* Empty: every path must bail without touching keys[]. */
    CHECK(!cbroker_omap_lookup(map, 10, &value));
    CHECK(!cbroker_omap_next(map, 10, &key, &value));

    /* Single entry -- head and tail are the same slot. */
    CHECK(cbroker_omap_insert(map, 20, (void*)0x20) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_lookup(map, 20, &value) && value == (void*)0x20);
    CHECK(!cbroker_omap_lookup(map, 19, &value));
    CHECK(!cbroker_omap_lookup(map, 21, &value));
    CHECK(cbroker_omap_next(map, 19, &key, &value) && key == 20);
    CHECK(!cbroker_omap_next(map, 20, &key, &value));
    CHECK(!cbroker_omap_next(map, 21, &key, &value));

    /* Two entries: the interior narrowing is empty. */
    CHECK(cbroker_omap_insert(map, 40, (void*)0x40) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_lookup(map, 20, &value) && value == (void*)0x20);
    CHECK(cbroker_omap_lookup(map, 40, &value) && value == (void*)0x40);
    CHECK(!cbroker_omap_lookup(map, 30, &value));
    CHECK(cbroker_omap_next(map, 20, &key, &value) && key == 40);
    CHECK(cbroker_omap_next(map, 30, &key, &value) && key == 40);
    CHECK(!cbroker_omap_next(map, 40, &key, &value));

    /* Three or more: endpoints short-circuit, the middle goes through the
     * loop, and an interior miss still lands on the lower bound (which is
     * what insert() relies on to place a new key). */
    CHECK(cbroker_omap_insert(map, 30, (void*)0x30) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_insert(map, 60, (void*)0x60) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_lookup(map, 30, &value) && value == (void*)0x30);
    CHECK(!cbroker_omap_lookup(map, 35, &value));
    CHECK(cbroker_omap_next(map, 35, &key, &value) && key == 40);
    CHECK(cbroker_omap_next(map, 40, &key, &value) && key == 60);
    CHECK(!cbroker_omap_next(map, 60, &key, &value));
    CHECK(!cbroker_omap_next(map, UINT64_MAX, &key, &value));

    /* Insert into each of the three regions the fast paths partition. */
    CHECK(cbroker_omap_insert(map, 10, (void*)0x10) == CBROKER_OMAP_OK); /* below head */
    CHECK(cbroker_omap_insert(map, 50, (void*)0x50) == CBROKER_OMAP_OK); /* interior */
    CHECK(cbroker_omap_insert(map, 70, (void*)0x70) == CBROKER_OMAP_OK); /* above tail */
    CHECK(cbroker_omap_size(map) == 7);
    {
        const uint64_t expected_keys[] = {10, 20, 30, 40, 50, 60, 70};
        void* const expected_values[] = {(void*)0x10, (void*)0x20, (void*)0x30,
                                         (void*)0x40, (void*)0x50, (void*)0x60,
                                         (void*)0x70};
        size_t i = 0;
        bool more = cbroker_omap_first(map, &key, &value);
        while (more) {
            CHECK(i < 7 && key == expected_keys[i]);
            CHECK(value == expected_values[i]);
            i++;
            more = cbroker_omap_next(map, key, &key, &value);
        }
        CHECK(i == 7);
    }

    /* Same checks with head > 0, so the endpoint probes are exercised against
     * a window that does not start at slot 0. */
    CHECK(cbroker_omap_delete_and_next(map, 10, NULL, NULL, NULL));
    CHECK(cbroker_omap_delete_and_next(map, 20, NULL, NULL, NULL));
    CHECK(cbroker_omap_lookup(map, 30, &value) && value == (void*)0x30);
    CHECK(!cbroker_omap_lookup(map, 20, &value));
    CHECK(!cbroker_omap_lookup(map, 10, &value));
    CHECK(cbroker_omap_next(map, 10, &key, &value) && key == 30);
    CHECK(!cbroker_omap_next(map, 70, &key, &value));

    cbroker_omap_destroy(map, NULL, NULL);
}

static void test_growth_and_drain(void) {
    cbroker_omap_t* map = cbroker_omap_new();
    const uint64_t n = 5000;
    uint64_t i;
    uint64_t key;
    void* value;
    bool has_next;

    CASE("growth and drain");
    for (i = 0; i < n; i++) {
        CHECK(cbroker_omap_insert(map, i * 3, (void*)(uintptr_t)(i + 1)) == CBROKER_OMAP_OK);
    }
    CHECK(cbroker_omap_size(map) == n);

    for (i = 0; i < n; i++) {
        CHECK(cbroker_omap_lookup(map, i * 3, &value));
        CHECK(value == (void*)(uintptr_t)(i + 1));
        CHECK(!cbroker_omap_lookup(map, i * 3 + 1, &value));
    }

    /* Drain from the head, checking the successor each time. */
    for (i = 0; i < n; i++) {
        CHECK(cbroker_omap_delete_and_next(map, i * 3, &has_next, &key, &value));
        CHECK(has_next == (i + 1 < n));
        if (has_next) {
            CHECK(key == (i + 1) * 3);
            CHECK(value == (void*)(uintptr_t)(i + 2));
        }
    }
    CHECK(cbroker_omap_size(map) == 0);

    /* Descending inserts: the fast path never fires, every one shifts. */
    CASE("descending inserts");
    for (i = n; i > 0; i--) {
        CHECK(cbroker_omap_insert(map, i, (void*)(uintptr_t)i) == CBROKER_OMAP_OK);
    }
    CHECK(cbroker_omap_size(map) == n);
    CHECK(cbroker_omap_first(map, &key, &value) && key == 1);
    for (i = 1; i < n; i++) {
        CHECK(cbroker_omap_next(map, i, &key, &value) && key == i + 1);
    }
    cbroker_omap_destroy(map, NULL, NULL);
}

/* The steady state: append at the tail, remove the head. The live window walks
 * off the end of the buffer over and over, so this is what the slide path is
 * for -- and it must not keep reallocating. */
static void test_sliding_window_is_stable(void) {
    cbroker_omap_t* map = cbroker_omap_new();
    const uint64_t cycles = 200000;
    unsigned long allocations_after_warmup;
    uint64_t i;
    void* value;

    CASE("sliding window does not grow without bound");
    for (i = 0; i < 64; i++) {
        CHECK(cbroker_omap_insert(map, i, (void*)(uintptr_t)(i + 1)) == CBROKER_OMAP_OK);
    }
    for (i = 0; i < 1000; i++) {
        CHECK(cbroker_omap_delete_and_next(map, i, NULL, NULL, NULL) ||
              cbroker_omap_insert(map, i + 64, NULL) != CBROKER_OMAP_OK);
        CHECK(cbroker_omap_insert(map, i + 64, (void*)(uintptr_t)(i + 65)) ==
              CBROKER_OMAP_OK);
    }
    allocations_after_warmup = total_allocations;

    for (; i < cycles; i++) {
        CHECK(cbroker_omap_delete_and_next(map, i, NULL, NULL, NULL));
        CHECK(cbroker_omap_insert(map, i + 64, (void*)(uintptr_t)(i + 65)) ==
              CBROKER_OMAP_OK);
        CHECK(cbroker_omap_size(map) == 64);
    }
    CHECK(total_allocations == allocations_after_warmup);
    CHECK(cbroker_omap_lookup(map, cycles + 63, &value));
    CHECK(value == (void*)(uintptr_t)(cycles + 64));

    cbroker_omap_destroy(map, NULL, NULL);
}

static void test_allocation_failure(void) {
    cbroker_omap_t* map;
    void* value = NULL;
    uint64_t i;

    CASE("allocation failure");

    /* new() fails cleanly. */
    alloc_budget = 0;
    CHECK(cbroker_omap_new() == NULL);
    alloc_budget = -1;

    /* The first insert has to allocate the block; failing it leaves an empty
     * but usable map. */
    map = cbroker_omap_new();
    CHECK(map != NULL);
    alloc_budget = 0;
    CHECK(cbroker_omap_insert(map, 1, (void*)0x1) == CBROKER_OMAP_NOMEM);
    alloc_budget = -1;
    CHECK(cbroker_omap_size(map) == 0);
    CHECK(cbroker_omap_insert(map, 1, (void*)0x1) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_lookup(map, 1, &value) && value == (void*)0x1);

    /* A failed grow leaves the existing contents intact. */
    for (i = 2; i <= 32; i++) {
        CHECK(cbroker_omap_insert(map, i, (void*)(uintptr_t)i) == CBROKER_OMAP_OK);
    }
    CHECK(cbroker_omap_size(map) == 32);
    alloc_budget = 0;
    CHECK(cbroker_omap_insert(map, 33, (void*)0x33) == CBROKER_OMAP_NOMEM);
    alloc_budget = -1;
    CHECK(cbroker_omap_size(map) == 32);
    for (i = 1; i <= 32; i++) {
        CHECK(cbroker_omap_lookup(map, i, &value) && value == (void*)(uintptr_t)i);
    }
    CHECK(cbroker_omap_insert(map, 33, (void*)0x33) == CBROKER_OMAP_OK);
    CHECK(cbroker_omap_size(map) == 33);

    cbroker_omap_destroy(map, NULL, NULL);
}

static void test_destroy_callback(void) {
    cbroker_omap_t* map = cbroker_omap_new();
    uint64_t i;

    CASE("destroy frees every value in order");
    for (i = 0; i < 100; i++) {
        uint64_t key = (i * 37) % 100; /* scattered insert order */
        CHECK(cbroker_omap_insert(map, key, (void*)(uintptr_t)(key + 1)) ==
              CBROKER_OMAP_OK);
    }
    /* Remove a few so the callback runs over a window that is not at offset 0. */
    CHECK(cbroker_omap_delete_and_next(map, 0, NULL, NULL, NULL));
    CHECK(cbroker_omap_delete_and_next(map, 1, NULL, NULL, NULL));
    CHECK(cbroker_omap_delete_and_next(map, 50, NULL, NULL, NULL));

    value_seen_count = 0;
    cbroker_omap_destroy(map, record_freed_value, (void*)0xC0FFEE);
    CHECK(value_seen_count == 97);
    for (i = 1; i < value_seen_count; i++) {
        CHECK(value_seen_keys[i - 1] < value_seen_keys[i]);
    }
}

/*********************************************************************/
/* Randomised comparison against the model                           */
/*********************************************************************/

static uint64_t rng_state;

static uint64_t rng_next(void) {
    rng_state ^= rng_state << 13;
    rng_state ^= rng_state >> 7;
    rng_state ^= rng_state << 17;
    return rng_state;
}

static uint64_t rng_below(uint64_t bound) {
    return rng_next() % bound;
}

static void* value_for(uint64_t key) {
    return (void*)(uintptr_t)(key * 2654435761u + 1);
}

static void test_random(uint64_t seed, unsigned long iterations) {
    cbroker_omap_t* map = cbroker_omap_new();
    uint64_t tail_key = 1000;
    unsigned long i;

    CASE("randomised vs model");
    rng_state = seed ? seed : 1;
    model_reset();

    for (i = 0; i < iterations; i++) {
        uint64_t roll = rng_below(100);
        uint64_t key;

        if (roll < 45 || model_count == 0) {
            /* Tail append: the dominant case, mostly consecutive keys. */
            tail_key += 1 + rng_below(3);
            key = tail_key;
        } else if (roll < 55) {
            /* Middle insert: land between two live keys, or on one of them to
             * exercise the duplicate path. */
            key = model_keys[rng_below(model_count)] + rng_below(2);
        } else if (roll < 70) {
            /* Lookup, half the time for a key that is present. */
            void* value = (void*)0xBADBAD;
            bool present;

            key = (rng_below(2) == 0) ? model_keys[rng_below(model_count)]
                                      : tail_key + 1 + rng_below(1000);
            present = cbroker_omap_lookup(map, key, &value);
            {
                size_t idx;
                bool expected = model_find(key, &idx);
                CHECK(present == expected);
                CHECK(!present || value == model_values[idx]);
                CHECK(present || value == (void*)0xBADBAD);
            }
            continue;
        } else if (roll < 90) {
            key = model_keys[0]; /* delete the smallest: the normal case */
        } else if (roll < 97) {
            key = model_keys[rng_below(model_count)]; /* delete a middle key */
        } else {
            key = tail_key + 1 + rng_below(1000); /* delete something absent */
        }

        if (roll < 55) {
            cbroker_omap_result_t result = cbroker_omap_insert(map, key, value_for(key));
            bool inserted = model_insert(key, value_for(key));
            CHECK(result == (inserted ? CBROKER_OMAP_OK : CBROKER_OMAP_DUPLICATE));
        } else {
            bool has_next = false;
            uint64_t next_key = 0;
            void* next_value = NULL;
            bool model_has_next = false;
            uint64_t model_next_key = 0;
            void* model_next_value = NULL;
            bool removed;
            bool model_removed;

            removed = cbroker_omap_delete_and_next(map, key, &has_next, &next_key,
                                                   &next_value);
            model_removed = model_delete_and_next(key, &model_has_next, &model_next_key,
                                                  &model_next_value);
            CHECK(removed == model_removed);
            if (removed) {
                CHECK(has_next == model_has_next);
                if (has_next) {
                    CHECK(next_key == model_next_key);
                    CHECK(next_value == model_next_value);
                }
            }
        }

        check_matches_model(map);

        /* Keep the working set near the described peak. */
        while (model_count > 4000) {
            CHECK(cbroker_omap_delete_and_next(map, model_keys[0], NULL, NULL, NULL));
            model_delete_and_next(model_keys[0], &(bool){false}, &(uint64_t){0},
                                  &(void*){NULL});
        }
    }

    cbroker_omap_destroy(map, NULL, NULL);
}

/*********************************************************************/

int main(int argc, char** argv) {
    uint64_t seed = (argc > 1) ? strtoull(argv[1], NULL, 0) : 0x9E3779B97F4A7C15ull;
    unsigned long iterations = (argc > 2) ? strtoul(argv[2], NULL, 0) : 200000;

    test_empty();
    test_single_entry();
    test_duplicate_and_null_value();
    test_delete_and_next_positions();
    test_extremes();
    test_endpoint_fast_paths();
    test_growth_and_drain();
    test_sliding_window_is_stable();
    test_allocation_failure();
    test_destroy_callback();
    test_random(seed, iterations);

    CASE("no leaks");
    CHECK(live_allocations == 0);

    printf("%lu checks, %lu failures (seed 0x%" PRIx64 ", %lu iterations)\n", checks_run,
           failures, seed, iterations);
    return failures == 0 ? 0 : 1;
}
