#include "cbroker_omap.h"

#include <assert.h>
#include <string.h>

/* Allocation goes through ERTS by default, so the map shows up in the VM's
 * memory accounting like the rest of the NIF. The test harness overrides these
 * with -D so it can build this file without erl_nif.h. */
#if !defined(CBROKER_OMAP_ALLOC) || !defined(CBROKER_OMAP_FREE)
#include "erl_nif.h"
#define CBROKER_OMAP_ALLOC(size) enif_alloc((size))
#define CBROKER_OMAP_FREE(ptr) enif_free((ptr))
#endif

#define CBROKER_OMAP_INITIAL_CAPACITY 32

/* Entries live in keys[head..tail), ascending and duplicate-free; values[i]
 * belongs to keys[i]. Both arrays are views into one allocation owned by
 * `keys`, so `values` is never freed on its own. */
struct cbroker_omap {
    uint64_t* keys;
    void** values;
    size_t head;
    size_t tail;
    size_t capacity;
};

#define ENTRY_BYTES (sizeof(uint64_t) + sizeof(void*))

/*********************************************************************/

#ifdef CBROKER_OMAP_DEBUG_CHECKS
static void omap_assert_invariants(const cbroker_omap_t* map)
{
    assert(map->head <= map->tail);
    assert(map->tail <= map->capacity);
    assert(map->capacity == 0 || map->keys != NULL);
    for (size_t i = map->head + 1; i < map->tail; i++) {
        assert(map->keys[i - 1] < map->keys[i]);
    }
}
#else
#define omap_assert_invariants(map) ((void)(map))
#endif

/* Index of the first entry whose key is >= `key`, i.e. the point at which
 * `key` belongs. Returns tail when every key is smaller. */
static size_t omap_search(const cbroker_omap_t* map, uint64_t key, bool* found)
{
    size_t lo = map->head;
    size_t hi = map->tail;

    if (lo == hi) {
        *found = false;
        return hi;
    }

    /* Both hot cases -- the smallest key, and the newest (largest) one -- are
     * settled without entering the loop. */
    if (key <= map->keys[lo]) {
        *found = (key == map->keys[lo]);
        return lo;
    }
    if (key >= map->keys[hi - 1]) {
        *found = (key == map->keys[hi - 1]);
        return *found ? hi - 1 : hi;
    }

    /* Both endpoints are now known to differ from `key`, and to bracket it, so
     * the answer is strictly interior: keys[head] < key < keys[tail - 1]. */
    lo++;
    hi--;

    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (map->keys[mid] < key) {
            lo = mid + 1;
        }
        else {
            hi = mid;
        }
    }

    *found = (lo < map->tail && map->keys[lo] == key);
    return lo;
}

/* Re-anchors the live window at the start of the buffer, freeing up `head`
 * slots at the tail. */
static void omap_slide_to_front(cbroker_omap_t* map)
{
    size_t count = map->tail - map->head;

    if (map->head == 0) {
        return;
    }

    memmove(map->keys, &map->keys[map->head], count * sizeof(uint64_t));
    memmove(map->values, &map->values[map->head], count * sizeof(void*));
    map->head = 0;
    map->tail = count;
}

/* Moves the live entries into a fresh block of `new_capacity` entries,
 * anchored at the front. Leaves the map untouched on failure. */
static bool omap_reallocate(cbroker_omap_t* map, size_t new_capacity)
{
    size_t count = map->tail - map->head;

    assert(new_capacity >= count);

    if (new_capacity > SIZE_MAX / ENTRY_BYTES) {
        return false;
    }

    uint64_t* new_keys = CBROKER_OMAP_ALLOC(new_capacity * ENTRY_BYTES);
    if (new_keys == NULL) {
        return false;
    }
    void** new_values = (void**)((char*)new_keys + new_capacity * sizeof(uint64_t));

    if (count > 0) {
        memcpy(new_keys, &map->keys[map->head], count * sizeof(uint64_t));
        memcpy(new_values, &map->values[map->head], count * sizeof(void*));
    }
    if (map->keys != NULL) {
        CBROKER_OMAP_FREE(map->keys);
    }

    map->keys = new_keys;
    map->values = new_values;
    map->head = 0;
    map->tail = count;
    map->capacity = new_capacity;
    return true;
}

/* Guarantees one free slot at the tail (and so, since sliding only ever moves
 * entries towards the front, that tail < capacity). */
static bool omap_make_room(cbroker_omap_t* map)
{
    size_t count;
    size_t new_capacity;

    if (map->tail < map->capacity) {
        return true;
    }

    /* The window has drifted to the end of a buffer that is still mostly
     * empty -- the steady state when entries are appended at the tail and
     * removed from the head. Sliding it back is cheaper than growing, and the
     * "at most half full" test leaves capacity/2 free slots behind, so the
     * O(count) slide is amortised over that many appends. */
    count = map->tail - map->head;
    if (map->head > 0 && count <= map->capacity / 2) {
        omap_slide_to_front(map);
        return true;
    }

    new_capacity = (map->capacity == 0) ? CBROKER_OMAP_INITIAL_CAPACITY : map->capacity * 2;
    return omap_reallocate(map, new_capacity);
}

/* Opens up a free slot at `idx` by pushing [idx, tail) one place right. */
static void omap_shift_right(cbroker_omap_t* map, size_t idx)
{
    size_t count = map->tail - idx;

    assert(map->tail < map->capacity);
    memmove(&map->keys[idx + 1], &map->keys[idx], count * sizeof(uint64_t));
    memmove(&map->values[idx + 1], &map->values[idx], count * sizeof(void*));
    map->tail++;
}

/* Opens up a free slot at `idx - 1` by pushing [head, idx) one place left. */
static void omap_shift_left(cbroker_omap_t* map, size_t idx)
{
    size_t count = idx - map->head;

    assert(map->head > 0);
    memmove(&map->keys[map->head - 1], &map->keys[map->head], count * sizeof(uint64_t));
    memmove(&map->values[map->head - 1], &map->values[map->head], count * sizeof(void*));
    map->head--;
}

/*********************************************************************/

cbroker_omap_t* cbroker_omap_new(void)
{
    cbroker_omap_t* map = CBROKER_OMAP_ALLOC(sizeof(cbroker_omap_t));

    if (map == NULL) {
        return NULL;
    }

    map->keys = NULL;
    map->values = NULL;
    map->head = 0;
    map->tail = 0;
    map->capacity = 0;
    return map;
}

void cbroker_omap_destroy(cbroker_omap_t* map,
                          void (*free_value)(uint64_t key, void* value, void* ctx), void* ctx)
{
    if (map == NULL) {
        return;
    }

    if (free_value != NULL) {
        for (size_t i = map->head; i < map->tail; i++) {
            free_value(map->keys[i], map->values[i], ctx);
        }
    }
    if (map->keys != NULL) {
        CBROKER_OMAP_FREE(map->keys);
    }
    CBROKER_OMAP_FREE(map);
}

cbroker_omap_result_t cbroker_omap_insert(cbroker_omap_t* map, uint64_t key, void* value)
{
    size_t count = map->tail - map->head;
    size_t offset;
    size_t idx;
    size_t slot;

    if (count == 0 || key > map->keys[map->tail - 1]) {
        /* Fast path: the append that near-monotonic keys almost always want. */
        offset = count;
    }
    else {
        bool found;
        size_t pos = omap_search(map, key, &found);
        if (found) {
            return CBROKER_OMAP_DUPLICATE;
        }
        /* Held relative to head, because making room may slide the window. */
        offset = pos - map->head;
    }

    if (!omap_make_room(map)) {
        return CBROKER_OMAP_NOMEM;
    }

    idx = map->head + offset;
    if (idx == map->tail) {
        slot = map->tail++;
    }
    else if (map->head > 0 && (idx - map->head) < (map->tail - idx)) {
        omap_shift_left(map, idx);
        slot = idx - 1;
    }
    else {
        omap_shift_right(map, idx);
        slot = idx;
    }

    map->keys[slot] = key;
    map->values[slot] = value;

    omap_assert_invariants(map);
    return CBROKER_OMAP_OK;
}

bool cbroker_omap_lookup(const cbroker_omap_t* map, uint64_t key, void** value_out)
{
    bool found;
    size_t idx = omap_search(map, key, &found);

    if (!found) {
        return false;
    }
    if (value_out != NULL) {
        *value_out = map->values[idx];
    }
    return true;
}

bool cbroker_omap_delete_and_next(cbroker_omap_t* map, uint64_t key, bool* has_next,
                                  uint64_t* next_key_out, void** next_value_out)
{
    bool found;
    size_t idx = omap_search(map, key, &found);
    bool next_exists;

    if (!found) {
        return false;
    }

    /* Read the successor out before removing, because closing the gap moves
     * entries around it either way. */
    next_exists = (idx + 1 < map->tail);
    if (next_exists) {
        if (next_key_out != NULL) {
            *next_key_out = map->keys[idx + 1];
        }
        if (next_value_out != NULL) {
            *next_value_out = map->values[idx + 1];
        }
    }
    if (has_next != NULL) {
        *has_next = next_exists;
    }

    if (idx == map->head) {
        /* The common case: dropping the smallest key. */
        map->head++;
    }
    else if (idx + 1 == map->tail) {
        map->tail--;
    }
    else if ((idx - map->head) < (map->tail - 1 - idx)) {
        size_t count = idx - map->head;
        memmove(&map->keys[map->head + 1], &map->keys[map->head], count * sizeof(uint64_t));
        memmove(&map->values[map->head + 1], &map->values[map->head], count * sizeof(void*));
        map->head++;
    }
    else {
        size_t count = map->tail - idx - 1;
        memmove(&map->keys[idx], &map->keys[idx + 1], count * sizeof(uint64_t));
        memmove(&map->values[idx], &map->values[idx + 1], count * sizeof(void*));
        map->tail--;
    }

    if (map->head == map->tail) {
        /* Draining to empty is the natural moment to re-anchor the window,
         * and it keeps the steady state from ever needing to slide. */
        map->head = 0;
        map->tail = 0;
    }

    omap_assert_invariants(map);
    return true;
}

bool cbroker_omap_take(cbroker_omap_t* map, uint64_t key, void** value_out)
{
    bool found;
    size_t idx = omap_search(map, key, &found);

    if (!found) {
        return false;
    }
    else if (value_out != NULL) {
        *value_out = map->values[idx];
    }

    //

    if (idx == map->head) {
        /* The common case: dropping the smallest key. */
        map->head++;
    }
    else if (idx + 1 == map->tail) {
        map->tail--;
    }
    else if ((idx - map->head) < (map->tail - 1 - idx)) {
        size_t count = idx - map->head;
        memmove(&map->keys[map->head + 1], &map->keys[map->head], count * sizeof(uint64_t));
        memmove(&map->values[map->head + 1], &map->values[map->head], count * sizeof(void*));
        map->head++;
    }
    else {
        size_t count = map->tail - idx - 1;
        memmove(&map->keys[idx], &map->keys[idx + 1], count * sizeof(uint64_t));
        memmove(&map->values[idx], &map->values[idx + 1], count * sizeof(void*));
        map->tail--;
    }

    if (map->head == map->tail) {
        /* Draining to empty is the natural moment to re-anchor the window,
         * and it keeps the steady state from ever needing to slide. */
        map->head = 0;
        map->tail = 0;
    }

    omap_assert_invariants(map);
    return true;
}

bool cbroker_omap_take_first(cbroker_omap_t* map, uint64_t* key_out, void** value_out)
{
    if (map->head == map->tail) {
        return false;
    }

    *key_out = map->keys[map->head];
    *value_out = map->values[map->head];
    map->head++;

    if (map->head == map->tail) {
        map->head = 0;
        map->tail = 0;
    }

    omap_assert_invariants(map);
    return true;
}

size_t cbroker_omap_size(const cbroker_omap_t* map) { return map->tail - map->head; }

bool cbroker_omap_first(const cbroker_omap_t* map, uint64_t* key_out, void** value_out)
{
    if (map->head == map->tail) {
        return false;
    }
    if (key_out != NULL) {
        *key_out = map->keys[map->head];
    }
    if (value_out != NULL) {
        *value_out = map->values[map->head];
    }
    return true;
}

bool cbroker_omap_last(const cbroker_omap_t* map, uint64_t* key_out, void** value_out)
{
    if (map->head == map->tail) {
        return false;
    }
    if (key_out != NULL) {
        *key_out = map->keys[map->tail - 1];
    }
    if (value_out != NULL) {
        *value_out = map->values[map->tail - 1];
    }
    return true;
}

bool cbroker_omap_next(const cbroker_omap_t* map, uint64_t key, uint64_t* key_out, void** value_out)
{
    bool found;
    size_t idx;

    /* The common call is "successor of the largest key", which get_next_batch
     * makes on every batch advance and which has no answer. One compare
     * settles it, and an empty map, without a search. */
    if (map->head == map->tail || key >= map->keys[map->tail - 1]) {
        return false;
    }

    idx = omap_search(map, key, &found);

    /* omap_search lands on the first key >= `key`, which is already the
     * successor unless it is `key` itself. */
    if (found) {
        idx++;
    }
    if (idx >= map->tail) {
        return false;
    }

    if (key_out != NULL) {
        *key_out = map->keys[idx];
    }
    if (value_out != NULL) {
        *value_out = map->values[idx];
    }
    return true;
}

void** cbroker_omap_values(const cbroker_omap_t* map) { return &map->values[map->head]; }
