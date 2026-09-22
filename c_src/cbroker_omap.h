#ifndef CBROKER_OMAP_H
#define CBROKER_OMAP_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* An ordered map from uint64_t keys to void* values.
 *
 * Tuned for the broker's access pattern: keys arrive mostly in ascending
 * order, and entries are mostly removed from the low end, with occasional
 * removals from the middle. Entries are held in a single sorted array with a
 * sliding live window, so a tail insert and a delete-of-the-smallest are both
 * O(1), and a middle insert or delete costs one memmove of whichever side of
 * the array is shorter.
 *
 * There is no per-entry allocation: the whole map is two arrays carved out of
 * one block, 16 bytes per entry on a 64-bit target.
 *
 * NOT thread safe; callers serialise access themselves.
 *
 * Note that entries move as the map is mutated. Values (being pointers the map
 * only stores and hands back) stay put, but the storage holding a key/value
 * pair does not, so nothing may retain the address of a slot across a call to
 * cbroker_omap_insert() or cbroker_omap_delete_and_next().
 */
typedef struct cbroker_omap cbroker_omap_t;

typedef enum {
    CBROKER_OMAP_OK = 0,
    /* Key already present; the existing value was left untouched. */
    CBROKER_OMAP_DUPLICATE,
    /* Out of memory; the map is unchanged. */
    CBROKER_OMAP_NOMEM
} cbroker_omap_result_t;

/* Returns NULL on allocation failure. */
cbroker_omap_t* cbroker_omap_new(void);

/* Frees the map. If free_value is not NULL it is called once per remaining
 * entry, in ascending key order, with ctx passed through. Tolerates a NULL
 * map. */
void cbroker_omap_destroy(cbroker_omap_t* map,
                          void (*free_value)(uint64_t key, void* value, void* ctx), void* ctx);

cbroker_omap_result_t cbroker_omap_insert(cbroker_omap_t* map, uint64_t key, void* value);

/* Returns false if the key is absent, leaving *value_out untouched. The out
 * param (rather than a returned pointer) keeps a stored NULL value
 * distinguishable from an absent key. */
bool cbroker_omap_lookup(const cbroker_omap_t* map, uint64_t key, void** value_out);

/* Removes key and reports the entry that followed it.
 *
 * Returns false if the key was absent: nothing is removed and no out param is
 * written. Returns true if it was removed, and sets *has_next to whether a
 * larger key existed; *next_key_out and *next_value_out are written only when
 * *has_next is true. Every out param may be NULL. */
bool cbroker_omap_delete_and_next(cbroker_omap_t* map, uint64_t key, bool* has_next,
                                  uint64_t* next_key_out, void** next_value_out);

bool cbroker_omap_take(cbroker_omap_t* map, uint64_t key, void** value_out);

bool cbroker_omap_take_first(cbroker_omap_t* map, uint64_t* key_out, void** value_out);

size_t cbroker_omap_size(const cbroker_omap_t* map);

/* Smallest entry. False if the map is empty. */
bool cbroker_omap_first(const cbroker_omap_t* map, uint64_t* key_out, void** value_out);

/* Largest entry. False if the map is empty. */
bool cbroker_omap_last(const cbroker_omap_t* map, uint64_t* key_out, void** value_out);

/* Smallest entry whose key is strictly greater than key; key itself need not
 * be present. False if there is none. Together with cbroker_omap_first() this
 * walks the map in ascending order. */
bool cbroker_omap_next(const cbroker_omap_t* map, uint64_t key, uint64_t* key_out,
                       void** value_out);

size_t cbroker_omap_all_next(const cbroker_omap_t* map, uint64_t key, uint64_t** keys_out,
                             void*** values_out);

void** cbroker_omap_values(const cbroker_omap_t* map);

#endif /* CBROKER_OMAP_H */
