#include "erl_nif.h"

#include <assert.h>
#include <math.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "cbroker_omap.h"

/*********************************************************************/

/* The columns below are aligned on purpose. */
/* clang-format off */
#define ATOM_LIST \
    X(_async,                 "async") \
    X(_await,                 "await") \
    X(_badarg,                "badarg") \
    X(_badopt,                "badopt") \
    X(_badopts,               "badopts") \
    X(_batches,               "batches") \
    X(_batch_consumed,        "batch_consumed") \
    X(_batch_full,            "batch_full")  \
    X(_cancelled,             "cancelled") \
    X(_cancelled_nb,          "cancelled_nb") \
    X(_cells,                 "cells") \
    X(_closed,                "closed") \
    X(_creator,               "creator") \
    X(_consumed_count,        "consumed_count") \
    X(_depends_on_creator,    "depends_on_creator") \
    X(_empty,                 "empty") \
    X(_false,                 "false") \
    X(_id,                    "id") \
    X(_left,                  "left")  \
    X(_left_count,            "left_count")  \
    X(_match,                 "match") \
    X(_matched,               "matched") \
    X(_nr_of_cells_per_batch, "nr_of_cells_per_batch") \
    X(_nr_of_schedulers,      "nr_of_schedulers") \
    X(_nb,                    "nb") \
    X(_ref_count,             "ref_count") \
    X(_regular,               "regular") \
    X(_retry,                 "retry") \
    X(_right,                 "right") \
    X(_right_count,           "right_count") \
    X(_stopped,               "stopped") \
    X(_tag_batch_shift,       "tag_batch_shift") \
    X(_tag_offset_mask,       "tag_offset_mask") \
    X(_too_late,              "too_late") \
    X(_true,                  "true") \
    X(_waiting,               "waiting") \
    X(_zzzzzz,                "zzzzzzz")
/* clang-format on */

#define MAX(a, b) ((a) >= (b) ? (a) : (b))
#define MIN(a, b) ((a) <= (b) ? (a) : (b))

#define DO_LOG false

#if DO_LOG
#define LOG(fmt, ...)                                                                              \
    do {                                                                                           \
        enif_fprintf(stderr, fmt "\n\r", ##__VA_ARGS__);                                           \
        fflush(stderr);                                                                            \
    } while (0)
#else
#define LOG(fmt, ...)
#endif

#define LOG_UNCOND(fmt, ...)                                                                       \
    do {                                                                                           \
        enif_fprintf(stderr, fmt "\n\r", ##__VA_ARGS__);                                           \
        fflush(stderr);                                                                            \
    } while (0)

/*********************************************************************/

//

typedef uint_fast64_t offset_t;

typedef offset_t batch_id_t;

//

typedef ssize_t ref_count_t;

//

typedef struct {
    ErlNifTime enqueue_ts;
    ErlNifPid pid;
    ErlNifEnv* env;
    ERL_NIF_TERM offer;
    //
    ERL_NIF_TERM broker_term;
    batch_id_t batch_id;
    offset_t offset;
    void* tag;
} match_t;

//

typedef struct {
    ErlNifMonitor mon;
    match_t* match;
} tag_t;

//

typedef struct {
    _Atomic(match_t*) match;
} cell_t;

//

typedef struct {
    batch_id_t id;
    _Atomic(ref_count_t) ref_count;
    _Atomic(offset_t) left_count;
    _Atomic(offset_t) right_count;
    atomic_size_t consumed_count;
    size_t nr_of_cells;
    cell_t cells[];
} batch_t;

//

typedef struct {
    atomic_bool is_closed;
    cbroker_omap_t* batches;
} global_state_t;

//

typedef struct {
    void** array;
    size_t count;
    size_t size;
    void* (*alloc_cb)();
    void (*clear_cb)(void*);
    void (*free_cb)(void*);
} mempool_t;

//

typedef struct {
    bool is_closed;
    cbroker_omap_t* batches;
    batch_id_t left_id;
    batch_id_t right_id;
    //
    batch_t* new_batch; // allocated outside critical section, ready to go
    mempool_t match_pool;
    mempool_t env_pool;
    mempool_t tag_pool;
} local_state_t;

//

typedef struct {
    bool depends_on_creator;
} broker_opts_t;

//

typedef struct {
    broker_opts_t opts;
    ErlNifPid creator_pid;
    ErlNifMonitor creator_mon;
    //
    size_t nr_of_schedulers;
    size_t nr_of_cells_per_batch;
    unsigned tag_batch_shift;
    uint64_t tag_offset_mask;
    //
    ErlNifMutex* global_lock;
    global_state_t global_state;
    //
    local_state_t local_states[];
} broker_t;

//

typedef ssize_t thread_id_t;

/*********************************************************************/

typedef struct {
    batch_t* batch;
    bool found_locally;
    broker_t* broker;
    local_state_t* local_state;
} handle_t;

//

typedef struct {
    ErlNifEnv* env;
    ErlNifTime enqueue_ts;
    ErlNifPid self;
    ERL_NIF_TERM self_term;
    //
    ERL_NIF_TERM broker_term;
    ERL_NIF_TERM side;
    ERL_NIF_TERM offer;
    //
    broker_t* broker;
    bool is_left;
    bool is_async;
    bool is_nb;
    local_state_t* local_state;
    size_t copied_bytes;
} ask_ctx_t;

//

typedef struct {
    batch_t* batch;
    offset_t offset;
    bool consume_slot;
    match_t* our_match; // optional, reuse if we allocated it but ended up 2nd
    ERL_NIF_TERM our_tag;
    match_t* opposite_match;
} ask_out_t;

/*********************************************************************/

#define X(field, name) ERL_NIF_TERM field;
static struct {
    ATOM_LIST
} Atoms;
#undef X

//

static struct {
    ErlNifResourceType* broker;
    ErlNifResourceType* tag;
} ResourceTypes;

static _Atomic(thread_id_t) next_thread_id = 0;
static _Thread_local thread_id_t my_thread_id = -1;

static match_t sentinel_match_success;
static match_t sentinel_match_cancelled;

/*********************************************************************/

static thread_id_t get_or_assign_thread_id(const size_t nr_of_schedulers)
{
    if (my_thread_id == -1) {
        if (enif_thread_type() == ERL_NIF_THR_NORMAL_SCHEDULER) {
            my_thread_id = atomic_fetch_add_explicit(&next_thread_id, 1, memory_order_relaxed);
        }
        else {
            my_thread_id = nr_of_schedulers;
        }
    }
    assert(my_thread_id >= 0);
    return my_thread_id;
}

/*********************************************************************/

static ErlNifTime monotonic_ts() { return enif_monotonic_time(ERL_NIF_USEC); }

/*********************************************************************/

static ERL_NIF_TERM raise_tuple2(ErlNifEnv* env, ERL_NIF_TERM reason_type,
                                 ERL_NIF_TERM reason_content)
{
    ERL_NIF_TERM reason = enif_make_tuple2(env, reason_type, reason_content);
    return enif_raise_exception(env, reason);
}

static ERL_NIF_TERM make_badarg(ErlNifEnv* env, ERL_NIF_TERM term)
{
    return raise_tuple2(env, Atoms._badarg, term);
}

static ERL_NIF_TERM make_badopts(ErlNifEnv* env, ERL_NIF_TERM term)
{
    return raise_tuple2(env, Atoms._badopts, term);
}

static ERL_NIF_TERM make_badopt(ErlNifEnv* env, ERL_NIF_TERM term)
{
    return raise_tuple2(env, Atoms._badopt, term);
}

static ERL_NIF_TERM make_await(ErlNifEnv* env, ERL_NIF_TERM tag)
{
    return enif_make_tuple2(env, Atoms._await, tag);
}

static ERL_NIF_TERM make_match(ErlNifEnv* env, ERL_NIF_TERM match_ref, ERL_NIF_TERM offer,
                               ErlNifTime enqueue_ts)
{
    int_fast64_t sojourn_time = monotonic_ts() - enqueue_ts;
    return enif_make_tuple4(env, Atoms._match, match_ref, offer,
                            enif_make_int64(env, sojourn_time));
}

static ERL_NIF_TERM make_cancelled(ErlNifEnv* env, bool did_broker_close, int64_t sojourn_time)
{
    ERL_NIF_TERM name = (did_broker_close ? Atoms._stopped : Atoms._cancelled);
    return enif_make_tuple2(env, name, enif_make_int64(env, sojourn_time));
}

// static ERL_NIF_TERM make_tag(ErlNifEnv* env, broker_t* broker, ERL_NIF_TERM broker_term,
//                              ERL_NIF_TERM side, const batch_id_t batch_id, const offset_t offset)
// {
//     assert(offset <= broker->tag_offset_mask);
//     const int64_t batch_id_bits = batch_id << broker->tag_batch_shift;
//
//     if ((batch_id_bits >> broker->tag_batch_shift) != batch_id ||
//         (batch_id_bits >= ((1llu << 59)))) {
//         // batch_id is too large
//         ERL_NIF_TERM batch_id_term = enif_make_uint64(env, batch_id);
//         int64_t signed_offset = (side == Atoms._left ? -offset : offset);
//         ERL_NIF_TERM offset_term = enif_make_int64(env, signed_offset);
//         return enif_make_tuple3(env, broker_term, batch_id_term, offset_term);
//     }
//     else {
//         const int64_t offset_bits = offset & broker->tag_offset_mask;
//         const int64_t tag_bits = batch_id_bits | offset_bits;
//         const int64_t signed_tag_bits = (side == Atoms._left ? -tag_bits : tag_bits);
//         ERL_NIF_TERM tag_bits_term = enif_make_int64(env, signed_tag_bits);
//         return enif_make_list_cell(env, broker_term, tag_bits_term);
//     }
// }

/*********************************************************************/

static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out_broker)
{
    return enif_get_resource(env, term, ResourceTypes.broker, (void**)out_broker);
}

static int get_boolean(ERL_NIF_TERM term, bool* out)
{
    if (term == Atoms._true) {
        *out = true;
        return 1;
    }
    else if (term == Atoms._false) {
        *out = false;
        return 1;
    }
    return 0;
}

static int get_broker_opts(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM* out_bad_opt,
                           broker_opts_t* out_opts)
{
    ERL_NIF_TERM head, tail, key, value;
    int arity = 0;
    const ERL_NIF_TERM* elements;

    while (enif_get_list_cell(env, term, &head, &tail)) {
        term = tail;

        if (enif_is_atom(env, head)) {
            key = head;
            value = Atoms._true;
        }
        else if (enif_get_tuple(env, head, &arity, &elements) && arity == 2 &&
                 enif_is_atom(env, elements[0])) {
            key = elements[0];
            value = elements[1];
        }
        else {
            *out_bad_opt = head;
            return -1;
        }

        //

        if (key == Atoms._depends_on_creator && get_boolean(value, &out_opts->depends_on_creator)) {
            continue;
        }
        else {
            *out_bad_opt = head;
            return -1;
        }
    }

    //

    if (enif_is_empty_list(env, term)) {
        return 0;
    }

    return -2;
}

static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, tag_t** out_tag)
{
    return enif_get_resource(env, term, ResourceTypes.tag, (void**)out_tag);
}

// static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM* out_broker_term,
//                    broker_t** out_broker, ERL_NIF_TERM* out_side, batch_id_t* out_batch_id,
//                    offset_t* out_offset)
//{
//     broker_t* broker = NULL;
//
//     ERL_NIF_TERM head, tail;
//     int64_t signed_tag_bits = 0;
//
//     if (enif_get_list_cell(env, term, &head, &tail)) {
//         if (get_broker(env, head, &broker) && enif_get_int64(env, tail, &signed_tag_bits)) {
//             *out_broker_term = head;
//             *out_broker = broker;
//
//             uint64_t tag_bits = 0;
//
//             if (signed_tag_bits < 0) {
//                 *out_side = Atoms._left;
//                 tag_bits = -signed_tag_bits;
//             }
//             else {
//                 *out_side = Atoms._right;
//                 tag_bits = signed_tag_bits;
//             }
//
//             *out_batch_id = tag_bits >> broker->tag_batch_shift;
//             *out_offset = tag_bits & broker->tag_offset_mask;
//             return 1;
//         }
//         return 0;
//     }
//
//     int arity = 0;
//     const ERL_NIF_TERM* elements = NULL;
//     batch_id_t batch_id = 0;
//     int64_t signed_offset = 0;
//
//     if (enif_get_tuple(env, term, &arity, &elements) && arity == 3 &&
//         get_broker(env, elements[0], &broker) && enif_get_uint64(env, elements[1], &batch_id) &&
//         enif_get_int64(env, elements[2], &signed_offset)) {
//         *out_broker_term = elements[0];
//         *out_broker = broker;
//         *out_batch_id = batch_id;
//
//         if (signed_offset < 0) {
//             *out_side = Atoms._left;
//             *out_offset = -signed_offset;
//         }
//         else {
//             *out_side = Atoms._right;
//             *out_offset = signed_offset;
//         }
//         return 1;
//     }
//     return 0;
// }

/*********************************************************************/

static void mempool_init(mempool_t* pool)
{
    pool->count = 8;
    pool->size = 8;
    pool->array = enif_alloc(pool->size * sizeof(void*));

    for (size_t i = 0; i < pool->count; i++) {
        pool->array[i] = pool->alloc_cb();
    }
}

static void* mempool_get(ask_ctx_t* ctx, mempool_t* pool)
{
    void* obj = NULL;

    if (pool->count == 0) {
        obj = pool->alloc_cb();
    }
    else {
        size_t count = --pool->count;
        obj = pool->array[count];
    }

    assert(obj != NULL);
    return obj;
}

static void mempool_return(mempool_t* pool, void* obj)
{
    if (pool->count >= pool->size) {
        pool->free_cb(obj);
        return;
    }
    else {
        pool->clear_cb(obj);
        pool->array[pool->count++] = obj;
    }
}

static void mempool_destroy(mempool_t* pool)
{
    for (size_t i = 0; i < pool->count; i++) {
        void* obj = pool->array[i];
        pool->free_cb(obj);
    }

    if (pool->array != NULL) {
        enif_free(pool->array);
    }

    pool->array = NULL;
    pool->count = 0;
    pool->size = 0;
}

/*********************************************************************/

static void* env_pool_cb_alloc() { return enif_alloc_env(); }

static void env_pool_cb_clear(void* obj)
{
    ErlNifEnv* env = (ErlNifEnv*)obj;
    enif_clear_env(env);
}

static void env_pool_cb_free(void* obj) { enif_free_env(obj); }

static void env_pool_init(mempool_t* pool)
{
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = env_pool_cb_alloc;
    pool->clear_cb = env_pool_cb_clear;
    pool->free_cb = env_pool_cb_free;
    mempool_init(pool);
}

/*********************************************************************/

static void* match_pool_cb_alloc()
{
    match_t* match = enif_alloc(sizeof(match_t));
    memset(match, 0, sizeof(match_t));
    return match;
}

static void match_pool_cb_clear(void* obj)
{
    match_t* match = (match_t*)obj;
    memset(match, 0, sizeof(match_t));
}

static void match_pool_cb_free(void* obj) { enif_free(obj); }

static void match_pool_init(mempool_t* pool)
{
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = match_pool_cb_alloc;
    pool->clear_cb = match_pool_cb_clear;
    pool->free_cb = match_pool_cb_free;
    mempool_init(pool);
}

/*********************************************************************/

static void* tag_pool_cb_alloc()
{
    tag_t* tag = enif_alloc_resource(ResourceTypes.tag, sizeof(tag_t));
    memset(tag, 0, sizeof(tag_t));
    return tag;
}

static void tag_pool_cb_clear(void* obj)
{
    tag_t* tag = (tag_t*)obj;
    memset(tag, 0, sizeof(tag_t));
}

static void tag_pool_cb_free(void* obj) { enif_release_resource(obj); }

static void tag_pool_init(mempool_t* pool)
{
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = tag_pool_cb_alloc;
    pool->clear_cb = tag_pool_cb_clear;
    pool->free_cb = tag_pool_cb_free;
    mempool_init(pool);
}

/*********************************************************************/

static void handle_init(handle_t* handle, batch_t* batch, const bool found_locally,
                        broker_t* broker, local_state_t* local_state)
{
    memset(handle, 0, sizeof(handle_t));
    handle->batch = batch;
    handle->found_locally = found_locally;
    handle->broker = broker;
    handle->local_state = local_state;
}

/*********************************************************************/

static size_t new_broker_size(const size_t nr_of_schedulers)
{
    return sizeof(broker_t) + (nr_of_schedulers * sizeof(local_state_t));
}

/*********************************************************************/

static size_t new_batch_size(const size_t nr_of_cells)
{
    return sizeof(batch_t) + (nr_of_cells * sizeof(cell_t));
}

static void batch_init(batch_t* batch, const batch_id_t id, const size_t nr_of_cells)
{
    memset(batch, 0, new_batch_size(nr_of_cells));
    batch->id = id;
    atomic_store(&batch->ref_count, 1);
    atomic_store(&batch->left_count, 0);
    atomic_store(&batch->right_count, 0);
    atomic_store(&batch->consumed_count, 0);
    batch->nr_of_cells = nr_of_cells;
}

static batch_t* batch_new(const batch_id_t id, const size_t nr_of_cells)
{
    const size_t size = new_batch_size(nr_of_cells);
    batch_t* batch = enif_alloc(size);
    batch_init(batch, id, nr_of_cells);
    return batch;
}

static void batch_ref_count_inc(batch_t* batch)
{
    ref_count_t ref_count =
        1 + atomic_fetch_add_explicit(&batch->ref_count, +1, memory_order_relaxed);
    assert(ref_count >= 2);
}

static ERL_NIF_TERM batch_to_term(ErlNifEnv* env, const batch_t* batch)
{
    const size_t nr_of_cells = batch->nr_of_cells;
    ERL_NIF_TERM* cell_terms = enif_alloc(nr_of_cells * sizeof(ERL_NIF_TERM));

    for (offset_t i = 0; i < nr_of_cells; i++) {
        const cell_t* cell = &batch->cells[i];
        const match_t* match = atomic_load(&cell->match);

        if (match == NULL) {
            cell_terms[i] = Atoms._empty;
        }
        else if (match == &sentinel_match_cancelled) {
            cell_terms[i] = Atoms._cancelled;
        }
        else if (match == &sentinel_match_success) {
            cell_terms[i] = Atoms._matched;
        }
        else {
            cell_terms[i] = Atoms._waiting;
        }
    }

    const ref_count_t ref_count = atomic_load(&batch->ref_count);
    const offset_t left_count = atomic_load(&batch->left_count);
    const offset_t right_count = atomic_load(&batch->right_count);
    const size_t consumed_count = atomic_load(&batch->consumed_count);

    ERL_NIF_TERM cell_terms_list = enif_make_list_from_array(env, cell_terms, nr_of_cells);
    enif_free(cell_terms);

    return enif_make_list6(
        env, enif_make_tuple2(env, Atoms._id, enif_make_uint64(env, batch->id)),
        enif_make_tuple2(env, Atoms._ref_count, enif_make_int64(env, ref_count)),
        enif_make_tuple2(env, Atoms._left_count, enif_make_uint64(env, left_count)),
        enif_make_tuple2(env, Atoms._right_count, enif_make_uint64(env, right_count)),
        enif_make_tuple2(env, Atoms._consumed_count, enif_make_uint64(env, consumed_count)),
        enif_make_tuple2(env, Atoms._cells, cell_terms_list));
}

/*********************************************************************/

static void return_batch(local_state_t* local_state, batch_t** batch_ptr)
{
    batch_t* batch = *batch_ptr;

    if (local_state->new_batch == NULL) {
        local_state->new_batch = batch;
    }
    else {
        enif_free(batch);
    }

    *batch_ptr = NULL;
}

static void handle_ref_count_dec(handle_t* handle)
{
    batch_t* batch = handle->batch;
    assert(batch != NULL);

    broker_t* broker = handle->broker;
    local_state_t* local_state = handle->local_state;

    batch_id_t batch_id = batch->id;
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;
    batch_t* returnable_batch = NULL;

    /* acq_rel, not relaxed: release so this thread's cell writes precede the
     * free below; acquire so the thread that observes 1 sees every other
     * dropper's writes
     */
    ref_count_t ref_count =
        (atomic_fetch_sub_explicit(&batch->ref_count, 1, memory_order_acq_rel) - 1);
    LOG("DESC REF COUNT for batch %u: %u", batch_id, ref_count);
    assert(ref_count >= 1);

    if (handle->found_locally > 0) {
        cbroker_omap_delete_and_next(local_state->batches, batch_id, NULL, NULL, NULL);
    }

    if (ref_count == 1) {
        global_state_t* global_state = &broker->global_state;
        enif_mutex_lock(broker->global_lock);

        if (cbroker_omap_lookup(global_state->batches, batch_id, (void**)&batch)) {
            ref_count = atomic_load_explicit(&batch->ref_count, memory_order_acquire);
            assert(ref_count >= 1);

            if (ref_count == 1) {
                bool has_next = true;
                cbroker_omap_delete_and_next(global_state->batches, batch_id, &has_next, NULL,
                                             NULL);

                LOG("CONSUME: batch %u removed", batch_id);

                if (has_next) {
                    returnable_batch = batch;
                }
                else {
                    // We reuse the batch right away, ensuring batch IDs are not reused
                    // by a thread that lagged behind
                    const batch_id_t next_batch_id = batch_id + 1;
                    batch_init(batch, next_batch_id, batch->nr_of_cells);
                    map_res = cbroker_omap_insert(global_state->batches, next_batch_id, batch);
                    assert(map_res == CBROKER_OMAP_OK);
                    batch = NULL;
                }
            }
        }

        enif_mutex_unlock(broker->global_lock);
    }

    handle->batch = NULL;

    if (returnable_batch != NULL) {
        if (local_state == NULL) {
            enif_free(returnable_batch);
        }
        else {
            return_batch(local_state, &returnable_batch);
        }
        assert(returnable_batch == NULL);
    }
}

static bool handle_consume_slot(handle_t* handle)
{
    batch_t* batch = handle->batch;

    /* acq_rel: the incrementer that reaches nr_of_cells triggers teardown, so
     * this behaves as a reference release. See `batch_lower_ref_count`. */
    size_t consumed_count =
        1 + atomic_fetch_add_explicit(&batch->consumed_count, 1, memory_order_acq_rel);

    if (consumed_count < batch->nr_of_cells) {
        return false;
    }
    else {
        // assert(consumed_count == batch->nr_of_cells); // no longer true due to batch_cancel_all()
        handle_ref_count_dec(handle);
        return true;
    }
}

static void batch_consume_local_slot(broker_t* broker, local_state_t* local_state, batch_t* batch)
{
    handle_t handle;
    memset(&handle, 0, sizeof(handle_t));

    handle.batch = batch;
    handle.found_locally = true;
    handle.broker = broker;
    handle.local_state = local_state;
    handle_consume_slot(&handle);
}

static bool batch_lookup(broker_t* broker, local_state_t* local_state, batch_id_t batch_id,
                         handle_t* out_handle)
{
    batch_t* batch = NULL;

    if (local_state != NULL &&
        cbroker_omap_lookup(local_state->batches, batch_id, (void**)&batch)) {
        out_handle->batch = batch;
        out_handle->found_locally = true;
        out_handle->broker = broker;
        out_handle->local_state = local_state;
        return true;
    }
    else {
        global_state_t* global_state = &broker->global_state;
        enif_mutex_lock(broker->global_lock);
        if (cbroker_omap_lookup(global_state->batches, batch_id, (void**)&batch)) {
            atomic_fetch_add_explicit(&batch->ref_count, 1, memory_order_relaxed);
            enif_mutex_unlock(broker->global_lock);
            out_handle->batch = batch;
            out_handle->found_locally = false;
            out_handle->broker = broker;
            out_handle->local_state = local_state;
            return true;
        }
        else {
            enif_mutex_unlock(broker->global_lock);
            return false;
        }
    }
}

///////////////

/*********************************************************************/

static void broker_get_all_batches(broker_t* broker, local_state_t* local_state,
                                   handle_t** out_array, size_t* out_nr_of_batches)
{
    global_state_t* global_state = &broker->global_state;
    enif_mutex_lock(broker->global_lock);

    const size_t nr_of_batches = cbroker_omap_size(global_state->batches);
    const size_t array_size = nr_of_batches * sizeof(handle_t);
    handle_t* array = enif_alloc(array_size);
    memset(array, 0, array_size);

    batch_t** batches = (batch_t**)cbroker_omap_values(global_state->batches);

    for (size_t i = 0; i < nr_of_batches; i++) {
        batch_t* batch = batches[i];
        const batch_id_t batch_id = batch->id;

        handle_t* handle = &array[i];
        handle->batch = batch;

        handle->found_locally =
            (local_state != NULL ? cbroker_omap_lookup(local_state->batches, batch_id, NULL)
                                 : false);

        handle->broker = broker;
        handle->local_state = local_state;

        if (!handle->found_locally) {
            batch_ref_count_inc(batch);
        }
    }

    enif_mutex_unlock(broker->global_lock);

    *out_array = array;
    *out_nr_of_batches = nr_of_batches;
}

/*********************************************************************/

/*********************************************************************/

static batch_t* global_state_init(global_state_t* global_state, const size_t nr_of_cells_per_batch)
{
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;

    //

    global_state->is_closed = false;
    global_state->batches = cbroker_omap_new();

    const batch_id_t first_batch_id = 1;
    batch_t* first_batch = batch_new(first_batch_id, nr_of_cells_per_batch);

    map_res = cbroker_omap_insert(global_state->batches, first_batch->id, first_batch);
    assert(map_res == CBROKER_OMAP_OK);

    return first_batch;
}

static void global_state_close(global_state_t* global_state)
{
    bool prev_value = atomic_exchange(&global_state->is_closed, true);
    assert(prev_value == false);
}

/*********************************************************************/

static void ensure_new_batch(batch_t** ptr, const size_t nr_of_cells_per_batch, ask_ctx_t* ask_ctx)
{
    if (*ptr == NULL) {
        *ptr = batch_new(0, nr_of_cells_per_batch);
        if (ask_ctx != NULL) {
            ask_ctx->copied_bytes += new_batch_size((*ptr)->nr_of_cells);
        }
    }
}

static void ensure_one_entry_in_pool(mempool_t* pool)
{
    if (pool->count == 0) {
        assert(pool->size > 0);
        pool->array[pool->count++] = pool->alloc_cb();
    }
}

static void local_states_init(local_state_t local_states[], const size_t nr_of_schedulers,
                              batch_t* first_batch)
{
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;
    assert(first_batch != NULL);

    for (thread_id_t thread_id = 0; thread_id < nr_of_schedulers; thread_id++) {
        local_state_t* local_state = &local_states[thread_id];
        local_state->is_closed = false;
        local_state->batches = cbroker_omap_new();

        map_res = cbroker_omap_insert(local_state->batches, first_batch->id, first_batch);
        assert(map_res == CBROKER_OMAP_OK);
        batch_ref_count_inc(first_batch);

        LOG("[local_states_init] First batch is %llu", first_batch->id);
        local_state->left_id = first_batch->id;
        local_state->right_id = first_batch->id;

        ensure_new_batch(&local_state->new_batch, first_batch->nr_of_cells, NULL);
        match_pool_init(&local_state->match_pool);
        tag_pool_init(&local_state->tag_pool);
        env_pool_init(&local_state->env_pool);
    }
}

static void local_states_dirty_close(local_state_t local_states[], const size_t nr_of_schedulers)
{
    // dirty writes
    for (thread_id_t thread_id = 0; thread_id < nr_of_schedulers; thread_id++) {
        local_state_t* local_state = &local_states[thread_id];
        local_state->is_closed = true;
    }
}

/*********************************************************************/

static local_state_t* broker_local_state(broker_t* broker)
{
    const thread_id_t thread_id = get_or_assign_thread_id(broker->nr_of_schedulers);
    assert(thread_id >= 0);

    if (thread_id < broker->nr_of_schedulers) {
        return &broker->local_states[thread_id];
    }
    return NULL;
}

static batch_t* ask_get_next_batch(ask_ctx_t* ctx, const batch_id_t prev_batch_id)
{
    local_state_t* local_state = ctx->local_state;

    batch_id_t next_batch_id = 0;
    batch_t* next_batch = NULL;
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;

    if (!cbroker_omap_next(local_state->batches, prev_batch_id, &next_batch_id,
                           (void**)&next_batch)) {
        broker_t* broker = ctx->broker;
        global_state_t* global_state = &broker->global_state;

        enif_mutex_lock(broker->global_lock);

        if (atomic_load(&global_state->is_closed)) {
            enif_mutex_unlock(broker->global_lock);
            local_state->is_closed = true;
            return NULL;
        }

        if (!cbroker_omap_next(global_state->batches, prev_batch_id, &next_batch_id,
                               (void**)&next_batch)) {
            next_batch_id = prev_batch_id + 1;
            next_batch = local_state->new_batch;
            local_state->new_batch = NULL;
            assert(next_batch != NULL);
            batch_init(next_batch, next_batch_id, next_batch->nr_of_cells);
            map_res = cbroker_omap_insert(global_state->batches, next_batch_id, next_batch);
            assert(map_res == CBROKER_OMAP_OK);
            atomic_store_explicit(&next_batch->ref_count, 2, memory_order_relaxed);
        }
        else {
            size_t ref_count =
                1 + atomic_fetch_add_explicit(&next_batch->ref_count, 1, memory_order_seq_cst);
            LOG("ASC||| REF COUNT for batch %u: %u", next_batch_id, ref_count);
            assert(ref_count >= 1);
        }

        enif_mutex_unlock(broker->global_lock);

        map_res = cbroker_omap_insert(local_state->batches, next_batch_id, next_batch);
        assert(map_res == CBROKER_OMAP_OK);
    }

    if (ctx->is_left) {
        local_state->left_id = next_batch_id;
    }
    else {
        local_state->right_id = next_batch_id;
    }

    if (local_state->new_batch == NULL) {
        local_state->new_batch = batch_new(0, ctx->broker->nr_of_cells_per_batch);
    }

    return next_batch;
}

static match_t* match_new(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset)
{
    LOG("[%T] [match_new] New match!", ctx->self_term);
    local_state_t* local_state = ctx->local_state;

    LOG("[%T] [match_new] Grabbing new match from local state", ctx->self_term);
    match_t* match = mempool_get(ctx, &local_state->match_pool);
    assert(match != NULL);

    match->enqueue_ts = ctx->enqueue_ts;
    match->pid = ctx->self;
    match->env = mempool_get(ctx, &local_state->env_pool);
    // TODO increment copied bytes?

    LOG("[%T] [match_new] Copying offer", ctx->self_term);
    match->offer = enif_make_copy(match->env, ctx->offer);
    ctx->copied_bytes += enif_term_size(ctx->offer);

    match->broker_term = enif_make_copy(match->env, ctx->broker_term);
    ctx->copied_bytes += enif_term_size(ctx->broker_term);

    match->batch_id = batch_id;
    match->offset = offset;

    tag_t* tag = mempool_get(ctx, &local_state->tag_pool);
    assert(tag != NULL);
    tag->match = match;
    match->tag = tag;

    LOG("[%T] [match_new] Creating monitor", ctx->self_term);
    int mon_res = enif_monitor_process(ctx->env, tag, &match->pid, &tag->mon);
    assert(mon_res == 0);

    return match;
}

/*********************************************************************/

static void notify_if_alive(ErlNifEnv* caller_env, ErlNifPid* pid, ErlNifEnv* msg_env,
                            ERL_NIF_TERM msg)
{
    if (!enif_send(caller_env, pid, msg_env, msg)) {
        /* We assert that the recipient is no longer alive
         * to ensure we're not running from a dirty NIF.
         *
         * Otherwise, the recipient could never be notified
         * of a cancellation (or a match) after the caller
         * had been killed while running the NIF - which
         * would be Very Bad.
         */
        assert(!enif_is_process_alive(caller_env, pid));
    }
}

static void notify_other_of_match_v1(ask_ctx_t* ctx, match_t** our_match_ptr,
                                     match_t* opposite_match, ERL_NIF_TERM match_ref)
{
    match_t* our_match = *our_match_ptr;

    ErlNifEnv* msg_env = our_match->env;

    ERL_NIF_TERM match_ref_copy = enif_make_copy(msg_env, match_ref);
    ERL_NIF_TERM msg_content =
        make_match(msg_env, match_ref_copy, our_match->offer, opposite_match->enqueue_ts);
    ERL_NIF_TERM tag_term = enif_make_resource(msg_env, opposite_match->tag);
    ERL_NIF_TERM msg = enif_make_tuple2(msg_env, tag_term, msg_content);

    // ctx->copied_bytes += enif_term_size(ctx->broker_term);
    // ctx->copied_bytes += enif_term_size(match_ref);

    notify_if_alive(ctx->env, &opposite_match->pid, msg_env, msg);

    //

    tag_t* our_tag = (tag_t*)our_match->tag;
    assert(our_tag != NULL);
    assert(our_tag->match == our_match);

    int demonitor_res = enif_demonitor_process(ctx->env, our_tag, &our_tag->mon);
    assert(demonitor_res == 0);

    our_match->tag = NULL;
    our_tag->match = NULL;

    mempool_return(&ctx->local_state->env_pool, our_match->env);
    our_match->env = NULL;

    our_match->tag = NULL;
    if (ctx->is_async) {
        // We're going to use the tag
        enif_release_resource(our_tag);
    }
    else {
        mempool_return(&ctx->local_state->tag_pool, our_tag);
    }

    mempool_return(&ctx->local_state->match_pool, our_match);
    *our_match_ptr = NULL;
}

static void notify_other_of_match_v2(ask_ctx_t* ctx, match_t* opposite_match,
                                     ERL_NIF_TERM match_ref)
{
    assert(opposite_match->tag != NULL);

    ErlNifEnv* env = ctx->env;

    ERL_NIF_TERM msg_content = make_match(env, match_ref, ctx->offer, opposite_match->enqueue_ts);
    ERL_NIF_TERM tag = enif_make_resource(env, opposite_match->tag);
    ERL_NIF_TERM msg = enif_make_tuple2(env, tag, msg_content);

    notify_if_alive(ctx->env, &opposite_match->pid, NULL, msg);
}

static void notify_self_of_match(ask_ctx_t* ctx, ERL_NIF_TERM our_tag, match_t* opposite_match,
                                 ERL_NIF_TERM match_ref)
{
    ErlNifEnv* env = ctx->env;

    ERL_NIF_TERM offer_copy = enif_make_copy(ctx->env, opposite_match->offer);
    ctx->copied_bytes += enif_term_size(opposite_match->offer);

    ERL_NIF_TERM msg_content = make_match(env, match_ref, offer_copy, ctx->enqueue_ts);
    ERL_NIF_TERM msg = enif_make_tuple2(env, our_tag, msg_content);

    notify_if_alive(ctx->env, &ctx->self, NULL, msg);
}

static void notify_of_cancellation(ErlNifEnv* env, match_t* match, bool did_broker_close)
{
    assert(match->tag != NULL);

    ERL_NIF_TERM tag = enif_make_resource(env, match->tag);
    int64_t sojourn_time = monotonic_ts() - match->enqueue_ts;
    ERL_NIF_TERM cancelled = make_cancelled(env, did_broker_close, sojourn_time);
    ERL_NIF_TERM msg = enif_make_tuple2(env, tag, cancelled);
    notify_if_alive(env, &match->pid, NULL, msg);
}

/*********************************************************************/

static void batch_cancel_all(ErlNifEnv* env, local_state_t* local_state, batch_t* batch,
                             broker_t* broker, ERL_NIF_TERM broker_term, bool did_broker_close)
{
    atomic_size_t consumed_count = atomic_load(&batch->consumed_count);
    if (consumed_count >= batch->nr_of_cells) {
        return;
    }

    atomic_store(&batch->consumed_count, batch->nr_of_cells);
    offset_t left_offset = atomic_exchange(&batch->left_count, batch->nr_of_cells);
    offset_t right_offset = atomic_exchange(&batch->right_count, batch->nr_of_cells);
    offset_t starting_offset = MIN(left_offset, right_offset);

    for (offset_t offset = starting_offset; offset < batch->nr_of_cells; offset++) {
        cell_t* cell = &batch->cells[offset];
        match_t* match = atomic_load(&cell->match);

        if (match == &sentinel_match_cancelled || match == &sentinel_match_success) {
            continue;
        }
        else if (atomic_compare_exchange_strong(&cell->match, &match, &sentinel_match_cancelled)) {
            if (match != NULL) {
                tag_t* tag = (tag_t*)match->tag;
                assert(tag != NULL);

                if (enif_demonitor_process(env, tag, &tag->mon) == 0) {
                    notify_of_cancellation(env, match, true);
                    tag->match = NULL;

                    enif_free_env(match->env);
                    match->env = NULL;
                    match->tag = NULL;
                    enif_free(match);
                }
                enif_release_resource(tag);
            }
        }
        else {
            assert(match == &sentinel_match_cancelled || match == &sentinel_match_success);
        }
    }
}

static void broker_return_all_handles(broker_t* broker, handle_t** array_ptr,
                                      const size_t nr_of_batches)
{
    handle_t* array = *array_ptr;

    for (size_t i = 0; i < nr_of_batches; i++) {
        handle_t* handle = &array[i];
        handle_ref_count_dec(handle);
    }

    enif_free(array);
    *array_ptr = NULL;
}

/*********************************************************************/

static match_t* batch_offset_ask_ensure_our_match(ask_ctx_t* ctx, const batch_id_t batch_id,
                                                  const offset_t offset, ask_out_t* out)
{
    match_t* our_match = out->our_match;

    if (our_match == NULL) {
        out->our_match = match_new(ctx, batch_id, offset);
        out->our_tag = enif_make_resource(ctx->env, out->our_match->tag);
    }
    else {
        LOG("[%T] Reusing already allocated match", ctx->self_term);
        our_match->batch_id = batch_id;
        our_match->offset = offset;
    }
    return out->our_match;
}

static ERL_NIF_TERM batch_offset_ask(ask_ctx_t* ctx, batch_t* batch, offset_t offset,
                                     _Atomic(offset_t)* offset_counter, ask_out_t* out)
{
    LOG("[%T] Asking batch %llu, offset %llu", ctx->self_term, batch->id, offset);
    size_t cell_offset = offset % batch->nr_of_cells;
    LOG("[%T] cell_offset: %llu", ctx->self_term, cell_offset);

    cell_t* cell = &batch->cells[cell_offset];

    _Atomic(match_t*)* match_ptr = &cell->match;
    match_t* match = NULL;

    // FIXME behaviour for `is_nb`
    match = atomic_load(match_ptr);

    // First

    if (match == NULL) {
        if (ctx->is_nb) {
            offset_t expected_offset = offset + 1;
            if (atomic_compare_exchange_strong(offset_counter, &expected_offset, offset)) {
                // counter reverted to previous position
                return Atoms._cancelled_nb;
            }
            else if (atomic_compare_exchange_strong(match_ptr, &match, &sentinel_match_cancelled)) {
                // slot filled with cancellation
                out->consume_slot = true;
                return Atoms._cancelled_nb;
            }
        }
        else {
            LOG("[%T] Allocating our own match", ctx->self_term);
            match_t* our_match = batch_offset_ask_ensure_our_match(ctx, batch->id, offset, out);

            LOG("[%T] Exchanging our own match expecting null", ctx->self_term);
            if (atomic_compare_exchange_strong(match_ptr, &match, our_match)) {
                out->our_match = NULL;
                return Atoms._await;
            }
            LOG("[%T] Null-expecting exchange failed", ctx->self_term);
        }
    }

    if (match == &sentinel_match_cancelled) {
        LOG("[%T] Match is too late: cancelled", ctx->self_term);
        return Atoms._cancelled;
    }

    // Second

    assert(match != &sentinel_match_success);
    LOG("[%T] Exchanging success expecting opposite match", ctx->self_term);

    if (atomic_compare_exchange_strong(match_ptr, &match, &sentinel_match_success)) {
        LOG("[%T] Exchanging succeeded", ctx->self_term);
        tag_t* tag = (tag_t*)match->tag;
        assert(tag != NULL);

        if (enif_demonitor_process(ctx->env, tag, &tag->mon) == 0) {
            out->opposite_match = match;
            out->consume_slot = true;
            return Atoms._match;
        }
        else {
            enif_release_resource(tag);
            return Atoms._cancelled;
        }
    }

    // Cancelled

    assert(match == &sentinel_match_cancelled);
    return Atoms._cancelled;
}

static ERL_NIF_TERM batch_ask(ask_ctx_t* ctx, batch_t* batch, ask_out_t* out)
{
    LOG("Asking batch %llu", batch->id);
    offset_t offset = 0;

    _Atomic(offset_t)* offset_counter = (ctx->is_left ? &batch->left_count : &batch->right_count);

    offset = atomic_fetch_add_explicit(offset_counter, 1, memory_order_relaxed);

    if (offset >= batch->nr_of_cells) {
        LOG("Batch %llu is full", batch->id);
        if (atomic_load_explicit(&batch->consumed_count, memory_order_relaxed) >=
            batch->nr_of_cells) {
            return Atoms._batch_consumed;
        }
        return Atoms._batch_full;
    }
    else {
        ERL_NIF_TERM match_res = batch_offset_ask(ctx, batch, offset, offset_counter, out);

        if (match_res == Atoms._cancelled) {
            return match_res;
        }
        else {
            out->offset = offset;
            return match_res;
        }
    }
}

static batch_t* local_state_get_batch(local_state_t* local_state, const batch_id_t batch_id)
{
    batch_t* batch = NULL;
    LOG("Looking up batch %llu", batch_id);
    cbroker_omap_lookup(local_state->batches, batch_id, (void**)&batch);
    return batch;
}

static ERL_NIF_TERM ask_loop(ask_ctx_t* ctx, ask_out_t* out)
{
    local_state_t* local_state = ctx->local_state;
    batch_id_t batch_id = (ctx->is_left ? local_state->left_id : local_state->right_id);
    batch_t* batch = NULL;
    batch_t* skipped_batch = NULL;
    ERL_NIF_TERM match_res = Atoms._retry;

    const int max_attempts = 400;

    for (int attempt_nr = 1; attempt_nr <= max_attempts && match_res == Atoms._retry;
         attempt_nr++) {
        if (batch == NULL) {
            batch = local_state_get_batch(local_state, batch_id);
        }

        if (batch != NULL) {
            match_res = batch_ask(ctx, batch, out);

            if (match_res == Atoms._batch_full) {
                skipped_batch = batch;
                batch = NULL;
                match_res = Atoms._retry;
            }
            else if (match_res == Atoms._batch_consumed) {
                handle_t handle;
                handle_init(&handle, batch, true, ctx->broker, local_state);
                handle_ref_count_dec(&handle);
                batch = NULL;
                match_res = Atoms._retry;
            }
            else if (match_res == Atoms._cancelled) {
                match_res = Atoms._retry;
                continue;
            }
            else {
                out->batch = batch;
            }
        }

        if (batch == NULL) {
            if ((batch = ask_get_next_batch(ctx, batch_id)) == NULL) {
                return Atoms._closed;
            }
        }

        batch_id = batch->id;

        if (skipped_batch != NULL) {
            batch_id_t opposite_id = (ctx->is_left ? local_state->right_id : local_state->left_id);
            if (opposite_id > skipped_batch->id) {
                handle_t skipped_handle;
                handle_init(&skipped_handle, skipped_batch, true, ctx->broker, local_state);
                handle_ref_count_dec(&skipped_handle);
            }
            skipped_batch = NULL;
        }
    }

    LOG("[ask_loop] match_res: %T", match_res);
    return match_res;
}

/*********************************************************************/

static ERL_NIF_TERM nif_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifPid self;

    if (!enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM bad_opt;
    broker_opts_t opts;
    memset(&opts, 0, sizeof(broker_opts_t));

    if (argc > 0) {
        int opts_res = get_broker_opts(env, argv[0], &bad_opt, &opts);

        if (opts_res == -2) {
            return make_badopts(env, argv[0]);
        }
        else if (opts_res == -1) {
            return make_badopt(env, bad_opt);
        }
        assert(opts_res == 0);
    }

    ErlNifSysInfo sys_info;
    enif_system_info(&sys_info, sizeof(sys_info));
    const size_t nr_of_schedulers = sys_info.scheduler_threads;
    assert(nr_of_schedulers > 0);

    const size_t broker_size = new_broker_size(nr_of_schedulers);
    broker_t* broker = enif_alloc_resource(ResourceTypes.broker, broker_size);
    assert(broker != NULL);
    memset(broker, 0, broker_size);

    memcpy(&broker->opts, &opts, sizeof(broker_opts_t));
    broker->creator_pid = self;
    int mon_res = enif_monitor_process(env, broker, &broker->creator_pid, &broker->creator_mon);
    assert(mon_res == 0);

    broker->nr_of_schedulers = nr_of_schedulers;
    broker->nr_of_cells_per_batch = 32 * nr_of_schedulers;
    broker->tag_batch_shift = ceil(log2(broker->nr_of_cells_per_batch));
    broker->tag_offset_mask = (1ull << broker->tag_batch_shift) - 1;

    broker->global_lock = enif_mutex_create("cbroker.global_lock");
    batch_t* first_batch = global_state_init(&broker->global_state, broker->nr_of_cells_per_batch);
    local_states_init(broker->local_states, nr_of_schedulers, first_batch);

    ERL_NIF_TERM broker_term = enif_make_resource(env, broker);
    enif_release_resource(broker);
    return broker_term;
}

/*********************************************************************/

static inline void consume_timeslice(ErlNifEnv* env, const size_t copied_bytes)
{
    if (copied_bytes == 0) {
        return;
    }

    size_t copy_size = copied_bytes / sizeof(ERL_NIF_TERM);

    // ERTS_MSG_COPY_WORDS_PER_REDUCTION
    const size_t magic_v1 = 64;
    // CONTEXT_REDS
    const size_t magic_v2 = 4000;

    const size_t msg_copy_reds = copy_size / magic_v1;
    int percent = MAX(1, MIN(100, (100 * msg_copy_reds) / magic_v2));

    if (percent != 0) {
        enif_consume_timeslice(env, percent);
    }
}

static ERL_NIF_TERM nif_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ask_ctx_t ctx;
    memset(&ctx, 0, sizeof(ask_ctx_t));
    ctx.env = env;
    ctx.enqueue_ts = monotonic_ts();

    if (enif_self(env, &ctx.self)) {
        ctx.self_term = enif_make_pid(env, &ctx.self);
    }
    else {
        return enif_make_badarg(env);
    }

    ctx.broker_term = argv[0];
    ctx.side = argv[1];
    ctx.offer = argv[2];
    ERL_NIF_TERM ask_type = (argc >= 4 ? argv[3] : Atoms._regular);

    if (!get_broker(env, ctx.broker_term, &ctx.broker)) {
        return make_badarg(env, ctx.broker_term);
    }

    if (ctx.side == Atoms._left) {
        ctx.is_left = true;
    }
    else if (ctx.side != Atoms._right) {
        return make_badarg(env, ctx.side);
    }

    if (ask_type == Atoms._async) {
        ctx.is_async = true;
    }
    else if (ask_type == Atoms._nb) {
        ctx.is_nb = true;
    }
    else if (ask_type != Atoms._regular) {
        return make_badarg(env, ask_type);
    }

    /////////

    LOG("[ask] Getting local state");
    ctx.local_state = broker_local_state(ctx.broker);
    assert(ctx.local_state != NULL);

    if (ctx.local_state->is_closed) {
        return Atoms._closed;
    }

    ensure_new_batch(&ctx.local_state->new_batch, ctx.broker->nr_of_cells_per_batch, &ctx);
    ensure_one_entry_in_pool(&ctx.local_state->match_pool);
    ensure_one_entry_in_pool(&ctx.local_state->env_pool);
    ensure_one_entry_in_pool(&ctx.local_state->tag_pool);

    ask_out_t out;
    memset(&out, 0, sizeof(ask_out_t));

    ERL_NIF_TERM match_res = ask_loop(&ctx, &out);

    ////

    if (match_res == Atoms._await) {
        assert(out.batch != NULL);
        match_res = make_await(env, out.our_tag);

        // TODO review
        ensure_one_entry_in_pool(&ctx.local_state->match_pool);
        ensure_one_entry_in_pool(&ctx.local_state->env_pool);
        ensure_one_entry_in_pool(&ctx.local_state->tag_pool);

        // batch_preemptively_ensure_next(ctx.broker, ctx.local_state, out.batch, out.offset);
    }
    else if (match_res == Atoms._match) {
        match_t* our_match = out.our_match;
        match_t* opposite_match = out.opposite_match;
        out.opposite_match = NULL;

        assert(opposite_match != NULL);

        ERL_NIF_TERM match_ref = enif_make_ref(env);

        LOG("dmatch: about to notify other");

        //

        if (our_match != NULL) {
            notify_other_of_match_v1(&ctx, &our_match, opposite_match, match_ref);
            assert(our_match == NULL);
        }
        else {
            notify_other_of_match_v2(&ctx, opposite_match, match_ref);
        }

        //

        if (out.consume_slot) {
            LOG("dmatch: about to consume slot");
            batch_consume_local_slot(ctx.broker, ctx.local_state, out.batch);
        }

        //

        if (ctx.is_async) {
            notify_self_of_match(&ctx, out.our_tag, opposite_match, match_ref);
            match_res = make_await(env, out.our_tag);
        }
        else {
            ERL_NIF_TERM opposite_offer = enif_make_copy(env, opposite_match->offer);
            ctx.copied_bytes += enif_term_size(opposite_offer);
            match_res = make_match(env, match_ref, opposite_offer, ctx.enqueue_ts);
        }

        mempool_return(&ctx.local_state->env_pool, opposite_match->env);
        opposite_match->env = NULL;

        tag_t* opposite_tag = (tag_t*)opposite_match->tag;
        assert(opposite_tag != NULL);
        assert(opposite_tag->match == opposite_match);

        opposite_match->tag = NULL;
        opposite_tag->match = NULL;
        mempool_return(&ctx.local_state->match_pool, opposite_match);

        enif_release_resource(opposite_tag);
        opposite_match = NULL;
        out.opposite_match = NULL;
    }
    else {
        if (out.consume_slot) {
            assert(out.batch != NULL);
            batch_consume_local_slot(ctx.broker, ctx.local_state, out.batch);
        }

        if (out.our_match != NULL) {
            match_t* our_match = out.our_match;
            tag_t* our_tag = our_match->tag;
            assert(our_tag != NULL);

            int demonitor_res = enif_demonitor_process(env, our_tag, &our_tag->mon);
            assert(demonitor_res == 0);

            mempool_return(&ctx.local_state->env_pool, our_match->env);
            our_match->env = NULL;

            mempool_return(&ctx.local_state->tag_pool, our_match->tag);
            our_match->tag = NULL;

            mempool_return(&ctx.local_state->match_pool, our_match);
            our_match = NULL;
            out.our_match = NULL;
        }
    }

    //

    if (match_res == Atoms._cancelled) {
        match_res = Atoms._retry;
    }
    else if (match_res == Atoms._cancelled_nb) {
        int64_t sojourn_time = monotonic_ts() - ctx.enqueue_ts;
        match_res = make_cancelled(env, false, sojourn_time);
    }

    //

    consume_timeslice(env, ctx.copied_bytes);
    return match_res;
}

static ERL_NIF_TERM nif_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifPid self;
    tag_t* tag = NULL;

    if (!enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM tag_term = argv[0];

    if (!get_tag(env, tag_term, &tag)) {
        return make_badarg(env, tag_term);
    }

    if (enif_demonitor_process(env, tag, &tag->mon) != 0) {
        // too late
        return Atoms._too_late;
    }

    match_t* match = tag->match;
    assert(match != NULL);
    assert(match->tag == tag);

    broker_t* broker = NULL;
    int get_broker_res = get_broker(match->env, match->broker_term, &broker);
    assert(get_broker_res);

    local_state_t* local_state = broker_local_state(broker);
    assert(local_state != NULL);

    handle_t handle;
    memset(&handle, 0, sizeof(handle_t));

    if (local_state->is_closed) {
        return Atoms._too_late;
    }

    if (!batch_lookup(broker, local_state, match->batch_id, &handle)) {
        return Atoms._too_late;
    }

    batch_t* batch = handle.batch;
    ERL_NIF_TERM res;

    assert(match->offset < batch->nr_of_cells);

    cell_t* cell = &batch->cells[match->offset];

    if (atomic_compare_exchange_strong(&cell->match, &match, &sentinel_match_cancelled)) {
        ErlNifPid cancelled_pid = match->pid;
        int64_t sojourn_time = monotonic_ts() - match->enqueue_ts;

        handle_consume_slot(&handle);

        if (enif_compare_pids(&cancelled_pid, &self)) {
            notify_of_cancellation(env, match, false);
        }

        mempool_return(&local_state->env_pool, match->env);
        match->env = NULL;

        tag->match = NULL;
        enif_release_resource(tag);

        match->tag = NULL;
        mempool_return(&local_state->match_pool, match);

        res = make_cancelled(env, false, sojourn_time);
    }
    else {
        assert(match == &sentinel_match_cancelled || match == &sentinel_match_success);
        res = Atoms._too_late;
    }

    if (handle.batch != NULL && !handle.found_locally) {
        handle_ref_count_dec(&handle);
    }
    return res;
}

//

static ERL_NIF_TERM nif_to_list(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    broker_t* broker = NULL;
    ERL_NIF_TERM broker_term = argv[0];

    if (!get_broker(env, broker_term, &broker)) {
        return make_badarg(env, broker_term);
    }

    local_state_t* local_state = broker_local_state(broker);
    assert(local_state != NULL);

    handle_t* handles = NULL;
    size_t nr_of_batches = 0;
    broker_get_all_batches(broker, local_state, &handles, &nr_of_batches);

    //

    ERL_NIF_TERM* batch_terms = enif_alloc(nr_of_batches * sizeof(ERL_NIF_TERM));

    for (size_t i = 0; i < nr_of_batches; i++) {
        handle_t* handle = &handles[i];
        batch_terms[i] = batch_to_term(env, handle->batch);
    }

    //

    broker_return_all_handles(broker, &handles, nr_of_batches);
    assert(handles == NULL);

    ERL_NIF_TERM batch_terms_list = enif_make_list_from_array(env, batch_terms, nr_of_batches);
    enif_free(batch_terms);

    return enif_make_list6(
        env, enif_make_tuple2(env, Atoms._creator, enif_make_pid(env, &broker->creator_pid)),
        enif_make_tuple2(env, Atoms._nr_of_schedulers,
                         enif_make_uint64(env, broker->nr_of_schedulers)),
        enif_make_tuple2(env, Atoms._nr_of_cells_per_batch,
                         enif_make_uint64(env, broker->nr_of_cells_per_batch)),
        enif_make_tuple2(env, Atoms._tag_batch_shift,
                         enif_make_uint64(env, broker->tag_batch_shift)),
        enif_make_tuple2(env, Atoms._tag_offset_mask,
                         enif_make_uint64(env, broker->tag_offset_mask)),
        enif_make_tuple2(env, Atoms._batches, batch_terms_list));
}

/*********************************************************************/

static ErlNifFunc nif_funcs[] = {{"new", 0, nif_new},       {"new", 1, nif_new},
                                 {"ask", 3, nif_ask},       {"ask", 4, nif_ask},
                                 {"cancel", 1, nif_cancel}, {"to_list", 1, nif_to_list}};

/*********************************************************************/

static void local_batch_destroy(batch_id_t key, void* obj, void* ctx)
{
    // We don't actually free the batch here, we just make sure that it's present in global state
    global_state_t* global_state = (global_state_t*)ctx;
    batch_t* batch = (batch_t*)obj;
    bool present_in_global_state = cbroker_omap_lookup(global_state->batches, batch->id, NULL);
    assert(present_in_global_state);
}

static void batch_destroy(batch_id_t key, void* obj, void* ctx)
{
    assert(ctx == NULL);
    batch_t* batch = (batch_t*)obj;

    for (offset_t i = 0; i < batch->nr_of_cells; i++) {
        cell_t* cell = &batch->cells[i];
        match_t* match = atomic_load(&cell->match);

        if (match == &sentinel_match_cancelled || match == &sentinel_match_success) {
            continue;
        }
        else {
            bool exchange =
                atomic_compare_exchange_strong(&cell->match, &match, &sentinel_match_cancelled);
            assert(exchange);
            // TODO?
        }
    }

    enif_free(batch);
}

static void broker_dtor(ErlNifEnv* caller_env, void* obj)
{
    broker_t* broker = (broker_t*)obj;
    enif_mutex_destroy(broker->global_lock);

    global_state_t* global_state = &broker->global_state;

    //

    for (thread_id_t thread_id = 0; thread_id < broker->nr_of_schedulers; thread_id++) {
        local_state_t* local_state = &broker->local_states[thread_id];
        void* destroy_ctx = global_state;
        cbroker_omap_destroy(local_state->batches, local_batch_destroy, destroy_ctx);
        local_state->batches = NULL;

        batch_t* new_batch = local_state->new_batch;
        if (new_batch != NULL) {
            batch_destroy(new_batch->id, new_batch, NULL);
            local_state->new_batch = NULL;
        }

        mempool_destroy(&local_state->match_pool);
        mempool_destroy(&local_state->env_pool);
        mempool_destroy(&local_state->tag_pool);
    }

    //

    cbroker_omap_destroy(global_state->batches, batch_destroy, NULL);
    global_state->batches = NULL;
}

static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
    broker_t* broker = (broker_t*)obj;
    ERL_NIF_TERM broker_term = enif_make_resource(caller_env, broker);

    if (!broker->opts.depends_on_creator) {
        return;
    }

    global_state_close(&broker->global_state);
    local_states_dirty_close(broker->local_states, broker->nr_of_schedulers);

    local_state_t* local_state = broker_local_state(broker);
    handle_t* handles = NULL;
    size_t nr_of_batches = 0;
    broker_get_all_batches(broker, local_state, &handles, &nr_of_batches);

    //

    for (size_t i = 0; i < nr_of_batches; i++) {
        handle_t* handle = &handles[i];
        batch_t* batch = handle->batch;
        batch_cancel_all(caller_env, local_state, batch, broker, broker_term, true);
    }

    //

    broker_return_all_handles(broker, &handles, nr_of_batches);
    assert(handles == NULL);
}

/*********************************************************************/

static void tag_dtor(ErlNifEnv* caller_env, void* obj)
{
    tag_t* tag = (tag_t*)obj;
    match_t* match = tag->match;

    if (match != NULL) {
        ErlNifEnv* env = match->env;

        if (env != NULL) {
            enif_free_env(env);
            match->env = NULL;
        }

        memset(match, 0, sizeof(match_t));
        enif_free(match);
        tag->match = NULL;
    }

    memset(tag, 0, sizeof(tag_t));
}

static void tag_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
    ERL_NIF_TERM pid_term = enif_make_pid(caller_env, pid);

    tag_t* tag = (tag_t*)obj;

    match_t* match = tag->match;
    assert(match != NULL);
    assert(match->tag == tag);

    LOG_UNCOND("[match DOWN %T] batch %llu, offset %llu", pid_term, match->batch_id, match->offset);

    broker_t* broker = NULL;
    int res = get_broker(match->env, match->broker_term, &broker);
    assert(res);

    const batch_id_t batch_id = match->batch_id;
    const offset_t offset = match->offset;

    local_state_t* local_state = broker_local_state(broker);
    handle_t handle;
    memset(&handle, 0, sizeof(handle_t));

    if (batch_lookup(broker, local_state, batch_id, &handle)) {
        LOG("[match DOWN %T] batch found", pid_term, match->batch_id);
        batch_t* batch = handle.batch;
        assert(offset < batch->nr_of_cells);

        cell_t* cell = &batch->cells[offset];

        if (atomic_compare_exchange_strong(&cell->match, &match, &sentinel_match_cancelled)) {
            LOG("[match DOWN %T] match cancelled", pid_term, match->batch_id);

            if (local_state == NULL) {
                enif_free_env(match->env);
                match->env = NULL;

                enif_free(match);
                tag->match = NULL;
            }
            else {
                mempool_return(&local_state->env_pool, match->env);
                match->env = NULL;

                mempool_return(&local_state->match_pool, match);
                tag->match = NULL;
            }
            enif_release_resource(tag);

            handle_consume_slot(&handle);
        }
        else {
            LOG("[match DOWN %T] Too late to cancel match", pid_term);
            assert((match == &sentinel_match_cancelled) || (match == &sentinel_match_success));
        }

        if (handle.batch != NULL && !handle.found_locally) {
            handle_ref_count_dec(&handle);
        }
    }
}

/*********************************************************************/

static void init_atoms(ErlNifEnv* caller_env)
{
    memset(&Atoms, 0, sizeof(Atoms));
#define X(field, name) Atoms.field = enif_make_atom(caller_env, name);
    ATOM_LIST
#undef X
}

static void broker_resource_load(ErlNifEnv* caller_env)
{
    ErlNifResourceTypeInit callbacks = {broker_dtor, NULL, broker_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.broker =
        enif_init_resource_type(caller_env, "cbroker", &callbacks, flags, &flags);
    assert(ResourceTypes.broker != NULL);
}

static void tag_resource_load(ErlNifEnv* caller_env)
{
    ErlNifResourceTypeInit callbacks = {tag_dtor, NULL, tag_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.tag =
        enif_init_resource_type(caller_env, "cbroker.tag", &callbacks, flags, &flags);
    assert(ResourceTypes.tag != NULL);
}

static int on_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info)
{
    init_atoms(caller_env);

    memset(&ResourceTypes, 0, sizeof(ResourceTypes));
    broker_resource_load(caller_env);
    tag_resource_load(caller_env);

    return 0;
}

ERL_NIF_INIT(cbroker_nif, nif_funcs, on_load, NULL, NULL, NULL);
