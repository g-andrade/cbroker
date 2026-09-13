#include "erl_nif.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "cbroker_omap.h"

/*********************************************************************/

//    X(_about_to_sleep,       "about_to_sleep") \
//    X(_active,               "active") \
//    X(_delayed_match,        "delayed_match") \
//    X(_depends_on_creator,   "depends_on_creator") \
//    X(_empty,                "empty") \
//    X(_error,                "error") \
//    X(_false,                "false") \
//    X(_fully_async,          "fully_async") \
//    X(_instant_match_first,  "instant_match_first") \
//    X(_instant_match_second, "instant_match_second") \
//    X(_nb,                   "nb") \
//    X(_ok,                   "ok") \
//    X(_self_stopped,         "self_stopped") \
//    X(_stopped,              "stopped") \
//    X(_too_late,             "too_late") \
//    X(_true,                 "true") \


/* The columns below are aligned on purpose. */
/* clang-format off */
#define ATOM_LIST \
    X(_async,                "async") \
    X(_await,                "await") \
    X(_badarg,               "badarg") \
    X(_batch_consumed,       "batch_consumed") \
    X(_batch_full,           "batch_full")  \
    X(_cancelled,            "cancelled") \
    X(_closed,               "closed") \
    X(_left,                 "left")  \
    X(_match,                "match") \
    X(_nb,                   "nb") \
    X(_none,                 "none") \
    X(_regular,              "regular") \
    X(_retry,                "retry") \
    X(_right,                "right") \
    X(_zzzzzz,               "zzzzzzz")
/* clang-format on */

#define MAX(a, b) ((a) >= (b) ? (a) : (b))
#define MIN(a, b) ((a) <= (b) ? (a) : (b))

#define LOG(fmt, ...)
/*#define LOG(fmt, ...) do { \
    enif_fprintf(stderr, fmt "\n\r", ##__VA_ARGS__); \
     fflush(stderr); \
} while (0)*/

/*********************************************************************/

#define CELL_COUNT_CANCELLED -128

#define TARGET_MEMPOOL_SIZE 0

/*********************************************************************/

//

typedef uint_fast64_t offset_t;
typedef _Atomic(offset_t) atomic_offset_t;

typedef offset_t batch_id_t;

//

//

typedef struct {
    ErlNifTime enqueue_ts;
    ERL_NIF_TERM side;
    ErlNifPid pid;
    ErlNifEnv* env;
    ERL_NIF_TERM offer;
} match_t;

//

typedef struct {
    ErlNifMonitor mon;
    ErlNifEnv* env;
    ERL_NIF_TERM broker_term;
    ERL_NIF_TERM tag_term; // self-reference
    ERL_NIF_TERM side;
    batch_id_t batch_id;
    offset_t offset;
} tag_t;

//

typedef struct {
    _Atomic(match_t*) match;
    _Atomic(tag_t*) tag;
} cell_t;

//

typedef ssize_t ref_count_t;

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
    bool is_closed;
    cbroker_omap_t* batches;
} global_state_t;

//

typedef struct {
    void** array;
    size_t count;
    size_t size;
    void* alloc_ctx;
    void* (*alloc_cb)(void*);
    void (*clear_cb)(void*);
    void (*free_cb)(void*);
} mempool_t;

//

typedef struct {
    bool is_closed;
    cbroker_omap_t* batches;
    batch_id_t left_id;
    batch_id_t right_id;
    mempool_t env_pool;
    mempool_t match_pool;
    batch_t* new_batch; // allocated outside critical section, ready to go
} local_state_t;

//

typedef struct {
    ErlNifPid creator_pid;
    ErlNifMonitor creator_mon;
    //
    size_t nr_of_schedulers;
    size_t nr_of_cells_per_batch;
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
} ask_ctx_t;

//

typedef struct {
    batch_t* batch;
    offset_t offset;
    bool consume_slot;
    match_t* our_match; // optional, reuse if we allocated it but ended up 2nd
    ERL_NIF_TERM our_tag_term;
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

static match_t sentinel_match_out;
static match_t sentinel_match_cancelled;
static tag_t sentinel_monitor_done;

/*********************************************************************/

static const thread_id_t get_or_assign_thread_id()
{
    if (my_thread_id == -1) {
        my_thread_id = atomic_fetch_add_explicit(&next_thread_id, 1, memory_order_relaxed);
    }
    assert(my_thread_id >= 0);
    return my_thread_id;
}

/*********************************************************************/

static ErlNifTime monotonic_ts() {
    return enif_monotonic_time(ERL_NIF_USEC);
}

/*********************************************************************/

static ERL_NIF_TERM make_badarg(ErlNifEnv* env, ERL_NIF_TERM term) {
    ERL_NIF_TERM reason = enif_make_tuple2(env, Atoms._badarg, term);
    return enif_raise_exception(env, reason);
}

static ERL_NIF_TERM make_await(ErlNifEnv* env, ERL_NIF_TERM tag)
{
    return enif_make_tuple2(env, Atoms._await, tag);
}

static ERL_NIF_TERM make_match(ErlNifEnv* env, ERL_NIF_TERM match_ref,
                               ERL_NIF_TERM offer, ErlNifTime enqueue_ts)
{
    int_fast64_t sojourn_time = enif_monotonic_time(ERL_NIF_NSEC) - enqueue_ts;
    return enif_make_tuple4(env, Atoms._match, match_ref, offer,
                            enif_make_int64(env, sojourn_time));
}

static ERL_NIF_TERM make_opposite_side(ERL_NIF_TERM side)
{
    if (side == Atoms._left) {
        return Atoms._right;
    }
    else {
        assert(side == Atoms._right);
        return Atoms._left;
    }
}

/*********************************************************************/

static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out_broker) {
    return enif_get_resource(env, term, ResourceTypes.broker, (void**) out_broker);
}

/*********************************************************************/

static void mempool_init(mempool_t* pool)
{
    const size_t initial_size = 1024; // TARGET_MEMPOOL_SIZE;
    pool->size = initial_size;
    pool->array = enif_alloc(pool->size * sizeof(void*));

    pool->count = pool->size;
    for (size_t i = 0; i < initial_size; i++) {
        pool->array[i] = pool->alloc_cb(pool->alloc_ctx);
        pool->clear_cb(pool->array[i]);
    }
}

static void* mempool_get(mempool_t* pool)
{
    void* obj = NULL;

    if (pool->count == 0) {
        obj = pool->alloc_cb(pool->alloc_ctx);
    }
    else {
        size_t count = --pool->count;
        obj = pool->array[count];

        if (count <= (pool->size >> 1) && count >= 8) {
            pool->size = pool->size >> 1;
            assert(pool->size >= count);
            pool->array = enif_realloc(pool->array, pool->size * sizeof(void*));
        }
        assert(pool->array != NULL);
    }

    assert(obj != NULL);
    pool->clear_cb(obj);
    return obj;
}

static void mempool_return(mempool_t* pool, void* obj)
{
    if (pool->count >= TARGET_MEMPOOL_SIZE) {
        pool->free_cb(obj);
        return;
    }
    else if (pool->count == pool->size) {
        if (pool->size == 0) {
            pool->size = 4;
            pool->array = enif_alloc(pool->size * sizeof(void*));
        }
        else {
            pool->size *= 2;
            pool->array = enif_realloc(pool->array, pool->size * sizeof(void*));
        }
        assert(pool->array != NULL);
    }

    pool->clear_cb(obj);
    pool->array[pool->count++] = obj;
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

static void broker_dtor(ErlNifEnv* caller_env, void* obj) {
    // TODO
}

static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon) {
}

/*********************************************************************/

static void tag_dtor(ErlNifEnv* caller_env, void* obj) {
}

static void tag_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon) {
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

/*********************************************************************/

static void handle_init(handle_t* handle, 
                        batch_t* batch, bool found_locally,
                         broker_t* broker, local_state_t* local_state) {
    memset(handle, 0, sizeof(handle_t));
    handle->batch = batch;
    handle->found_locally = found_locally;
    handle->broker = broker;
    handle->local_state = local_state;
}

/*********************************************************************/

static size_t new_broker_size(const size_t nr_of_schedulers) {
    return sizeof(broker_t) + (nr_of_schedulers * sizeof(local_state_t));
}

/*********************************************************************/

static size_t new_batch_size(const size_t nr_of_cells) {
    return sizeof(batch_t) + (nr_of_cells * sizeof(cell_t));
}

static size_t batch_size(batch_t* batch) {
    return sizeof(batch_t) + (batch->nr_of_cells * sizeof(cell_t));
}

static void batch_init(batch_t* batch, const batch_id_t id, const size_t nr_of_cells) {
    memset(batch, 0, new_batch_size(nr_of_cells));
    atomic_store(&batch->ref_count, 1);
    atomic_store(&batch->left_count, 0);
    atomic_store(&batch->right_count, 0);
    atomic_store(&batch->consumed_count, 0);
    batch->nr_of_cells = nr_of_cells;
}

static batch_t* batch_new(const batch_id_t id, const size_t nr_of_cells) {
    const size_t size = new_batch_size(nr_of_cells);
    batch_t* batch = enif_alloc(size);
    batch_init(batch, id, nr_of_cells);
    return batch;
}

static void batch_ref_count_inc(batch_t* batch) {
    ref_count_t ref_count = 1 + atomic_fetch_add_explicit(&batch->ref_count, +1, memory_order_relaxed);
    assert(ref_count >= 2);
}

/*********************************************************************/

static void handle_ref_count_dec(handle_t* handle) {
    batch_t* batch = handle->batch;
    assert(batch != NULL);

    broker_t* broker = handle->broker;
    local_state_t* local_state = handle->local_state;

    batch_id_t batch_id = batch->id;
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;
    batch_t* freeable_batch = NULL;

    /* acq_rel, not relaxed: release so this thread's cell writes precede the
     * free below; acquire so the thread that observes 1 sees every other
     * dropper's writes
     */
    size_t ref_count = atomic_fetch_sub_explicit(&batch->ref_count, 1, memory_order_acq_rel) - 1;
    LOG("DESC REF COUNT for batch %u: %u", batch_id, ref_count);
    assert(ref_count >= 1);

    if (handle->found_locally) {
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
                /*LOG("CONSUME: batch %u deleted", batch_id);
                memset(batch, 0, sizeof(batch_t) + (batch->nr_of_cells * sizeof(cell_t)));
                enif_free(batch);

                if (!has_next) {
                    // we create the next batch right away, ensuring batch IDs are not reused
                    // by a thread that lagged behind.
                    batch_id_t next_batch_id = batch_id + 1;
                    batch_t* next_batch = batch_new(next_batch_id, broker->nr_of_cells_per_batch);
                    map_res = cbroker_omap_insert(global_state->batches, next_batch_id, next_batch);
                    assert(map_res == CBROKER_OMAP_OK);
                    atomic_store_explicit(&next_batch->ref_count, 1, memory_order_relaxed);
                }*/

                LOG("CONSUME: batch %u removed", batch_id);

                if (has_next) {
                    freeable_batch = batch;
                } else {
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

    if (freeable_batch != NULL) {
        enif_free(freeable_batch);
        freeable_batch = NULL;
    }
}

///////////////

/*********************************************************************/
/*********************************************************************/

static void* env_pool_cb_alloc(void* ctx) {
    assert(ctx == NULL);
    return enif_alloc_env();
}

static void env_pool_cb_clear(void* env) {
    enif_clear_env((ErlNifEnv*) env);
}

static void env_pool_cb_free(void* env) {
    enif_free_env(env);
}

static void env_pool_init(mempool_t* pool) {
    pool->alloc_ctx = NULL;
    pool->alloc_cb = env_pool_cb_alloc;
    pool->clear_cb = env_pool_cb_clear;
    pool->free_cb = env_pool_cb_free;
    mempool_init(pool);
}

/*********************************************************************/

static void* match_pool_cb_alloc(void* ctx) {
    assert(ctx == NULL);
    return enif_alloc(sizeof(match_t));
}

static void match_pool_cb_clear(void* match) {
    memset(match, 0, sizeof(match_t));
}

static void match_pool_cb_free(void* match) {
    enif_free(match);
}

static void match_pool_init(mempool_t* pool) {
    pool->alloc_ctx = NULL;
    pool->alloc_cb = match_pool_cb_alloc;
    pool->clear_cb = match_pool_cb_clear;
    pool->free_cb = match_pool_cb_free;
    mempool_init(pool);
}

/*********************************************************************/

/*********************************************************************/

static batch_t* global_state_init(global_state_t* global_state, const size_t nr_of_cells_per_batch) {
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

/*********************************************************************/

static void local_states_init(local_state_t local_states[], 
                              const size_t nr_of_schedulers, 
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

        local_state->left_id = first_batch->id;
        local_state->right_id = first_batch->id;

        env_pool_init(&local_state->env_pool);
        match_pool_init(&local_state->match_pool);
        local_state->new_batch = batch_new(0, first_batch->nr_of_cells);
    }

}

/*********************************************************************/

static local_state_t* broker_local_state(broker_t* broker) {
    const thread_id_t thread_id = get_or_assign_thread_id();
    assert(thread_id >= 0);
    assert(thread_id < broker->nr_of_schedulers);
    return &broker->local_states[thread_id];
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

        if (global_state->is_closed) {
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
    match_t* match = mempool_get(&ctx->local_state->match_pool);

    ErlNifEnv* match_env = mempool_get(&ctx->local_state->env_pool);
    match->enqueue_ts = ctx->enqueue_ts;
    match->side = ctx->side;
    match->pid = ctx->self;
    match->env = match_env;
    match->offer = enif_make_copy(match_env, ctx->offer);

    return match;
}

/*********************************************************************/

static tag_t* tag_new(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset)
{
    tag_t* tag =
        (tag_t*)enif_alloc_resource(ResourceTypes.tag, sizeof(tag_t));
    memset(tag, 0, sizeof(tag_t));

    tag->env = mempool_get(&ctx->local_state->env_pool);
    tag->broker_term = enif_make_copy(tag->env, ctx->broker_term);
    tag->tag_term = enif_make_resource(tag->env, tag);
    tag->batch_id = batch_id;
    tag->offset = offset;
    tag->side = ctx->side;

    /* We should never fail to monitor selves within the queue
     * as long as we're running as a regular NIF
     */
    int monitor_res = enif_monitor_process(ctx->env, tag, &ctx->self, &tag->mon);
    assert(monitor_res == 0);

    enif_release_resource(tag);

    return tag;
}

static void tag_remove(ErlNifEnv* env, local_state_t* local_state, tag_t** tag_ptr)
{
    tag_t* tag = *tag_ptr;
    enif_demonitor_process(env, tag, &tag->mon);

    ErlNifEnv* mon_env = tag->env;
    tag->env = NULL;

    if (local_state != NULL) {
        mempool_return(&local_state->env_pool, mon_env);
    }
    else {
        enif_free_env(mon_env);
    }

    *tag_ptr = NULL;
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

static void notify_of_match(ask_ctx_t* ctx, ErlNifPid* pid, ERL_NIF_TERM tag_term,
                            match_t** match_ptr,
                            const ERL_NIF_TERM side, const batch_id_t batch_id,
                            const offset_t offset, const ERL_NIF_TERM match_ref,
                            ErlNifTime enqueue_ts)
{
    ErlNifEnv* caller_env = ctx->env;
    local_state_t* local_state = ctx->local_state;

    match_t* match = *match_ptr;
    assert(match != NULL);

    // We can reuse the env for the message
    ErlNifEnv* match_env = match->env;
    ERL_NIF_TERM msg_tag = enif_make_copy(match_env, tag_term);
    ERL_NIF_TERM msg_match_ref = enif_make_copy(match_env, match_ref);

    ERL_NIF_TERM msg_content = make_match(match_env, msg_match_ref, match->offer, enqueue_ts);

    ERL_NIF_TERM msg = enif_make_tuple2(match_env, msg_tag, msg_content);
    notify_if_alive(caller_env, pid, match_env, msg);

    mempool_return(&local_state->env_pool, match_env);
    mempool_return(&local_state->match_pool, match);

    *match_ptr = NULL;
}

static void notify_of_match_v2(ask_ctx_t* ctx, ErlNifPid* pid, const ERL_NIF_TERM side,
                               const batch_id_t batch_id, const offset_t offset,
                               const ERL_NIF_TERM match_ref,
                               ErlNifTime enqueue_ts)
{
    ErlNifEnv* caller_env = ctx->env;

    // We can reuse the env for the message
    ERL_NIF_TERM tag = make_tag_simple(caller_env, side, batch_id, offset);

    ERL_NIF_TERM msg_content = make_match(caller_env, match_ref, ctx->offer, enqueue_ts);

    ERL_NIF_TERM msg = enif_make_tuple2(caller_env, tag, msg_content);
    notify_if_alive(caller_env, pid, NULL, msg);
}

static void notify_of_cancellation(ErlNifEnv* env, const batch_id_t batch_id, const offset_t offset,
                                   match_t** match_in_cell_ptr, bool did_broker_stop)
{
    match_t* match_in_cell = *match_in_cell_ptr;
    assert(match_in_cell != NULL);
    assert(match_in_cell != &sentinel_match_cancelled);

    match_in_cell->env = NULL;

    ERL_NIF_TERM tag = make_tag_simple(env, match_in_cell->side, batch_id, offset);
    ERL_NIF_TERM msg_content = (did_broker_stop ? Atoms._stopped : Atoms._cancelled);
    ERL_NIF_TERM msg = enif_make_tuple2(env, tag, msg_content);
    notify_if_alive(env, &match_in_cell->pid, NULL, msg);

    enif_free(match_in_cell);
    *match_in_cell_ptr = NULL;
}

/*********************************************************************/

/*********************************************************************/

static ERL_NIF_TERM batch_offset_ask(ask_ctx_t* ctx, batch_t* batch, offset_t offset,
                                     ask_out_t* out)
{
    size_t cell_offset = offset % batch->nr_of_cells;
    LOG("cell_offset: %llu", cell_offset);

    cell_t* cell = &batch->cells[cell_offset];

    _Atomic(match_t*)* match_ptr = &cell->match;
    match_t* match = NULL;

    _Atomic(tag_t*)* tag_ptr = &cell->tag;
    tag_t* tag = NULL;

    // FIXME behaviour for `is_nb`
    match_t* our_match = NULL;

    match = atomic_load(match_ptr);

    // First

    if (match == NULL) {
        if (our_match == NULL) {
            our_match = match_new(ctx, batch->id, offset);
        }

        if (atomic_compare_exchange_strong(match_ptr, &match, our_match)) {
            // Enqueued

            if (atomic_load(tag_ptr) == &sentinel_monitor_done) {
                // Already cancelled or matched
                return Atoms._await;
            }
            else {
                our_match = NULL;
                tag_t* our_tag = tag_new(ctx, batch->id, cell_offset);
                out->our_tag_term = enif_make_copy(ctx->env, our_tag->tag_term);

                if (atomic_compare_exchange_strong(tag_ptr, &tag, our_tag)) {
                    // Monitored
                    LOG("monitored!! %p", our_tag);
                    return Atoms._await;
                }
                else {
                    LOG("tag: %p (sentinel: %p, ours %p)", tag, &sentinel_monitor_done,
                        our_tag);
                    assert(tag == &sentinel_monitor_done);
                    // Already cancelled or matched
                    tag_remove(ctx->env, ctx->local_state, &our_tag);
                    assert(our_tag == NULL);
                    return Atoms._await;
                }
            }
        }
    }

    // Second

    assert(match != &sentinel_match_out);

    if (atomic_compare_exchange_strong(match_ptr, &match, &sentinel_match_out)) {
        tag = atomic_exchange(tag_ptr, &sentinel_monitor_done);

        if (tag != NULL && tag != &sentinel_monitor_done) {
            LOG("tag on side 2: %p", tag);
            tag_remove(ctx->env, ctx->local_state, &tag);
            assert(tag == NULL);
        }

        out->our_match = our_match;
        out->opposite_match = match;
        out->consume_slot = true;
        return Atoms._match;
    }

    // Cancelled

    assert(match == &sentinel_match_cancelled);
    return Atoms._cancelled;
}

static ERL_NIF_TERM batch_ask(ask_ctx_t* ctx, batch_t* batch, ask_out_t* out)
{
    offset_t offset = 0;

    offset =
        (ctx->is_left ? atomic_fetch_add_explicit(&batch->left_count, 1, memory_order_relaxed)
                      : atomic_fetch_add_explicit(&batch->right_count, 1, memory_order_relaxed));

    if (offset >= batch->nr_of_cells) {
        if (atomic_load_explicit(&batch->consumed_count, memory_order_relaxed) >=
            batch->nr_of_cells) {
            return Atoms._batch_consumed;
        }
        return Atoms._batch_full;
    }
    else {
        ERL_NIF_TERM match_res = batch_offset_ask(ctx, batch, offset, out);

        if (match_res == Atoms._cancelled) {
            return match_res;
        }
        else {
            out->offset = offset;
            return match_res;
        }
    }
}


static ERL_NIF_TERM ask_loop(ask_ctx_t* ctx, ask_out_t* out) {
    local_state_t* local_state = ctx->local_state;
    batch_id_t batch_id = (ctx->is_left ? local_state->left_id : local_state->right_id);
    batch_t* batch = NULL;
    batch_t* skipped_batch = NULL;

    const int max_attempts = 400;

    for (int attempt_nr = 1; attempt_nr <= max_attempts; attempt_nr++) {
        if (batch == NULL) {
            cbroker_omap_lookup(local_state->batches, batch_id, (void**)&batch);
        }

        if (batch != NULL) {
            ERL_NIF_TERM match_res = batch_ask(ctx, batch, out);

            if (match_res == Atoms._batch_full) {
                skipped_batch = batch;
                batch = NULL;
            }
            else if (match_res == Atoms._batch_consumed) {
                handle_t handle;
                handle_init(&handle, batch, true, ctx->broker, local_state);
                handle_ref_count_dec(&handle);
                batch = NULL;
            }
            else if (match_res == Atoms._cancelled && !ctx->is_nb) {
                continue;
            }
            else {
                out->batch = batch;
                return match_res;
            }
        }

        assert(batch == NULL);
        batch = ask_get_next_batch(ctx, batch_id);
        batch_id = batch->id;

        if (skipped_batch != NULL) {
            batch_id_t opposite_id = (ctx->is_left ? local_state->right_id : local_state->left_id);
            if (opposite_id > skipped_batch->id) {
                handle_t skipped_handle;
                handle_init(&skipped_handle, skipped_batch, true, ctx->broker, local_state);
                handle_ref_count_dec(&skipped_handle);
                skipped_batch = NULL;
            }
        }
    }

    return Atoms._retry;
}

/*********************************************************************/

static ERL_NIF_TERM nif_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifPid self;

    if (! enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    ErlNifSysInfo sys_info;
    enif_system_info(&sys_info, sizeof(sys_info));
    const size_t nr_of_schedulers = sys_info.scheduler_threads;
    assert(nr_of_schedulers > 0);

    const size_t broker_size = new_broker_size(nr_of_schedulers);
    broker_t* broker = enif_alloc_resource(ResourceTypes.broker, broker_size);
    assert(broker != NULL);
    memset(broker, 0, broker_size);

    broker->creator_pid = self;
    int mon_res = enif_monitor_process(env, broker, &broker->creator_pid, &broker->creator_mon);
    assert(mon_res == 0);

    broker->nr_of_schedulers = nr_of_schedulers;
    broker->nr_of_cells_per_batch = 32 * nr_of_schedulers;

    broker->global_lock = enif_mutex_create("cbroker.global_lock");
    batch_t* first_batch = global_state_init(&broker->global_state, broker->nr_of_cells_per_batch);
    local_states_init(broker->local_states, nr_of_schedulers, first_batch);

    ERL_NIF_TERM broker_term = enif_make_resource(env, broker);
    enif_release_resource(broker);
    return broker_term;
}

/*********************************************************************/

static ERL_NIF_TERM nif_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ask_ctx_t ctx;
    memset(&ctx, 0, sizeof(ask_ctx_t));
    ctx.env = env;
    ctx.enqueue_ts = monotonic_ts();

    if (! enif_self(env, &ctx.self)) {
        return enif_make_badarg(env);
    }

    ctx.broker_term = argv[0];
    ctx.side = argv[1];
    ctx.offer = argv[2];
    ERL_NIF_TERM ask_type = (argc >= 3 ? argv[3] : Atoms._regular);

    if (! get_broker(env, ctx.broker_term, &ctx.broker)) {
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

    ctx.local_state = broker_local_state(ctx.broker);
        
    ask_out_t out;
    memset(&out, 0, sizeof(ask_out_t));
    out.our_tag_term = Atoms._none;

    ERL_NIF_TERM match_res = ask_loop(&ctx, &out);

    ////

    if (match_res == Atoms._await) {
        // 1 signal sent (monitor)
        ERL_NIF_TERM tag_term = out.our_tag_term;
        assert(tag_term != Atoms._none);
        match_res = make_await(env, tag_term);
        //batch_preemptively_ensure_next(ctx.broker, ctx.local_state, out.batch, out.offset);
        enif_consume_timeslice(env, 30);
    }
    else if (match_res == Atoms._match) {
        assert(out.our_tag_term == Atoms._none);

        match_t* our_match = out.our_match;
        match_t* opposite_match = out.opposite_match;
        out.opposite_match = NULL;

        const batch_id_t batch_id = out.batch->id;
        const offset_t offset = out.offset;

        assert(opposite_match != NULL);

        ERL_NIF_TERM match_ref = enif_make_ref(env);

        LOG("dmatch: about to notify other");
        const ERL_NIF_TERM opposite_side = make_opposite_side(ctx.side);

        //

        if (our_match != NULL) {
            notify_of_match(&ctx, &opposite_match->pid, &opposite_match->tag_term,
                            &our_match, opposite_side, batch_id, offset,
                            match_ref, opposite_match->enqueue_ts);

            assert(our_match == NULL);
        }
        else {
            notify_of_match_v2(&ctx, &opposite_match->pid, opposite_side, batch_id, offset,
                               match_ref, opposite_match->enqueue_ts);
        }

        //

        if (out.consume_slot) {
            LOG("dmatch: about to consume slot");
            batch_consume_local_slot(ctx.broker, ctx.local_state, out.batch);
        }

        //

        ERL_NIF_TERM self_tag = make_tag(&ctx, batch_id, out.offset);

        if (ask_type == Atoms._fully_async) {
            // 2 messages sent
            notify_of_match(&ctx, &ctx.self, &opposite_match, ctx.side, batch_id, offset, match_ref,
                            ctx.with_stats, ctx.enqueue_ts);

            assert(opposite_match == NULL);

            match_res = make_await(env, self_tag);
            // enif_consume_timeslice(env, 100);
        }
        else {
            // 1 message sent
            ERL_NIF_TERM opposite_value = enif_make_copy(env, opposite_match->offer);
            env_pool_return(ctx.local_state, opposite_match->env);
            match_pool_return(ctx.local_state, opposite_match);
            opposite_match = NULL;

            match_res = (ctx.with_stats ? make_match_with_stats(env, match_ref, opposite_value,
                                                                ctx.enqueue_ts)
                                        : make_match(env, match_ref, opposite_value));
            enif_consume_timeslice(env, 100);
        }
    }
    else if (out.consume_slot) {
        assert(out.batch != NULL);
        batch_consume_local_slot(ctx.broker, ctx.local_state, out.batch);
    }

    return match_res;
}
/*********************************************************************/


static ErlNifFunc nif_funcs[] = {
    {"new", 0, nif_new}
};

ERL_NIF_INIT(cbroker_nif3, nif_funcs, on_load, NULL, NULL, NULL);

