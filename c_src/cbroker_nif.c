#include "erl_nif.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "cbroker_omap.h"

/*********************************************************************/

#define ATOM_LIST \
    X(_await,                "await") \
    X(_badarg,               "badarg") \
    X(_batch_consumed,       "batch_consumed") \
    X(_batch_full,           "batch_full")  \
    X(_cancelled,            "cancelled") \
    X(_delayed_match,        "delayed_match") \
    X(_empty,                "empty")  \
    X(_instant_match_first,  "instant_match_first") \
    X(_instant_match_second, "instant_match_second") \
    X(_left,                 "left")  \
    X(_match,                "match") \
    X(_matched,              "matched") \
    X(_none,                 "none") \
    X(_ok,                   "ok")  \
    X(_retry,                "retry") \
    X(_right,                "right") \
    X(_self_stopped,         "self_stopped") \
    X(_todo,                 "todo") \
    X(_too_late,             "too_late") \
    X(_true,                 "true")


#define MAX(a, b) ((a) >= (b) ? (a) : (b))
#define MIN(a, b) ((a) <= (b) ? (a) : (b))

//#define LOG(fmt, ...) enif_fprintf(stderr, fmt "\n\r", __VA_ARGS__); fflush(stderr)
#define LOG(fmt, ...)

#define LOG2(fmt, ...) enif_fprintf(stderr, fmt "\n\r", __VA_ARGS__); fflush(stderr)

/*********************************************************************/

#define CELL_COUNT_CANCELLED -10

/*********************************************************************/

#define X(field, name) ERL_NIF_TERM field;
static struct {
    ATOM_LIST
} Atoms;
#undef X

//

static struct {
    ErlNifResourceType* broker;
    ErlNifResourceType* cmonitor;
} ResourceTypes;

typedef ssize_t thread_id_t;
static _Atomic(thread_id_t) next_thread_id = 0;
static _Thread_local thread_id_t my_thread_id = -1;

//

typedef uint_fast64_t offset_t;
typedef _Atomic(offset_t) atomic_offset_t;

typedef offset_t batch_id_t;

//

typedef struct {
    ErlNifPid pid;
    ErlNifEnv* env;
    ERL_NIF_TERM cmonitor_term;
    ERL_NIF_TERM exchange_value;
} match_t;

//

typedef int_fast8_t cell_count_t;

typedef struct {
    _Atomic(cell_count_t) count;
    _Atomic(match_t*) match;
} cell_t;

//

typedef struct {
    ErlNifMonitor mon;
    ErlNifEnv* env;
    ERL_NIF_TERM broker_term;
    batch_id_t batch_id;
    offset_t offset;
} cmonitor_t;

//

typedef struct {
    batch_id_t id;
    atomic_size_t ref_count;
    atomic_size_t consumed_count;
    atomic_offset_t left_count;
    atomic_offset_t right_count;
    size_t nr_of_cells;
    cell_t cells[];
} batch_t;
 
//

typedef struct {
    batch_t* batch;
    bool found_locally;
} batch_handle_t;

//

typedef struct {
    cbroker_omap_t* batches;
    // batch_id_t min_id;
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
    cbroker_omap_t* batches;
    batch_id_t left_id;
    batch_id_t right_id;
    mempool_t env_pool;
    mempool_t match_pool;
} local_state_t;

//

typedef struct {
    size_t nr_of_schedulers;
    size_t nr_of_cells_per_batch;
    //
    ErlNifMutex* global_lock;
    global_state_t global_state;
    //
    local_state_t local_states[];
} broker_t;

//

typedef struct {
    ErlNifEnv* env;
    ERL_NIF_TERM broker_term;
    broker_t* broker;
    ERL_NIF_TERM side;
    bool is_left;
    local_state_t* local_state;
    //
    ErlNifPid self;
    ERL_NIF_TERM self_term;
    ERL_NIF_TERM exchange_value;
} ask_ctx_t;

//

typedef struct {
    batch_t* batch;
    offset_t offset;
    bool consume_slot;
    match_t* our_match;
    match_t* opposite_match;
} ask_out_t;

/*********************************************************************/

static match_t sentinel_match_cancelled;

/*********************************************************************/

static const thread_id_t get_or_assign_thread_id() {
    if (my_thread_id == -1) {
        my_thread_id = atomic_fetch_add_explicit(&next_thread_id, 1, memory_order_relaxed);
    }
    assert(my_thread_id >= 0);
    return my_thread_id;
}

static size_t sizeof_broker(size_t nr_of_schedulers) {
    assert(nr_of_schedulers > 0);
    return sizeof(broker_t) + (nr_of_schedulers * sizeof(local_state_t));
}

/*********************************************************************/

static ERL_NIF_TERM make_badarg(ErlNifEnv* env, ERL_NIF_TERM term) {
    ERL_NIF_TERM reason = enif_make_tuple2(env, Atoms._badarg, term);
    return enif_raise_exception(env, reason);
}

static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out) {
    return enif_get_resource(env, term, ResourceTypes.broker, (void**) out);
}

static int get_cmonitor(ErlNifEnv* env, ERL_NIF_TERM term, cmonitor_t** out) {
    return enif_get_resource(env, term, ResourceTypes.cmonitor, (void**) out);
}

static ERL_NIF_TERM make_tag_simple(ErlNifEnv* env, ERL_NIF_TERM side, 
                                       batch_id_t batch_id, offset_t offset)
{
    return enif_make_tuple3(
        env,
        side,
        enif_make_uint64(env, batch_id),
        enif_make_uint64(env, offset)
    );
}

static ERL_NIF_TERM make_tag(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset) {
    return make_tag_simple(
        ctx->env, 
        ctx->side,
        batch_id,
        offset
    );
}

static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, 
                      ERL_NIF_TERM* out_side,
                      batch_id_t* out_batch_id, offset_t* out_offset)
{
    int arity = -1;
    const ERL_NIF_TERM* elements = NULL;
    batch_id_t batch_id;
    offset_t offset;

    LOG("ehhh %s", "");

    if (enif_get_tuple(env, term, &arity, &elements)
        && arity == 3
        && (elements[0] == Atoms._left || elements[0] == Atoms._right)
        && enif_get_uint64(env, elements[1], &batch_id)
        && enif_get_uint64(env, elements[2], &offset)
    ) {
        *out_side = elements[0];
        *out_batch_id = batch_id;
        *out_offset = offset;
        return 1;
    }
    else {
        return 0;
    }
}

static ERL_NIF_TERM make_await(ErlNifEnv* env, ERL_NIF_TERM tag) {
    return enif_make_tuple2(env, Atoms._await, tag);
}

static ERL_NIF_TERM make_match(ErlNifEnv* env, ERL_NIF_TERM match_ref, ERL_NIF_TERM exchange_value) {
    return enif_make_tuple3(env, Atoms._match, match_ref, exchange_value);
    //return enif_make_tuple2(env, Atoms._match, exchange_value);
}

//static ERL_NIF_TERM make_match_msg(ErlNifEnv* env, ERL_NIF_TERM tag, ERL_NIF_TERM exchange_value) {
//    return enif_make_tuple2(env, tag, enif_make_tuple2(env, Atoms._match, exchange_value));
//}

//static int get_batch(ErlNifEnv* env, ERL_NIF_TERM term, batch_t** out) {
//    return enif_get_resource(env, term, ResourceTypes.batch, (void**) out);
//}

/*********************************************************************/

static void* pool_get(mempool_t* pool) {
    if (pool->count == 0) {
        return pool->alloc_cb();
    }
    else {
        size_t count = --pool->count;
        void* obj = pool->array[count];

        if (count <= (pool->size >> 1) && count >= 8) {
            pool->size = pool->size >> 1;
            assert(pool->size >= count);
            pool->array = enif_realloc(pool->array, pool->size * sizeof(void*));
        }
        assert(pool->array != NULL);
        return obj;
    }
}

static void pool_return(mempool_t* pool, void* obj) {
    if (pool->count >= 2048) { // FIXME
        pool->free_cb(obj);
        return;
    }
    else if (pool->count == pool->size) {
        if (pool->size == 0) {
            pool->size = 4;
            pool->array = enif_alloc(pool->size * sizeof(void*));
        } else {
            pool->size *= 2;
            pool->array = enif_realloc(pool->array, pool->size * sizeof(void*));
        }
        assert(pool->array != NULL);
    }

    pool->clear_cb(obj);
    pool->array[pool->count++] = obj;
}

static void pool_init(mempool_t* pool) {
    const size_t initial_size = 0; // FIXME
    pool->size = initial_size;
    pool->array = enif_alloc(pool->size * sizeof(void*));

    pool->count = pool->size;
    for (size_t i = 0; i < initial_size; i++) {
        pool->array[i] = pool->alloc_cb();
    }
}

/*********************************************************************/

static ErlNifEnv* get_env(local_state_t* local_state) {
    ErlNifEnv* env = (ErlNifEnv*) pool_get(&local_state->env_pool);
    assert(env != NULL);
    return env;
}

static void return_env(local_state_t* local_state, ErlNifEnv* env) {
    assert(env != NULL);
    pool_return(&local_state->env_pool, env);
}

/*********************************************************************/

static match_t* get_match(local_state_t* local_state) {
    match_t* match = (match_t*) pool_get(&local_state->match_pool);
    assert(match != NULL);
    return match;
}

static void return_match(local_state_t* local_state, match_t* match) {
    assert(match != NULL);
    pool_return(&local_state->match_pool, match);
}

static void match_clear(match_t* match) {
    memset(match, 0, sizeof(match_t));
}

static void* match_alloc() {
    match_t* match = enif_alloc(sizeof(match_t));
    match_clear(match);
    return match;
}

/*********************************************************************/


static match_t* match_new_monitored(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset) {
    cmonitor_t* cmonitor = (cmonitor_t*) enif_alloc_resource(ResourceTypes.cmonitor, sizeof(cmonitor_t));
    memset(cmonitor, 0, sizeof(cmonitor_t));

    cmonitor->env = get_env(ctx->local_state);
    cmonitor->broker_term = enif_make_copy(cmonitor->env, ctx->broker_term);
    cmonitor->batch_id = batch_id;
    cmonitor->offset = offset;

    if (enif_monitor_process(ctx->env, cmonitor, &ctx->self, &cmonitor->mon)) {
        return_env(ctx->local_state, cmonitor->env);
        cmonitor->env = NULL;
        enif_release_resource(cmonitor);
        return NULL;
    }

    match_t* match = get_match(ctx->local_state);

    ErlNifEnv* match_env = get_env(ctx->local_state);
    match->pid = ctx->self;
    match->env = match_env;
    match->cmonitor_term = enif_make_resource(match_env, cmonitor);
    match->exchange_value = enif_make_copy(match_env, ctx->exchange_value);

    enif_release_resource(cmonitor);

    return match;
}

static match_t* match_new_unmonitored(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset) {
    match_t* match = get_match(ctx->local_state);
    memset(match, 0, sizeof(match_t));

    ErlNifEnv* match_env = get_env(ctx->local_state);
    match->pid = ctx->self;
    match->env = match_env;
    match->cmonitor_term = Atoms._none;
    match->exchange_value = enif_make_copy(match_env, ctx->exchange_value);

    return match;
}

static void match_demonitor_and_free(ErlNifEnv* caller_env, local_state_t* local_state, match_t** match_ptr) {
    match_t* match = *match_ptr;
    assert(match != NULL);

    cmonitor_t* cmonitor = NULL;
    
    if (get_cmonitor(caller_env, match->cmonitor_term, &cmonitor)) {
        enif_demonitor_process(caller_env, cmonitor, &cmonitor->mon);
    } else {
        assert(match->cmonitor_term == Atoms._none);
    }

    return_env(local_state, match->env);
    return_match(local_state, match);

    *match_ptr = NULL;
}

static void notify_of_cancellation(ErlNifEnv* env, local_state_t* local_state,
                                   ERL_NIF_TERM tag_term, match_t** match_in_cell_ptr) {
    match_t* match_in_cell = *match_in_cell_ptr;
    assert(match_in_cell != NULL);

    cmonitor_t* cmonitor = NULL;
    int get_monitor_res = get_cmonitor(env, match_in_cell->cmonitor_term, &cmonitor);
    assert(get_monitor_res);
    
    ErlNifEnv* match_env = match_in_cell->env;

    if (enif_demonitor_process(env, cmonitor, &cmonitor->mon) == 0) {
        ERL_NIF_TERM tag_copy = enif_make_copy(match_env, tag_term);
        ERL_NIF_TERM msg = enif_make_tuple2(match_env, tag_copy, Atoms._cancelled);

        enif_send(env, &match_in_cell->pid, match_env, msg);
    }

    return_env(local_state, match_env);
    return_match(local_state, match_in_cell);
    *match_in_cell_ptr = NULL;
}

static void notify_of_match(ask_ctx_t* ctx, ErlNifPid* pid, match_t** match_ptr, 
                            const ERL_NIF_TERM side, const batch_id_t batch_id, const offset_t offset,
                            const ERL_NIF_TERM match_ref) {
    ErlNifEnv* caller_env = ctx->env;
    local_state_t* local_state = ctx->local_state;

    match_t* match = *match_ptr;
    assert(match != NULL);

    ERL_NIF_TERM cmonitor_term = match->cmonitor_term;
    cmonitor_t* cmonitor = NULL;
    int get_monitor_res = get_cmonitor(caller_env, cmonitor_term, &cmonitor);

    // We can reuse the env for the message
    ErlNifEnv* match_env = match->env;
    ERL_NIF_TERM tag = make_tag_simple(match_env, side, batch_id, offset);
    ERL_NIF_TERM msg_match_ref = enif_make_copy(match_env, match_ref);
    ERL_NIF_TERM msg_content = make_match(match_env, msg_match_ref, match->exchange_value);
    ERL_NIF_TERM msg = enif_make_tuple2(match_env, tag, msg_content);

    enif_send(caller_env, pid, match_env, msg);

    return_env(local_state, match_env);
    return_match(local_state, match);

    if (get_monitor_res) {
        enif_demonitor_process(caller_env, cmonitor, &cmonitor->mon);
    } else {
        assert(cmonitor_term == Atoms._none);
    }

    *match_ptr = NULL;
}

static batch_t* batch_new(const batch_id_t id, const size_t nr_of_cells) {
    LOG("NEW BATCH!! %u", id);
    const size_t size = sizeof(batch_t) + (nr_of_cells * sizeof(cell_t));

    batch_t* batch = enif_alloc(size);
    assert(batch != NULL);
    memset(batch, 0, size);

    batch->id = id;
    batch->nr_of_cells = nr_of_cells;
    
    // for (int i=0; i<nr_of_cells; i++) {
    //     cell_t* cell = &batch->cells[i];
    //     cell->status = Atoms._empty;
    // }

    return batch;
}

///////////////////////////

static ERL_NIF_TERM make_opposite_side(ERL_NIF_TERM side) {
    if (side == Atoms._left) {
        return Atoms._right;
    } else {
        assert(side == Atoms._right);
        return Atoms._left;
    }
}

static ERL_NIF_TERM batch_offset_ask(ask_ctx_t* ctx, batch_t* batch, offset_t offset, ask_out_t* out) {
    size_t cell_offset = offset % batch->nr_of_cells;
    
    cell_t* cell = &batch->cells[cell_offset];

    cell_count_t cell_count = 1 + atomic_fetch_add_explicit(&cell->count, 1, memory_order_relaxed);

    if (cell_count == 1) {
        match_t* opposite_match = NULL;
        match_t* our_match = match_new_monitored(ctx, batch->id, offset);
        LOG("match value is: %p", our_match);

        if (our_match == NULL) {
            // Caller stopped in the mean time
            if (atomic_compare_exchange_strong(&cell->count, &cell_count, CELL_COUNT_CANCELLED)) {
                out->consume_slot = true;
                return Atoms._self_stopped;
            }  
            else if (cell_count < 0) {
                return Atoms._self_stopped;
            }

            // Too late
            assert(cell_count == 2);
            
            opposite_match = atomic_exchange(&cell->match, &sentinel_match_cancelled);

            if (opposite_match != NULL) {
                // The other side is already awaiting us; message it with the cancellation
                ErlNifEnv* tmp_env = get_env(ctx->local_state); // need a tmp env or we won't be able to send message
                ERL_NIF_TERM opposite_side = make_opposite_side(ctx->side);
                ERL_NIF_TERM tag = make_tag_simple(opposite_match->env, opposite_side, batch->id, offset);
                notify_of_cancellation(tmp_env, ctx->local_state, tag, &opposite_match);
                assert(opposite_match == NULL);
                return_env(ctx->local_state, tmp_env);
            }
            else {
                out->consume_slot = true;
            }

            return Atoms._self_stopped;
        }
        else if (atomic_compare_exchange_strong(&cell->match, &opposite_match, our_match)) {
            // Enqueued
            LOG("enqueued!! %s", "");
            return Atoms._await;
        } 
        else if (opposite_match == &sentinel_match_cancelled) {
            // Monitored triggered concurrently, we're cancelled
            our_match->cmonitor_term = Atoms._none;
            match_demonitor_and_free(ctx->env, ctx->local_state, &our_match);
            assert(our_match == NULL);
            return Atoms._self_stopped;
        }
        else {
            assert(opposite_match != NULL);
            match_t* match_in_cell = atomic_exchange(&cell->match, NULL);
            assert(match_in_cell == opposite_match);

            out->our_match = our_match;
            out->opposite_match = opposite_match;
            return Atoms._instant_match_second;
        }
    } else if (cell_count == 2) {
        match_t* our_match = match_new_unmonitored(ctx, batch->id, offset);
        match_t* opposite_match = atomic_exchange(&cell->match, our_match);

        if (opposite_match == NULL) {
            // The other side will message us and return our pending match to itself
            out->consume_slot = true;
            return Atoms._instant_match_first;
        }
        else if (opposite_match == &sentinel_match_cancelled) {
            match_t* match_in_cell = atomic_exchange(&cell->match, NULL);
            assert(match_in_cell == our_match);
            match_demonitor_and_free(ctx->env, ctx->local_state, &our_match);
            assert(our_match == NULL);
            return Atoms._cancelled;
        }
        else {
            /* Delayed Match
             * 1) message the other process with our exchange term
             * 2) return the first exchange term
             */

            match_t* match_in_cell = atomic_exchange(&cell->match, NULL);
            assert(match_in_cell == our_match);

            out->consume_slot = true;
            out->our_match = our_match;
            out->opposite_match = opposite_match;
            return Atoms._delayed_match;
        }
    }

    assert(cell_count < 0);
    return Atoms._cancelled;
}

////

static bool batch_lookup(broker_t* broker, local_state_t* local_state, batch_id_t batch_id, batch_handle_t* out_handle) {
    batch_t* batch = NULL;

    if (cbroker_omap_lookup(local_state->batches, batch_id, (void**) &batch)) {
        out_handle->batch = batch;
        out_handle->found_locally = true;
        return true;
    } 
    else {
        global_state_t* global_state = &broker->global_state;
        enif_mutex_lock(broker->global_lock);
        if (cbroker_omap_lookup(global_state->batches, batch_id, (void**) &batch)) {
            atomic_fetch_add_explicit(&batch->ref_count, 1, memory_order_relaxed);
            enif_mutex_unlock(broker->global_lock);
            out_handle->batch = batch;
            out_handle->found_locally = false;
            return true;
        }
        else {
            enif_mutex_unlock(broker->global_lock);
            return false;
        }
    }
}

////

static ERL_NIF_TERM batch_ask(ask_ctx_t* ctx, batch_t* batch, ask_out_t* out) {
    offset_t offset = 0;

    offset = (
        ctx->is_left ?
        atomic_fetch_add_explicit(&batch->left_count, 1, memory_order_relaxed)
        : atomic_fetch_add_explicit(&batch->right_count, 1, memory_order_relaxed)
    );

    if (offset >= batch->nr_of_cells) {
        if (atomic_load_explicit(&batch->consumed_count, memory_order_relaxed) >= batch->nr_of_cells) {
            return Atoms._batch_consumed;
        }
        return Atoms._batch_full;
    }
    else {
        ERL_NIF_TERM match_res = batch_offset_ask(ctx, batch, offset, out);

        if (match_res == Atoms._cancelled) {
            return match_res;
        } else {
            out->offset = offset;
            return match_res;
        }
    }
}

////

static batch_t* get_next_batch(ask_ctx_t* ctx, const batch_id_t prev_batch_id) {
    local_state_t* local_state = ctx->local_state;

    batch_id_t next_batch_id = 0;
    batch_t* next_batch = NULL;
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;

    if (! cbroker_omap_next(local_state->batches, prev_batch_id, &next_batch_id, (void**) &next_batch)) {
        broker_t* broker = ctx->broker;
        global_state_t* global_state = &broker->global_state;
        enif_mutex_lock(broker->global_lock);

        if (! cbroker_omap_next(global_state->batches, prev_batch_id, &next_batch_id, (void**) &next_batch)) {
            next_batch_id = prev_batch_id + 1;
            next_batch = batch_new(next_batch_id, broker->nr_of_cells_per_batch);
            map_res = cbroker_omap_insert(global_state->batches, next_batch_id, next_batch);
            assert(map_res == CBROKER_OMAP_OK);
            atomic_store_explicit(&next_batch->ref_count, 2, memory_order_relaxed);
        }
        else {
            size_t ref_count = 1 + atomic_fetch_add_explicit(&next_batch->ref_count, 1, memory_order_seq_cst);
            LOG("ASC||| REF COUNT for batch %u: %u", next_batch_id, ref_count);
            assert(ref_count >= 1);
        }

        enif_mutex_unlock(broker->global_lock);

        map_res = cbroker_omap_insert(local_state->batches, next_batch_id, next_batch);
        assert(map_res == CBROKER_OMAP_OK);
    }

    if (ctx->is_left) {
        local_state->left_id = next_batch_id;
    } else {
        local_state->right_id = next_batch_id;
    }

    return next_batch;
}

///////////////////////////

static void batch_lower_ref_count(broker_t* broker, local_state_t* local_state, batch_handle_t* handle) {
    batch_t* batch = handle->batch;
    assert(batch != NULL);

    batch_id_t batch_id = batch->id;
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;

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

        if (cbroker_omap_lookup(global_state->batches, batch_id, (void**) &batch)) {
            ref_count = atomic_load_explicit(&batch->ref_count, memory_order_acquire);
            assert(ref_count >= 1);

            if (ref_count == 1) {
                bool has_next = true;
                cbroker_omap_delete_and_next(global_state->batches, batch_id, &has_next, NULL, NULL);
                LOG("CONSUME: batch %u deleted", batch_id);
                memset(batch, 0, sizeof(batch_t) + (batch->nr_of_cells * sizeof(cell_t)));
                enif_free(batch);

                if (! has_next) {
                    // we create the next batch right away, ensuring batch IDs are not reused
                    // by a thread that lagged behind.
                    batch_id_t next_batch_id = batch_id + 1;
                    batch_t* next_batch = batch_new(next_batch_id, broker->nr_of_cells_per_batch);
                    map_res = cbroker_omap_insert(global_state->batches, next_batch_id, next_batch);
                    assert(map_res == CBROKER_OMAP_OK);
                    atomic_store_explicit(&next_batch->ref_count, 1, memory_order_relaxed);
                }
            }
        }

        enif_mutex_unlock(broker->global_lock);
    } 

    handle->batch = NULL;
}

////

static ERL_NIF_TERM ask(ask_ctx_t* ctx, ask_out_t* out) {
    local_state_t* local_state = ctx->local_state;
    batch_id_t batch_id = (ctx->is_left ? local_state->left_id : local_state->right_id);
    batch_t* batch = NULL;
    batch_t* skipped_batch = NULL;
    bool should_continue = true;

    ////

    const int max_attempts = MIN(100, MAX(5, ctx->broker->nr_of_cells_per_batch / 2));

    for (int attempt_nr = 0; (attempt_nr < max_attempts) && should_continue; attempt_nr++) {
        skipped_batch = NULL;
        if (batch == NULL) {
            cbroker_omap_lookup(local_state->batches, batch_id, (void**) &batch);
        }

        if (batch != NULL) {
            ERL_NIF_TERM match_res = batch_ask(ctx, batch, out);

            if (match_res == Atoms._batch_full) {
                skipped_batch = batch;
                batch = NULL;
            } 
            else if (match_res == Atoms._batch_consumed) {
                batch_handle_t handle;
                memset(&handle, 0, sizeof(batch_handle_t));
                handle.batch = batch;
                handle.found_locally = true;
                batch_lower_ref_count(ctx->broker, ctx->local_state, &handle);
                batch = NULL;
            }
            else if (match_res == Atoms._cancelled) {
                continue;
            } 
            else {
                out->batch = batch;
                return match_res;
            }
        }

        int timeslice_percent = 100 * attempt_nr / max_attempts;
        should_continue = !enif_consume_timeslice(ctx->env, timeslice_percent);

        batch = get_next_batch(ctx, batch_id);
        batch_id = batch->id;

        if (skipped_batch != NULL) {
            batch_id_t opposite_id = (ctx->is_left ? local_state->right_id : local_state->left_id);
            if (opposite_id > skipped_batch->id) {
                batch_handle_t skipped_batch_handle;
                memset(&skipped_batch_handle, 0, sizeof(batch_handle_t));
                skipped_batch_handle.batch = skipped_batch;
                skipped_batch_handle.found_locally = true;
                batch_lower_ref_count(ctx->broker, local_state, &skipped_batch_handle);
                skipped_batch = NULL;
            }
        }
    } 

    return Atoms._retry;
}


static void consume_batch_slot(broker_t* broker, local_state_t* local_state, batch_handle_t* handle) {
    batch_t* batch = handle->batch;

    /* acq_rel: the incrementer that reaches nr_of_cells triggers teardown, so
     * this behaves as a reference release. See `batch_lower_ref_count`. */
    size_t consumed_count = 1 + atomic_fetch_add_explicit(&batch->consumed_count, 1, memory_order_acq_rel);

    if (consumed_count < batch->nr_of_cells) {
        return;
    } else {
        assert(consumed_count == batch->nr_of_cells);
        batch_lower_ref_count(broker, local_state, handle);
    }
}

static void consume_local_batch_slot(broker_t* broker, local_state_t* local_state, batch_t* batch) {
    batch_handle_t handle;
    memset(&handle, 0, sizeof(batch_handle_t));

    handle.batch = batch;
    handle.found_locally = true;
    consume_batch_slot(broker, local_state, &handle);
}

/*********************************************************************/

static ERL_NIF_TERM
niff_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifPid self;
    if (! enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    ErlNifSysInfo sys_info;
    enif_system_info(&sys_info, sizeof(sys_info));
    size_t nr_of_schedulers = sys_info.scheduler_threads;
    assert(nr_of_schedulers > 0);

    const size_t broker_size = sizeof_broker(nr_of_schedulers);
    broker_t* broker = enif_alloc_resource(ResourceTypes.broker, broker_size);
    assert(broker != NULL);
    memset(broker, 0, broker_size);

    broker->nr_of_schedulers = nr_of_schedulers;
    broker->nr_of_cells_per_batch = 32 * nr_of_schedulers;
    broker->global_lock = enif_mutex_create("cbroker.mutex");

    broker->global_state.batches = cbroker_omap_new();
    assert(broker->global_state.batches != NULL);

    const batch_id_t first_batch_id = 1;
    batch_t* first_batch = batch_new(first_batch_id, broker->nr_of_cells_per_batch);

    cbroker_omap_result_t map_res = cbroker_omap_insert(broker->global_state.batches, first_batch_id, first_batch);
    assert(map_res == CBROKER_OMAP_OK);
    atomic_store_explicit(&first_batch->ref_count, 1, memory_order_relaxed);

    
    for (size_t i=0; i < nr_of_schedulers; i++) {
        // TODO only initialize local state for the amount of _online_ schedulers.
        local_state_t* local_state = &broker->local_states[i];
        local_state->batches = cbroker_omap_new();
        assert(local_state->batches != NULL);

        map_res = cbroker_omap_insert(local_state->batches, first_batch_id, first_batch);
        assert(map_res == CBROKER_OMAP_OK);
        atomic_fetch_add_explicit(&first_batch->ref_count, 1, memory_order_seq_cst);

        local_state->left_id = first_batch_id;
        local_state->right_id = first_batch_id;

        local_state->env_pool.alloc_cb = (void* (*)()) enif_alloc_env;
        local_state->env_pool.clear_cb = (void (*)(void*)) enif_clear_env;
        local_state->env_pool.free_cb = (void (*)(void*)) enif_free_env;
        pool_init(&local_state->env_pool);

        local_state->match_pool.alloc_cb = (void* (*)()) match_alloc;
        local_state->match_pool.clear_cb = (void (*)(void*)) match_clear;
        local_state->match_pool.free_cb = (void (*)(void*)) enif_free;
        pool_init(&local_state->match_pool);

    }

    return enif_make_resource(env, broker);
}

//

static ERL_NIF_TERM
niff_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ask_ctx_t ctx;
    memset(&ctx, 0, sizeof(ask_ctx_t));

    ctx.env = env;
    bool is_fully_async = false;

    ctx.broker_term = argv[0];
    ctx.side = argv[1];
    ctx.exchange_value = argv[2];

    if (! get_broker(env, ctx.broker_term, &ctx.broker)) {
        return make_badarg(env, ctx.broker_term);
    }

    if (enif_self(env, &ctx.self)) {
        ctx.self_term = enif_make_pid(env, &ctx.self);
    } else {
        return enif_make_badarg(env);
    }
        
    if (ctx.side == Atoms._left) {
        ctx.is_left = true;
    } else if (ctx.side != Atoms._right) {
        return make_badarg(env, ctx.side);
    }
    
    is_fully_async = (argc >= 4 && argv[3] == Atoms._true);

    ////////////////////////////

    thread_id_t thread_id = get_or_assign_thread_id();
    ctx.local_state = &ctx.broker->local_states[thread_id];
    assert(thread_id >= 0 && thread_id < ctx.broker->nr_of_schedulers);

    ask_out_t success;
    memset(&success, 0, sizeof(ask_out_t));

    ERL_NIF_TERM match_res = ask(&ctx, &success);

    ////

    if (match_res == Atoms._await) {
        ERL_NIF_TERM tag = make_tag(&ctx, success.batch->id, success.offset);
        match_res = make_await(env, tag);
    }
    else if (match_res == Atoms._instant_match_first) {
        // The other party will message us
        batch_id_t batch_id = success.batch->id;

        assert(success.consume_slot);
        consume_local_batch_slot(ctx.broker, ctx.local_state, success.batch);

        ERL_NIF_TERM tag = make_tag(&ctx, batch_id, success.offset);
        match_res = make_await(env, tag);
    }
    else if (match_res == Atoms._instant_match_second || match_res == Atoms._delayed_match) {
        match_t** our_match_ptr = &success.our_match;
        match_t** opposite_match_ptr = &success.opposite_match;
        const batch_id_t batch_id = success.batch->id;
        const offset_t offset = success.offset;

        assert(*our_match_ptr != NULL);
        assert(*opposite_match_ptr != NULL);

        ERL_NIF_TERM match_ref = enif_make_ref(env);

        LOG("dmatch: about to notify other %s", "");
        const ERL_NIF_TERM opposite_side = make_opposite_side(ctx.side);
        notify_of_match(&ctx, &((*opposite_match_ptr)->pid), our_match_ptr, opposite_side, batch_id, offset, match_ref);
        assert(success.our_match == NULL);

        if (success.consume_slot) {
            LOG("dmatch: about to consume slot %s", "");
            consume_local_batch_slot(ctx.broker, ctx.local_state, success.batch);
        }

        if (is_fully_async) {
            notify_of_match(&ctx, &ctx.self, opposite_match_ptr, ctx.side, batch_id, offset, match_ref);
            assert(success.opposite_match == NULL);
            ERL_NIF_TERM tag = make_tag(&ctx, batch_id, success.offset);
            match_res = make_await(env, tag);
        }
        else {
            ERL_NIF_TERM opposite_value = enif_make_copy(env, (*opposite_match_ptr)->exchange_value);
            match_demonitor_and_free(env, ctx.local_state, opposite_match_ptr);
            assert(success.opposite_match == NULL);
            match_res = make_match(env, match_ref, opposite_value);
        }
    } 
    else if (success.consume_slot) {
        assert(success.batch != NULL);
        consume_local_batch_slot(ctx.broker, ctx.local_state, success.batch);
    }

    //enif_consume_timeslice(env, 100);
    return match_res;
}

//

static ERL_NIF_TERM
niff_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    broker_t* broker = NULL;
    ERL_NIF_TERM side;
    batch_id_t batch_id;
    offset_t offset;
    ErlNifPid self;

    const ERL_NIF_TERM broker_term = argv[0];
    const ERL_NIF_TERM tag_term = argv[1];

    LOG("cancel: get broker %s", "");
    if (! get_broker(env, broker_term, &broker)) {
        return make_badarg(env, broker_term);
    }

    LOG("cancel: get tag %s", "");
    if (! get_tag(env, tag_term, &side, &batch_id, &offset)) {
        return make_badarg(env, tag_term);
    }

    LOG("cancel: get self %s", "");
    if (! enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    thread_id_t thread_id = get_or_assign_thread_id();
    LOG("cancel: thread id=%u", thread_id);
    assert(thread_id >= 0 && thread_id < broker->nr_of_schedulers);
    local_state_t* local_state = &broker->local_states[thread_id];

    //

    batch_handle_t handle;
    memset(&handle, 0, sizeof(batch_handle_t));

    if (! batch_lookup(broker, local_state, batch_id, &handle)) {
        return Atoms._too_late;
    }
    
    batch_t* batch = handle.batch;

    LOG("cancel: batch %u found, cell at offset %u", batch_id, offset);
    cell_t* cell = &batch->cells[offset];

    cell_count_t cell_count = 1;
    ERL_NIF_TERM cancel_res;

    if (atomic_compare_exchange_strong(&cell->count, &cell_count, CELL_COUNT_CANCELLED)) {
        match_t* match_in_cell = atomic_exchange(&cell->match, &sentinel_match_cancelled);
        assert(match_in_cell != NULL);

        if (enif_compare_pids(&match_in_cell->pid, &self) == 0) {
            match_demonitor_and_free(env, local_state, &match_in_cell);
        } else {
            notify_of_cancellation(env, local_state, tag_term, &match_in_cell);
        } 

        consume_batch_slot(broker, local_state, &handle);
        cancel_res = Atoms._cancelled;
    } 
    else if (cell_count < 0) {
        cancel_res = Atoms._cancelled;
    } 
    else {
        cancel_res = Atoms._too_late;
    }

    //

    if (!handle.found_locally && handle.batch != NULL) {
        batch_lower_ref_count(broker, local_state, &handle);
    }
    return cancel_res;
}

//

static ERL_NIF_TERM
niff_to_list(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    broker_t* broker = NULL;

    if (! get_broker(env, argv[0], &broker)) {
        return make_badarg(env, argv[0]);
    }

    global_state_t* global_state = &broker->global_state;
    enif_mutex_lock(broker->global_lock);

    size_t nr_of_batches = cbroker_omap_size(global_state->batches);
    ERL_NIF_TERM batch_terms[nr_of_batches];

    void** batches = cbroker_omap_values(global_state->batches);
    ERL_NIF_TERM empty_atom = enif_make_atom(env, "");
    ERL_NIF_TERM set_atom = enif_make_atom(env, "!!");
    ERL_NIF_TERM id_atom = enif_make_atom(env, "batch_id");
    ERL_NIF_TERM ref_count_atom = enif_make_atom(env, "ref_count");
    ERL_NIF_TERM consumed_atom = enif_make_atom(env, "consumed_count");
    ERL_NIF_TERM cells_atom = enif_make_atom(env, "cells");

    for (size_t i = 0; i < nr_of_batches; i++) {
        batch_t* batch = (batch_t*) batches[i];
        ERL_NIF_TERM cell_terms[batch->nr_of_cells];

        for (size_t j = 0; j < batch->nr_of_cells; j++) {
            cell_t* cell = &batch->cells[j];

            cell_count_t cell_count = atomic_load_explicit(&cell->count, memory_order_relaxed);
            ERL_NIF_TERM count_term = enif_make_int(env, cell_count);
            ERL_NIF_TERM match_term = (atomic_load_explicit(&cell->match, memory_order_relaxed) == NULL ? empty_atom : set_atom);
            cell_terms[j] = enif_make_tuple2(env, count_term, match_term);
        }

        ERL_NIF_TERM batch_kvlist[5];
        
        batch_kvlist[0] = enif_make_tuple2(env, id_atom, enif_make_uint64(env, batch->id));
        batch_kvlist[1] = enif_make_tuple2(env, ref_count_atom, enif_make_uint64(env, atomic_load_explicit(&batch->ref_count, memory_order_relaxed)));
        batch_kvlist[2] = enif_make_tuple2(env, consumed_atom, enif_make_uint64(env, atomic_load_explicit(&batch->consumed_count, memory_order_relaxed)));
        batch_kvlist[3] = enif_make_tuple2(env, cells_atom, enif_make_list_from_array(env, cell_terms, batch->nr_of_cells));
        batch_kvlist[4] = enif_make_atom(env, "------------------------");

        batch_terms[i] = enif_make_list_from_array(env, batch_kvlist, 5);
    }
    
    enif_mutex_unlock(broker->global_lock);

    return enif_make_list_from_array(env, batch_terms, nr_of_batches);
}

//

static ErlNifFunc nif_funcs[] = {
    {"new", 0, niff_new},
    {"ask", 3, niff_ask},
    {"ask", 4, niff_ask},
    {"cancel", 2, niff_cancel},
    {"to_list", 1, niff_to_list}
};

/*********************************************************************/

static void broker_stop(broker_t* broker) {
    global_state_t* global_state = &broker->global_state;
}

static void broker_dtor(ErlNifEnv* caller_env, void* obj) {
    broker_t* broker = (broker_t*) obj;
    broker_stop(broker);

    //enif_mutex_lock(broker->global_lock);

    //memset(obj, 0, sizeof(broker_t));
}

static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon) {
    broker_t* broker = (broker_t*) obj;
    broker_stop(broker);

//void cbroker_omap_destroy(cbroker_omap_t* map,
//                          void (*free_value)(uint64_t key, void* value, void* ctx),
//                          void* ctx);

}

/*********************************************************************/

static void cmonitor_dtor(ErlNifEnv* caller_env, void* obj) {
    cmonitor_t* cmonitor = (cmonitor_t*) obj;

    if (cmonitor->env != NULL) {
        enif_free_env(cmonitor->env);
    }
}

static void cmonitor_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon) {
    LOG("cdown: cmonitor=%p", obj);
    cmonitor_t* cmonitor = (cmonitor_t*) obj;
    batch_id_t batch_id = cmonitor->batch_id;
    offset_t offset = cmonitor->offset;

    LOG("cdown: get broker %llu", cmonitor->broker_term);
    broker_t* broker = NULL;
    int get_broker_res = get_broker(cmonitor->env, cmonitor->broker_term, &broker);
    assert(get_broker_res);

    thread_id_t thread_id = get_or_assign_thread_id();
    LOG("cdown: thread id=%u", thread_id);
    assert(thread_id >= 0 && thread_id < broker->nr_of_schedulers);
    local_state_t* local_state = &broker->local_states[thread_id];

    //

    batch_handle_t handle;
    memset(&handle, 0, sizeof(batch_handle_t));

    if (! batch_lookup(broker, local_state, batch_id, &handle)) {
        // too late
        return;
    }

    batch_t* batch = handle.batch;

    LOG("cdown: cell offset is %u", offset);
    cell_t* cell = &batch->cells[offset];

    cell_count_t cell_count = 1;

    if (atomic_compare_exchange_strong(&cell->count, &cell_count, -10)) {
        LOG("cdown: cancelled %s", "");
        match_t *match_in_cell = atomic_exchange(&cell->match, &sentinel_match_cancelled);

        if (match_in_cell != NULL) {
            return_env(local_state, match_in_cell->env);
            return_match(local_state, match_in_cell);
        }

        consume_batch_slot(broker, local_state, &handle);
    }

    if (!handle.found_locally && handle.batch != NULL) {
        batch_lower_ref_count(broker, local_state, &handle);
    }

    //

    if (cmonitor->env != NULL) {
        return_env(local_state, cmonitor->env);
        cmonitor->env = NULL;
    }
}

///////////////

/*********************************************************************/

static void init_atoms(ErlNifEnv* caller_env) {
    memset(&Atoms, 0, sizeof(Atoms));
    #define X(field, name) Atoms.field = enif_make_atom(caller_env, name);
    ATOM_LIST
    #undef X
}

static void broker_resource_load(ErlNifEnv* caller_env) {
    ErlNifResourceTypeInit callbacks = {broker_dtor, NULL, broker_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.broker = enif_init_resource_type(
        caller_env, 
        "cbroker",
        &callbacks,
        flags,
        &flags
    );
    assert(ResourceTypes.broker != NULL);
}

static void cmonitor_resource_load(ErlNifEnv* caller_env) {
    ErlNifResourceTypeInit callbacks = {cmonitor_dtor, NULL, cmonitor_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.cmonitor = enif_init_resource_type(
        caller_env, 
        "cbroker.cmonitor",
        &callbacks,
        flags,
        &flags
    );
    assert(ResourceTypes.cmonitor != NULL);
}

static int nif_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info) {
    init_atoms(caller_env);

    memset(&ResourceTypes, 0, sizeof(ResourceTypes));
    broker_resource_load(caller_env);
    cmonitor_resource_load(caller_env);

    return 0;
}

ERL_NIF_INIT(cbroker_nif, nif_funcs, nif_load, NULL, NULL, NULL);
