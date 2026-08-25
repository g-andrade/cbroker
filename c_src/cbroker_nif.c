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
    X(_cancelled,            "cancelled") \
    X(_delayed_match,        "delayed_match") \
    X(_empty,                "empty")  \
    X(_full,                 "full")  \
    X(_instant_match_first,  "instant_match_first") \
    X(_instant_match_second, "instant_match_second") \
    X(_left,                 "left")  \
    X(_match,                "match") \
    X(_matched,              "matched") \
    X(_none,                 "none") \
    X(_ok,                   "ok")  \
    X(_retry,                "retry") \
    X(_right,                "right") \
    X(_todo,                 "todo") \
    X(_too_late,             "too_late") \
    X(_true,                 "true")


//#define BATCH_NR_OF_CELLS 128

#define MAX(a, b) ((a) >= (b) ? (a) : (b))
#define MIN(a, b) ((a) <= (b) ? (a) : (b))

//#define BATCH_ARRAY_INITIAL_CAPACITY 8

#define LOG(fmt, ...) enif_fprintf(stderr, fmt "\n\r", __VA_ARGS__); fflush(stderr)

/*********************************************************************/


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
    ErlNifEnv* env;
    ERL_NIF_TERM cmonitor_term;
    ERL_NIF_TERM exchange_value;
} match_t;

//

typedef struct {
    _Atomic(ERL_NIF_TERM) status;
    _Atomic(void*) match;
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
    cbroker_omap_t* batches;
    // batch_id_t min_id;
} global_state_t;

//

typedef struct {
    cbroker_omap_t* batches;
    batch_id_t left_id;
    batch_id_t right_id;
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
    ErlNifPid other_pid;
    match_t* first_match;
    match_t* second_match;
} ask_out_t;

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

static ERL_NIF_TERM make_ticket_simple(ErlNifEnv* env, ERL_NIF_TERM side, 
                                       batch_id_t batch_id, offset_t offset)
{
    return enif_make_tuple3(
        env,
        side,
        enif_make_uint64(env, batch_id),
        enif_make_uint64(env, offset)
    );
}

static ERL_NIF_TERM make_ticket(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset) {
    return make_ticket_simple(
        ctx->env, 
        ctx->side,
        batch_id,
        offset
    );
}

static int get_ticket(ErlNifEnv* env, ERL_NIF_TERM term, 
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

static ERL_NIF_TERM make_await(ErlNifEnv* env, ERL_NIF_TERM ticket) {
    return enif_make_tuple2(env, Atoms._await, ticket);
}

static ERL_NIF_TERM make_match_msg(ErlNifEnv* env, ERL_NIF_TERM ticket, ERL_NIF_TERM exchange_value) {
    return enif_make_tuple2(env, ticket, enif_make_tuple2(env, Atoms._match, exchange_value));
}

//static int get_batch(ErlNifEnv* env, ERL_NIF_TERM term, batch_t** out) {
//    return enif_get_resource(env, term, ResourceTypes.batch, (void**) out);
//}

/*********************************************************************/


static match_t* match_new(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset) {
    cmonitor_t* cmonitor = (cmonitor_t*) enif_alloc_resource(ResourceTypes.cmonitor, sizeof(cmonitor_t));
    memset(cmonitor, 0, sizeof(cmonitor_t));

    cmonitor->env = enif_alloc_env();
    cmonitor->broker_term = enif_make_copy(cmonitor->env, ctx->broker_term);
    cmonitor->batch_id = batch_id;
    cmonitor->offset = offset;

    if (enif_monitor_process(ctx->env, cmonitor, &ctx->self, &cmonitor->mon)) {
        enif_free_env(cmonitor->env);
        cmonitor->env = NULL;
        enif_release_resource(cmonitor);
        return NULL;
    }

    match_t* match = enif_alloc(sizeof(match_t));
    memset(match, 0, sizeof(match_t));

    ErlNifEnv* match_env = enif_alloc_env();
    match->env = match_env;
    match->cmonitor_term = enif_make_resource(match_env, cmonitor);
    match->exchange_value = enif_make_copy(match_env, ctx->exchange_value);

    enif_release_resource(cmonitor);

    return match;
}

static void match_cancel_and_free(ErlNifEnv* caller_env, match_t* match) {
    cmonitor_t* cmonitor = NULL;
    assert(get_cmonitor(caller_env, match->cmonitor_term, &cmonitor));
    enif_demonitor_process(caller_env, cmonitor, &cmonitor->mon);

    enif_free_env(match->env);
    enif_free(match);
}

static void notify_other_of_match(ask_ctx_t* ctx, ask_out_t* success) {
    ERL_NIF_TERM other_side = (ctx->is_left ? Atoms._right : Atoms._left);
    ErlNifEnv* other_env = NULL;
    ERL_NIF_TERM copied_exchange_value;

    match_t* first_match = success->first_match;

    if (first_match == NULL) {
        other_env = enif_alloc_env();
        copied_exchange_value = enif_make_copy(other_env, ctx->exchange_value);
    } else {
        // We can reuse the env in first_match
        other_env = first_match->env;
        copied_exchange_value = first_match->exchange_value;

        cmonitor_t* cmonitor = NULL;
        assert(get_cmonitor(ctx->env, first_match->cmonitor_term, &cmonitor));
        enif_demonitor_process(ctx->env, cmonitor, &cmonitor->mon);
    }

    ERL_NIF_TERM other_ticket = make_ticket_simple(other_env, other_side, 
                                                   success->batch->id, success->offset);

    ERL_NIF_TERM other_msg = make_match_msg(other_env, other_ticket, copied_exchange_value);
    enif_send(ctx->env, &success->other_pid, other_env, other_msg);
    enif_free_env(other_env);

    if (first_match != NULL) {
        enif_free(first_match);
    }
}

static batch_t* batch_new(const batch_id_t id, const size_t nr_of_cells) {
    const size_t size = sizeof(batch_t) + (nr_of_cells * sizeof(cell_t));

    batch_t* batch = enif_alloc(size);
    assert(batch != NULL);
    memset(batch, 0, size);

    batch->id = id;
    batch->nr_of_cells = nr_of_cells;
    
    // TODO optimize initialization
    for (int i=0; i<nr_of_cells; i++) {
        cell_t* cell = &batch->cells[i];
        cell->status = Atoms._empty;
    }

    return batch;
}

///////////////////////////

static ERL_NIF_TERM batch_offset_ask(ask_ctx_t* ctx, batch_t* batch, offset_t offset, ask_out_t* out) {
    size_t cell_offset = offset % batch->nr_of_cells;
    
    cell_t* cell = &batch->cells[cell_offset];

    ERL_NIF_TERM status = Atoms._empty;
    ErlNifPid other_pid;

    if (atomic_compare_exchange_strong(&cell->status, &status, ctx->self_term)) {
        void* match = NULL;
        match_t* pending_match = match_new(ctx, batch->id, offset);
        LOG("match value is: %p", pending_match);

        if (pending_match == NULL) {
            // Caller stopped in the mean time
            return Atoms._cancelled;
        }
        else if (atomic_compare_exchange_strong(&cell->match, &match, pending_match)) {
            // Enqueued
            LOG("enqueued!! %s", "");
            return Atoms._await;
        } 
        else {
            assert(enif_get_local_pid(ctx->env, (ERL_NIF_TERM) match, &other_pid));

            if (enif_compare_pids(&other_pid, &ctx->self) == 0) {
                // Monitor triggered concurrently, we're cancelled
                match_cancel_and_free(ctx->env, pending_match);
                return Atoms._cancelled;
            } else {
                // Second in instant match - the other party will message us
                out->other_pid = other_pid;
                out->first_match = pending_match;
                return Atoms._instant_match_second;
            }
        }
    } else if (
        enif_get_local_pid(ctx->env, status, &other_pid)
        && atomic_compare_exchange_strong(&cell->status, &status, Atoms._matched)
    ) {
        LOG("matched with: %T", enif_make_pid(ctx->env, &other_pid));
        void* desired_match = (void*) ctx->self_term; // FIXME: this is highly questionable
        void* match = atomic_exchange(&cell->match, desired_match);
        LOG("match value is: %p", match);

        if (match == NULL) {
            // First in instant match - the other party will message us
            out->other_pid = other_pid;
            return Atoms._instant_match_first;
        } else {
            /* Delayed Match
             * 1) message `other_pid` with our exchange term
             * 2) return the first exchange term
             */
            out->other_pid = other_pid;
            out->second_match = (match_t*) match;
            return Atoms._delayed_match;
        }
    }

    assert(status == Atoms._cancelled);
    return Atoms._cancelled;
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
        return Atoms._full;
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

    if (! cbroker_omap_next(local_state->batches, prev_batch_id, &next_batch_id, (void**) &next_batch)) {
        broker_t* broker = ctx->broker;
        global_state_t* global_state = &broker->global_state;
        enif_mutex_lock(broker->global_lock);

        if (! cbroker_omap_next(global_state->batches, prev_batch_id, &next_batch_id, (void**) &next_batch)) {
            next_batch_id = prev_batch_id + 1;
            next_batch = batch_new(next_batch_id, broker->nr_of_cells_per_batch);
            assert(
                cbroker_omap_insert(global_state->batches, next_batch_id, next_batch)
                == CBROKER_OMAP_OK
            );
        }

        atomic_fetch_add_explicit(&next_batch->ref_count, 1, memory_order_relaxed);
        enif_mutex_unlock(broker->global_lock);

        assert(
            cbroker_omap_insert(local_state->batches, next_batch_id, next_batch)
            == CBROKER_OMAP_OK
        );
    }

    if (ctx->is_left) {
        local_state->left_id = next_batch_id;
    } else {
        local_state->right_id = next_batch_id;
    }

    return next_batch;
}

////

static ERL_NIF_TERM ask(ask_ctx_t* ctx, ask_out_t* out) {
    local_state_t* local_state = ctx->local_state;
    batch_id_t batch_id = (ctx->is_left ? local_state->left_id : local_state->right_id);
    batch_t* batch = NULL;

    ////

    const int max_attempts = MIN(100, MAX(5, ctx->broker->nr_of_cells_per_batch / 2));

    for (int attempt_nr = 0; attempt_nr < max_attempts; attempt_nr++) {
        if (batch == NULL) {
            cbroker_omap_lookup(local_state->batches, batch_id, (void**) &batch);
        }

        if (batch != NULL) {
            ERL_NIF_TERM match_res = batch_ask(ctx, batch, out);

            if (match_res == Atoms._full) {
                batch = NULL;
            } else if (match_res == Atoms._cancelled) {
                continue;
            } else {
                out->batch = batch;
                return match_res;
            }
        }

        batch = get_next_batch(ctx, batch_id);
    } 

    return Atoms._retry;
}

///////////////////////////

static void lower_refcount_and_remove_from_global_if_needed(broker_t* broker, batch_t* batch) {
    batch_id_t batch_id = batch->id;
    size_t ref_count = atomic_fetch_sub_explicit(&batch->ref_count, 1, memory_order_relaxed) - 1;
    assert(ref_count >= 1);

    if (ref_count == 1) {
        global_state_t* global_state = &broker->global_state;
        enif_mutex_lock(broker->global_lock);

        if (cbroker_omap_lookup(global_state->batches, batch_id, (void**) &batch)) {
            ref_count = atomic_load(&batch->ref_count);
            assert(ref_count >= 1);

            if (ref_count == 1) {
                cbroker_omap_delete_and_next(global_state->batches, batch_id, NULL, NULL, NULL);
                LOG("CONSUME: batch %u deleted", batch_id);
                enif_free(batch);
            }
        }

        enif_mutex_unlock(broker->global_lock);
    } 
}


static bool consume_batch_slot(broker_t* broker, local_state_t* local_state, batch_t* batch, bool present_in_local) {
    size_t consumed_count = 1 + atomic_fetch_add_explicit(&batch->consumed_count, 1, memory_order_relaxed);

    if (consumed_count < batch->nr_of_cells) {
        return false;
    }
    else {
        assert(consumed_count == batch->nr_of_cells);

        if (present_in_local) {
            assert(cbroker_omap_delete_and_next(local_state->batches, batch->id, NULL, NULL, NULL));
        }

        lower_refcount_and_remove_from_global_if_needed(broker, batch);

        return true;
    }
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
    memset(broker, 0, sizeof(broker_t));

    broker->nr_of_schedulers = nr_of_schedulers;

    //broker->nr_of_cells_per_batch = 4 * nr_of_schedulers;
    broker->nr_of_cells_per_batch = 1; // FIXME

    broker->global_lock = enif_mutex_create("cbroker.mutex");

    broker->global_state.batches = cbroker_omap_new();
    assert(broker->global_state.batches != NULL);

    const batch_id_t first_batch_id = 1;
    batch_t* first_batch = batch_new(first_batch_id, broker->nr_of_cells_per_batch);

    assert(
        cbroker_omap_insert(broker->global_state.batches, first_batch_id, first_batch)
        == CBROKER_OMAP_OK
    );
    atomic_fetch_add(&first_batch->ref_count, 1);

    
    for (size_t i=0; i < nr_of_schedulers; i++) {
        // TODO only initialize local state for the amount of _online_ schedulers.
        local_state_t* local_state = &broker->local_states[i];
        local_state->batches = cbroker_omap_new();
        assert(local_state->batches != NULL);

        assert(
            cbroker_omap_insert(local_state->batches, first_batch_id, first_batch)
            == CBROKER_OMAP_OK
        );
        atomic_fetch_add(&first_batch->ref_count, 1);

        local_state->left_id = first_batch_id;
        local_state->right_id = first_batch_id;
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

    ask_out_t success;
    memset(&success, 0, sizeof(ask_out_t));

    ERL_NIF_TERM match_res = ask(&ctx, &success);

    ////

    if (match_res == Atoms._await) {
        ERL_NIF_TERM ticket = make_ticket(&ctx, success.batch->id, success.offset);
        return make_await(env, ticket);
    }
    else if (match_res == Atoms._instant_match_first) {
        // The other party will message us
        batch_id_t batch_id = success.batch->id;

        notify_other_of_match(&ctx, &success);
        consume_batch_slot(ctx.broker, ctx.local_state, success.batch, true);

        ERL_NIF_TERM ticket = make_ticket(&ctx, batch_id, success.offset);
        return make_await(env, ticket);
    }
    else if (match_res == Atoms._instant_match_second) {
        // The other party will message us
        batch_id_t batch_id = success.batch->id;

        notify_other_of_match(&ctx, &success);

        ERL_NIF_TERM ticket = make_ticket(&ctx, batch_id, success.offset);
        return make_await(env, ticket);
    }
    else if (match_res == Atoms._delayed_match) {
        batch_id_t batch_id = success.batch->id;
        match_t* second_match = success.second_match;
        assert(second_match != NULL);

        LOG("dmatch: about to notify other %s", "");
        notify_other_of_match(&ctx, &success);

        LOG("dmatch: about to consume slot %s", "");
        consume_batch_slot(ctx.broker, ctx.local_state, success.batch, true);

        //
        LOG("dmatch: about to create ticket %s", "");
        ERL_NIF_TERM ticket = make_ticket(&ctx, batch_id, success.offset);
        LOG("dmatch: about to copy exchange value from %p", second_match);
        ERL_NIF_TERM copied_exchange_value = enif_make_copy(env, second_match->exchange_value);

        if (second_match->env != NULL) {
            LOG("dmatch: about to free second_match env %s", "");
            enif_free_env(second_match->env);
        }
        LOG("dmatch: about to free second_match %s", "");
        enif_free(second_match);

        if (is_fully_async) {
            LOG("dmatch: about to send msg to self %s", "");
            ERL_NIF_TERM self_msg = make_match_msg(env, ticket, copied_exchange_value);
            enif_send(env, &ctx.self, NULL, self_msg);
            return make_await(env, ticket);
        }
        else {
            return enif_make_tuple3(env, Atoms._match, ticket, copied_exchange_value);
        }
    }

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
    ERL_NIF_TERM self_term;

    LOG("cancel: get broker %s", "");
    if (! get_broker(env, argv[0], &broker)) {
        return make_badarg(env, argv[0]);
    }

    LOG("cancel: get ticket %s", "");
    if (! get_ticket(env, argv[1], &side, &batch_id, &offset)) {
        return make_badarg(env, argv[1]);
    }

    LOG("cancel: get self %s", "");
    if (enif_self(env, &self)) {
        self_term = enif_make_pid(env, &self);
    } else {
        return enif_make_badarg(env);
    }

    thread_id_t thread_id = get_or_assign_thread_id();
    LOG("cancel: thread id=%u", thread_id);
    local_state_t* local_state = &broker->local_states[thread_id];

    //

    batch_t* batch = NULL;
    bool present_in_local = false;
    bool was_consumed = false;

    LOG("cancel: looking up batch %u in local state", batch_id);
    present_in_local = cbroker_omap_lookup(local_state->batches, batch_id, (void**) &batch);

    if (! present_in_local) {
        LOG("cancel: looking up batch %u in global state", batch_id);
        global_state_t* global_state = &broker->global_state;
        enif_mutex_lock(broker->global_lock);
        if (cbroker_omap_lookup(global_state->batches, batch_id, (void**) &batch)) {
            atomic_fetch_add_explicit(&batch->ref_count, 1, memory_order_relaxed);
        }
        enif_mutex_unlock(broker->global_lock);
    }

    if (batch != NULL) {
        LOG("cancel: batch %u found, cell at offset %u", batch_id, offset);
        cell_t* cell = &batch->cells[offset];

        ERL_NIF_TERM status = self_term;
        ERL_NIF_TERM cancel_res;

        if (atomic_compare_exchange_strong(&cell->status, &status, Atoms._cancelled)) {
            match_t* match = atomic_exchange(&cell->match, NULL);
            assert(match != NULL);
            match_cancel_and_free(env, match);

            was_consumed = consume_batch_slot(broker, local_state, batch, present_in_local);
            cancel_res = Atoms._cancelled;
        } 
        else if (status == Atoms._cancelled) {
            cancel_res = Atoms._cancelled;
        } 
        else {
            cancel_res = Atoms._too_late;
        }

        if (!was_consumed && !present_in_local) {
            lower_refcount_and_remove_from_global_if_needed(broker, batch);
        }

        return cancel_res;
    }

    return Atoms._too_late;

        

}

//

static ErlNifFunc nif_funcs[] = {
    {"new", 0, niff_new},
    {"ask", 3, niff_ask},
    {"ask", 4, niff_ask},
    {"cancel", 2, niff_cancel}
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
    assert(get_broker(cmonitor->env, cmonitor->broker_term, &broker));

    thread_id_t thread_id = get_or_assign_thread_id();
    LOG("cdown: thread id=%u", thread_id);
    local_state_t* local_state = &broker->local_states[thread_id];

    batch_t* batch = NULL;
    bool present_in_local = false;

    LOG("cdown: looking up batch %u", batch_id);
    if (cbroker_omap_lookup(local_state->batches, batch_id, (void**) &batch)) {
        present_in_local = true;
    } else {
        LOG("cdown: looking up batch %u in global state", batch_id);
        global_state_t* global_state = &broker->global_state;
        enif_mutex_lock(broker->global_lock);
        if (cbroker_omap_lookup(global_state->batches, batch_id, (void**) &batch)) {
            atomic_fetch_add_explicit(&batch->ref_count, 1, memory_order_relaxed);
        }
        enif_mutex_unlock(broker->global_lock);
    }

    LOG("cdown: cell offset is %u", offset);
    cell_t* cell = &batch->cells[offset];
    ERL_NIF_TERM status = enif_make_pid(caller_env, pid);
    ERL_NIF_TERM pid_term = enif_make_pid(caller_env, pid);
    bool was_consumed = false;

    if (atomic_compare_exchange_strong(&cell->status, &status, Atoms._cancelled)) {
        LOG("cdown: cancelled %s", "");
        /* We set the match to self in case the enqueuing NIF call still hasn't
         * written `match_t` to status; this will signal it that it needs to free
         * the match.
         */
        void *match = NULL;
        atomic_compare_exchange_strong(&cell->match, &match, (match_t *)pid_term);

        was_consumed = consume_batch_slot(broker, local_state, batch, present_in_local);
    }

    if (!was_consumed && !present_in_local) {
        lower_refcount_and_remove_from_global_if_needed(broker, batch);
    }

    //

    if (cmonitor->env != NULL) {
        enif_free_env(cmonitor->env);
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
