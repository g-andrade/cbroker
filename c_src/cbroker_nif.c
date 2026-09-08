#include "erl_nif.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include "cbroker_omap.h"

/*********************************************************************/

/* The columns below are aligned on purpose. */
/* clang-format off */
#define ATOM_LIST \
    X(_about_to_sleep,       "about_to_sleep") \
    X(_active,               "active") \
    X(_await,                "await") \
    X(_badarg,               "badarg") \
    X(_batch_consumed,       "batch_consumed") \
    X(_batch_full,           "batch_full")  \
    X(_cancelled,            "cancelled") \
    X(_closed,               "closed") \
    X(_delayed_match,        "delayed_match") \
    X(_depends_on_creator,   "depends_on_creator") \
    X(_empty,                "empty") \
    X(_error,                "error") \
    X(_false,                "false") \
    X(_fully_async,          "fully_async") \
    X(_instant_match_first,  "instant_match_first") \
    X(_instant_match_second, "instant_match_second") \
    X(_left,                 "left")  \
    X(_match,                "match") \
    X(_matched,              "matched") \
    X(_nb,                   "nb") \
    X(_none,                 "none") \
    X(_ok,                   "ok") \
    X(_regular,              "regular") \
    X(_retry,                "retry") \
    X(_right,                "right") \
    X(_self_stopped,         "self_stopped") \
    X(_stopped,              "stopped") \
    X(_too_late,             "too_late") \
    X(_true,                 "true") \
    X(_wake_up,              "wake_up")
/* clang-format on */

#define MAX(a, b) ((a) >= (b) ? (a) : (b))
#define MIN(a, b) ((a) <= (b) ? (a) : (b))

#define LOG(fmt, ...)
// #define LOG(fmt, ...) do { \
//     enif_fprintf(stderr, fmt "\n\r", ##__VA_ARGS__); \
//      fflush(stderr); \
// } while (0)

/*********************************************************************/

#define CELL_COUNT_CANCELLED -128

/* Determined very informally, can probably be optimized (and different between
 * match and envs pools)
 */
#define TARGET_MEMPOOL_SIZE 8

/*********************************************************************/

//

typedef uint_fast64_t offset_t;
typedef _Atomic(offset_t) atomic_offset_t;

typedef offset_t batch_id_t;

//

typedef struct {
    bool depends_on_creator;
} broker_opts_t;

//

typedef struct {
    ERL_NIF_TERM side;
    ErlNifPid pid;
    ErlNifEnv* env;
    ERL_NIF_TERM cmonitor_term;
    ERL_NIF_TERM exchange_value;
    bool with_stats;
    ErlNifTime enqueue_time;
} match_t;

//

typedef int_fast8_t cell_count_t;

typedef struct {
    _Atomic(match_t*) match;
} cell_t;

//

typedef struct {
    ErlNifMonitor mon;
    ErlNifEnv* env;
    ERL_NIF_TERM broker_term;
    batch_id_t batch_id;
    offset_t offset;
    ERL_NIF_TERM side;
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
    bool is_closed;
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
    mempool_t env_pool;
    mempool_t match_pool;
} local_state_t;

//

typedef struct {
    broker_opts_t opts;
    ErlNifPid creator_pid;
    ErlNifMonitor owner_mon;
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

typedef struct {
    ErlNifEnv* env;
    ERL_NIF_TERM broker_term;
    broker_t* broker;
    ERL_NIF_TERM side;
    bool is_left;
    bool with_stats;
    bool is_nb;
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

typedef ssize_t thread_id_t;

/*********************************************************************/

static int on_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info);
;
static void init_atoms(ErlNifEnv* caller_env);
static void broker_resource_load(ErlNifEnv* caller_env);
static void cmonitor_resource_load(ErlNifEnv* caller_env);

static ERL_NIF_TERM nif_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_to_list(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ERL_NIF_TERM ask_loop(ask_ctx_t* ctx, ask_out_t* out);

static batch_t* batch_new(const batch_id_t id, const size_t nr_of_cells);
static size_t sizeof_broker(size_t nr_of_schedulers);
static ERL_NIF_TERM batch_ask(ask_ctx_t* ctx, batch_t* batch, ask_out_t* out);

static bool batch_lookup(broker_t* broker, local_state_t* local_state, batch_id_t batch_id,
                         batch_handle_t* out_handle);
static ERL_NIF_TERM batch_offset_ask(ask_ctx_t* ctx, batch_t* batch, offset_t offset,
                                     ask_out_t* out);
static void batch_consume_local_slot(broker_t* broker, local_state_t* local_state, batch_t* batch);
static bool batch_consume_slot(broker_t* broker, local_state_t* local_state,
                               batch_handle_t* handle);
static void batch_lower_ref_count(broker_t* broker, local_state_t* local_state,
                                  batch_handle_t* handle);
static batch_t* batch_get_next(ask_ctx_t* ctx, const batch_id_t prev_batch_id);
static void batch_preemptively_ensure_next(broker_t* broker, local_state_t* local_state,
                                           const batch_t* batch, const offset_t offset);

static void* match_alloc(void);
static match_t* match_new_monitored(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset);
//static match_t* match_new_unmonitored(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset);
static void match_demonitor_and_free(ErlNifEnv* caller_env, local_state_t* local_state,
                                     match_t** match_ptr);
static void match_clear(void* match);
static void match_free(void* match);

static void notify_of_match(ask_ctx_t* ctx, ErlNifPid* pid, match_t** match_ptr,
                            const ERL_NIF_TERM side, const batch_id_t batch_id,
                            const offset_t offset, const ERL_NIF_TERM match_ref, bool with_stats,
                            ErlNifTime enqueue_time);

static void notify_of_cancellation(ErlNifEnv* env, const batch_id_t batch_id,
                                   const offset_t offset, match_t** match_in_cell_ptr,
                                   bool did_broker_stop);

static void notify_if_alive(ErlNifEnv* caller_env, ErlNifPid* pid, ErlNifEnv* msg_env,
                            ERL_NIF_TERM msg);

static ErlNifEnv* env_pool_get(local_state_t* local_state);
static void env_pool_return(local_state_t* local_state, ErlNifEnv* env);
static void* env_pool_alloc_env(void);
static void env_pool_clear_env(void* env);
static void env_pool_free_env(void* env);

static match_t* match_pool_get(local_state_t* local_state);
static void match_pool_return(local_state_t* local_state, match_t* match);

static void mempool_init(mempool_t* pool);
static void* mempool_get(mempool_t* pool);
static void mempool_return(mempool_t* pool, void* obj);
static void mempool_destroy(mempool_t* pool);

static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out);
static int get_broker_opts(ErlNifEnv* env, ERL_NIF_TERM term, broker_opts_t* out);
static int get_cmonitor(ErlNifEnv* env, ERL_NIF_TERM term, cmonitor_t** out);
static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM* out_side,
                   batch_id_t* out_batch_id, offset_t* out_offset);

static ERL_NIF_TERM make_await(ErlNifEnv* env, ERL_NIF_TERM tag);
static ERL_NIF_TERM make_badarg(ErlNifEnv* env, ERL_NIF_TERM term);
static ERL_NIF_TERM make_match(ErlNifEnv* env, ERL_NIF_TERM match_ref, ERL_NIF_TERM exchange_value);
static ERL_NIF_TERM make_match_with_stats(ErlNifEnv* env, ERL_NIF_TERM match_ref,
                                          ERL_NIF_TERM exchange_value, ErlNifTime enqueue_time);
static ERL_NIF_TERM make_opposite_side(ERL_NIF_TERM side);
static ERL_NIF_TERM make_tag(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset);
static ERL_NIF_TERM make_tag_simple(ErlNifEnv* env, ERL_NIF_TERM side, batch_id_t batch_id,
                                    offset_t offset);

static const thread_id_t get_or_assign_thread_id(void);

static void broker_dtor(ErlNifEnv* caller_env, void* obj);
static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon);
static void batch_free_assert_presence_in_global(uint64_t key, void* value, void* ctx);
static void batch_free(uint64_t key, void* value, void* ctx);

static void cmonitor_dtor(ErlNifEnv* caller_env, void* obj);
static void cmonitor_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon);

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

static _Atomic(thread_id_t) next_thread_id = 0;
static _Thread_local thread_id_t my_thread_id = -1;

static match_t sentinel_match_cancelled;
static match_t sentinel_match_done;

static ErlNifFunc nif_funcs[] = {{"new", 0, nif_new},       {"new", 1, nif_new},
                                 {"ask", 4, nif_ask},       {"ask", 5, nif_ask},
                                 {"cancel", 2, nif_cancel}, {"to_list", 1, nif_to_list}};

ERL_NIF_INIT(cbroker_nif, nif_funcs, on_load, NULL, NULL, NULL);

/*********************************************************************/

static int on_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info)
{
    /* None of our NIFs may be scheduled on a dirty scheduler
     * - we rely on the calling process staying alive until
     * the NIF returns.
     */
    for (size_t i = 0; i < sizeof(nif_funcs) / sizeof(nif_funcs[0]); i++) {
        assert(nif_funcs[i].flags == 0);
    }

    init_atoms(caller_env);

    memset(&ResourceTypes, 0, sizeof(ResourceTypes));
    broker_resource_load(caller_env);
    cmonitor_resource_load(caller_env);

    return 0;
}

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

static void cmonitor_resource_load(ErlNifEnv* caller_env)
{
    ErlNifResourceTypeInit callbacks = {cmonitor_dtor, NULL, cmonitor_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.cmonitor =
        enif_init_resource_type(caller_env, "cbroker.cmonitor", &callbacks, flags, &flags);
    assert(ResourceTypes.cmonitor != NULL);
}

/*********************************************************************/

/*********************************************************************/

static ERL_NIF_TERM nif_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifPid self;
    if (!enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    broker_opts_t opts;
    memset(&opts, 0, sizeof(broker_opts_t));

    if (argc > 0 && !get_broker_opts(env, argv[0], &opts)) {
        return make_badarg(env, argv[0]);
    }

    ErlNifSysInfo sys_info;
    enif_system_info(&sys_info, sizeof(sys_info));
    size_t nr_of_schedulers = sys_info.scheduler_threads;
    assert(nr_of_schedulers > 0);

    const size_t broker_size = sizeof_broker(nr_of_schedulers);
    broker_t* broker = enif_alloc_resource(ResourceTypes.broker, broker_size);
    assert(broker != NULL);
    memset(broker, 0, broker_size);
    memcpy(&broker->opts, &opts, sizeof(broker_opts_t));

    broker->creator_pid = self;

    if (broker->opts.depends_on_creator) {
        int mon_res = enif_monitor_process(env, broker, &self, &broker->owner_mon);
        assert(mon_res == 0);
    }

    broker->nr_of_schedulers = nr_of_schedulers;
    broker->nr_of_cells_per_batch = 32 * nr_of_schedulers;

    broker->global_lock = enif_mutex_create("cbroker.mutex");

    broker->global_state.batches = cbroker_omap_new();
    assert(broker->global_state.batches != NULL);

    const batch_id_t first_batch_id = 1;
    batch_t* first_batch = batch_new(first_batch_id, broker->nr_of_cells_per_batch);

    cbroker_omap_result_t map_res =
        cbroker_omap_insert(broker->global_state.batches, first_batch_id, first_batch);
    assert(map_res == CBROKER_OMAP_OK);
    atomic_store_explicit(&first_batch->ref_count, 1, memory_order_relaxed);

    for (size_t i = 0; i < nr_of_schedulers; i++) {
        // TODO only initialize local state for the amount of _online_ schedulers.
        local_state_t* local_state = &broker->local_states[i];
        local_state->batches = cbroker_omap_new();
        assert(local_state->batches != NULL);

        map_res = cbroker_omap_insert(local_state->batches, first_batch_id, first_batch);
        assert(map_res == CBROKER_OMAP_OK);
        atomic_fetch_add_explicit(&first_batch->ref_count, 1, memory_order_seq_cst);

        local_state->left_id = first_batch_id;
        local_state->right_id = first_batch_id;

        local_state->env_pool.alloc_cb = env_pool_alloc_env;
        local_state->env_pool.clear_cb = env_pool_clear_env;
        local_state->env_pool.free_cb = env_pool_free_env;
        mempool_init(&local_state->env_pool);

        local_state->match_pool.alloc_cb = match_alloc;
        local_state->match_pool.clear_cb = match_clear;
        local_state->match_pool.free_cb = match_free;
        mempool_init(&local_state->match_pool);
    }

    ERL_NIF_TERM broker_term = enif_make_resource(env, broker);
    enif_release_resource(broker);
    return broker_term;
}

//

static ERL_NIF_TERM nif_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ask_ctx_t ctx;
    memset(&ctx, 0, sizeof(ask_ctx_t));

    ctx.env = env;
    ERL_NIF_TERM ask_type = Atoms._regular;

    ctx.broker_term = argv[0];
    ctx.side = argv[1];
    ctx.exchange_value = argv[2];
    ERL_NIF_TERM with_stats_term = argv[3];

    if (!get_broker(env, ctx.broker_term, &ctx.broker)) {
        return make_badarg(env, ctx.broker_term);
    }

    if (enif_self(env, &ctx.self)) {
        ctx.self_term = enif_make_pid(env, &ctx.self);
    }
    else {
        return enif_make_badarg(env);
    }

    if (ctx.side == Atoms._left) {
        ctx.is_left = true;
    }
    else if (ctx.side != Atoms._right) {
        return make_badarg(env, ctx.side);
    }

    if (with_stats_term == Atoms._true) {
        ctx.with_stats = true;
    }
    else if (with_stats_term != Atoms._false) {
        return make_badarg(env, with_stats_term);
    }

    if (argc >= 5) {
        ask_type = argv[4];
        if (ask_type == Atoms._nb) {
            ctx.is_nb = true;
        }
        else if (ask_type != Atoms._regular && ask_type != Atoms._fully_async) {
            return make_badarg(env, ask_type);
        }
    }

    ////////////////////////////

    thread_id_t thread_id = get_or_assign_thread_id();
    ctx.local_state = &ctx.broker->local_states[thread_id];
    assert(thread_id >= 0 && thread_id < ctx.broker->nr_of_schedulers);

    if (ctx.local_state->is_closed) {
        return Atoms._closed;
    }

    ask_out_t success;
    memset(&success, 0, sizeof(ask_out_t));

    ERL_NIF_TERM match_res = ask_loop(&ctx, &success);

    ////

    if (match_res == Atoms._await) {
        ERL_NIF_TERM tag = make_tag(&ctx, success.batch->id, success.offset);
        match_res = make_await(env, tag);
        batch_preemptively_ensure_next(ctx.broker, ctx.local_state, success.batch, success.offset);
    }
    else if (match_res == Atoms._matched) {
        match_t* our_match = success.our_match;
        success.our_match = NULL;

        match_t* opposite_match = success.opposite_match;
        success.opposite_match = NULL;

        const batch_id_t batch_id = success.batch->id;
        const offset_t offset = success.offset;

        assert(our_match != NULL);
        assert(opposite_match != NULL);

        ERL_NIF_TERM match_ref = enif_make_ref(env);

        LOG("dmatch: about to notify other");
        const ERL_NIF_TERM opposite_side = make_opposite_side(ctx.side);

        // 

        ErlNifTime our_enqueue_time = our_match->enqueue_time;

        notify_of_match(&ctx, &opposite_match->pid, &our_match,
                        opposite_side, batch_id,
                        offset, match_ref, opposite_match->with_stats,
                        opposite_match->enqueue_time);

        assert(our_match == NULL);

        //

        if (success.consume_slot) {
            LOG("dmatch: about to consume slot");
            batch_consume_local_slot(ctx.broker, ctx.local_state, success.batch);
        }

        //

        ERL_NIF_TERM self_tag = make_tag(&ctx, batch_id, success.offset);

        if (ask_type == Atoms._fully_async) {
            notify_of_match(&ctx, &ctx.self, &opposite_match,
                            ctx.side, batch_id,
                            offset, match_ref, ctx.with_stats,
                            our_enqueue_time);

            assert(opposite_match == NULL);

            match_res = make_await(env, self_tag);
        }
        else {
            ERL_NIF_TERM opposite_value = enif_make_copy(env, opposite_match->exchange_value);
            env_pool_return(ctx.local_state, opposite_match->env);
            match_pool_return(ctx.local_state, opposite_match);
            opposite_match = NULL;

            match_res = (ctx.with_stats ? make_match_with_stats(env, match_ref, opposite_value,
                                                                our_enqueue_time)
                                        : make_match(env, match_ref, opposite_value));
        }
    }
    else if (success.consume_slot) {
        assert(success.batch != NULL);
        batch_consume_local_slot(ctx.broker, ctx.local_state, success.batch);
    }

    return match_res;
}

//

static ERL_NIF_TERM nif_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    broker_t* broker = NULL;
    ERL_NIF_TERM side;
    batch_id_t batch_id;
    offset_t offset;
    ErlNifPid self;

    const ERL_NIF_TERM broker_term = argv[0];
    const ERL_NIF_TERM tag_term = argv[1];

    LOG("cancel: get broker");
    if (!get_broker(env, broker_term, &broker)) {
        return make_badarg(env, broker_term);
    }

    LOG("cancel: get tag");
    if (!get_tag(env, tag_term, &side, &batch_id, &offset)) {
        return make_badarg(env, tag_term);
    }

    LOG("cancel: get self");
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

    if (!batch_lookup(broker, local_state, batch_id, &handle)) {
        return Atoms._too_late;
    }

    batch_t* batch = handle.batch;

    LOG("cancel: batch %u found, cell at offset %u", batch_id, offset);
    cell_t* cell = &batch->cells[offset];

    ERL_NIF_TERM cancel_res = Atoms._none;

    _Atomic(match_t*)* match_ptr = &cell->match;
    match_t* match = atomic_load(match_ptr);
    match_t* cancelled_match = NULL;
    bool batch_consumed = false;

    if (match == NULL) {
        cancel_res = make_badarg(env, tag_term);
    }
    else if (match == &sentinel_match_done) {
        cancel_res = Atoms._too_late;
    }
    else if (match == &sentinel_match_cancelled) {
        cancel_res = Atoms._cancelled;
    }
    else if (match != NULL && atomic_compare_exchange_strong(match_ptr, &match, &sentinel_match_cancelled)) {
        cancel_res = Atoms._cancelled;
        cancelled_match = match;
        match = NULL;
    }

    //

    if (cancelled_match != NULL) {
        if (enif_compare_pids(&cancelled_match->pid, &self)) {
            notify_of_cancellation(env, batch_id, offset, &cancelled_match, false);
        }
        else {
            match_demonitor_and_free(env, local_state, &cancelled_match);
        }
        assert(cancelled_match == NULL);
        batch_consumed = batch_consume_slot(broker, local_state, &handle);
    }

    //

    if (!batch_consumed && !handle.found_locally && handle.batch != NULL) {
        batch_lower_ref_count(broker, local_state, &handle);
    }
    return cancel_res;
}

//

static ERL_NIF_TERM nif_to_list(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    broker_t* broker = NULL;

    if (!get_broker(env, argv[0], &broker)) {
        return make_badarg(env, argv[0]);
    }

    ERL_NIF_TERM env_pools_atom = enif_make_atom(env, "env_pools");
    ERL_NIF_TERM match_pools_atom = enif_make_atom(env, "match_pools");
    ERL_NIF_TERM batches_atom = enif_make_atom(env, "batches");

    ERL_NIF_TERM empty_atom = enif_make_atom(env, "");
    ERL_NIF_TERM set_atom = enif_make_atom(env, "!!");
    ERL_NIF_TERM id_atom = enif_make_atom(env, "batch_id");
    ERL_NIF_TERM ref_count_atom = enif_make_atom(env, "ref_count");
    ERL_NIF_TERM consumed_atom = enif_make_atom(env, "consumed_count");
    ERL_NIF_TERM cells_atom = enif_make_atom(env, "cells");
    ERL_NIF_TERM separator_atom = enif_make_atom(env, "------------------------");

    global_state_t* global_state = &broker->global_state;

    //////////////////
    /// Get temporary, ref-counted copies of global batches

    enif_mutex_lock(broker->global_lock);

    size_t nr_of_batches = cbroker_omap_size(global_state->batches);

    void** global_batches = cbroker_omap_values(global_state->batches);
    batch_t** copied_batches = enif_alloc(sizeof(batch_t*) * nr_of_batches);

    for (size_t i = 0; i < nr_of_batches; i++) {
        batch_t* batch = (batch_t*)global_batches[i];
        atomic_fetch_add_explicit(&batch->ref_count, 1, memory_order_relaxed);
        copied_batches[i] = batch;
    }

    enif_mutex_unlock(broker->global_lock);

    //////////////////
    /// Prepare output

    ERL_NIF_TERM batch_terms[nr_of_batches];

    for (size_t i = 0; i < nr_of_batches; i++) {
        batch_t* batch = copied_batches[i];
        ERL_NIF_TERM cell_terms[batch->nr_of_cells];
        size_t nr_of_cells = batch->nr_of_cells;

        for (size_t j = 0; j < nr_of_cells; j++) {
            cell_t* cell = &batch->cells[j];

            match_t* match = cell->match;
            ERL_NIF_TERM match_term = empty_atom;

            if (match == &sentinel_match_cancelled) {
                match_term = Atoms._cancelled;
            }
            else if (match == &sentinel_match_done) {
                match_term = Atoms._matched;
            }
            else if (match != NULL) {
                match_term = set_atom;
            }

            cell_terms[j] = match_term;
        }

        batch_id_t batch_id = batch->id;
        size_t ref_count = atomic_load_explicit(&batch->ref_count, memory_order_relaxed);
        size_t consumed_count = atomic_load_explicit(&batch->consumed_count, memory_order_relaxed);
        ERL_NIF_TERM cell_list = enif_make_list_from_array(env, cell_terms, nr_of_cells);

        ERL_NIF_TERM batch_kvlist[5];

        batch_kvlist[0] = enif_make_tuple2(env, id_atom, enif_make_uint64(env, batch_id));
        batch_kvlist[1] = enif_make_tuple2(env, ref_count_atom, enif_make_uint64(env, ref_count));
        batch_kvlist[2] =
            enif_make_tuple2(env, consumed_atom, enif_make_uint64(env, consumed_count));
        batch_kvlist[3] = enif_make_tuple2(env, cells_atom, cell_list);
        batch_kvlist[4] = separator_atom;

        batch_terms[i] = enif_make_list_from_array(env, batch_kvlist, 5);
    }

    //////////////////
    /// Destroy temporary, ref-counted copies of global batches

    for (size_t i = 0; i < nr_of_batches; i++) {
        batch_handle_t handle;
        handle.batch = copied_batches[i];
        handle.found_locally = false; // even if it is, it doesn't matter in this context
        batch_lower_ref_count(broker, NULL, &handle);
    }

    enif_free(copied_batches);

    /// Return

    ERL_NIF_TERM output_terms[3];

    ERL_NIF_TERM env_pool_terms[broker->nr_of_schedulers];
    ERL_NIF_TERM match_pool_terms[broker->nr_of_schedulers];

    for (size_t i = 0; i < broker->nr_of_schedulers; i++) {
        local_state_t* local_state = &broker->local_states[i];

        size_t env_pool_count = local_state->env_pool.count;
        env_pool_terms[i] = enif_make_uint64(env, env_pool_count);

        size_t match_pool_count = local_state->match_pool.count;
        match_pool_terms[i] = enif_make_uint64(env, match_pool_count);
    }

    ERL_NIF_TERM env_pools_list =
        enif_make_list_from_array(env, env_pool_terms, broker->nr_of_schedulers);
    output_terms[0] = enif_make_tuple2(env, env_pools_atom, env_pools_list);

    ERL_NIF_TERM match_pools_list =
        enif_make_list_from_array(env, match_pool_terms, broker->nr_of_schedulers);
    output_terms[1] = enif_make_tuple2(env, match_pools_atom, match_pools_list);

    ERL_NIF_TERM batches_list = enif_make_list_from_array(env, batch_terms, nr_of_batches);
    output_terms[2] = enif_make_tuple2(env, batches_atom, batches_list);

    return enif_make_list_from_array(env, output_terms, 3);
}

/*********************************************************************/

static ERL_NIF_TERM ask_loop(ask_ctx_t* ctx, ask_out_t* out)
{
    local_state_t* local_state = ctx->local_state;
    batch_id_t batch_id = (ctx->is_left ? local_state->left_id : local_state->right_id);
    batch_t* batch = NULL;
    batch_t* skipped_batch = NULL;

    ////

    // const int max_attempts = MIN(100, MAX(5, ctx->broker->nr_of_cells_per_batch / 2));
    const int max_attempts = 400;
    int percent_reported = 0;

    for (int attempt_nr = 1; attempt_nr <= max_attempts; attempt_nr++) {
        int percent = MAX(1, 100 * attempt_nr / max_attempts);
        if (percent != percent_reported) {
            percent_reported = percent;
            if (enif_consume_timeslice(ctx->env, percent)) {
                break;
            }
        }

        skipped_batch = NULL;
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
                batch_handle_t handle;
                memset(&handle, 0, sizeof(batch_handle_t));
                handle.batch = batch;
                handle.found_locally = true;
                batch_lower_ref_count(ctx->broker, ctx->local_state, &handle);
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

        batch = batch_get_next(ctx, batch_id);
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

        if (batch == NULL) {
            return Atoms._closed;
        }
    }

    if (percent_reported < 100) {
        // enif_consume_timeslice(ctx->env, 100);
    }

    return Atoms._retry;
}

/*********************************************************************/

static batch_t* batch_new(const batch_id_t id, const size_t nr_of_cells)
{
    LOG("NEW BATCH!! %u", id);
    const size_t size = sizeof(batch_t) + (nr_of_cells * sizeof(cell_t));

    batch_t* batch = enif_alloc(size);
    assert(batch != NULL);
    memset(batch, 0, size);

    batch->id = id;
    batch->nr_of_cells = nr_of_cells;

    return batch;
}

static size_t sizeof_broker(size_t nr_of_schedulers)
{
    assert(nr_of_schedulers > 0);
    return sizeof(broker_t) + (nr_of_schedulers * sizeof(local_state_t));
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

static ERL_NIF_TERM batch_offset_ask(ask_ctx_t* ctx, batch_t* batch, offset_t offset,
                                     ask_out_t* out)
{
    size_t cell_offset = offset % batch->nr_of_cells;

    cell_t* cell = &batch->cells[cell_offset];

    if (cell->match == &sentinel_match_cancelled) {
        // quick path
        return Atoms._cancelled;
    }

    _Atomic(match_t*)* match_ptr = &cell->match;
    match_t* match = NULL;
    match_t* our_match = match_new_monitored(ctx, batch->id, offset);

    // FIXME behaviour for `is_nb`

    if (atomic_compare_exchange_strong(match_ptr, &match, our_match)) {
        // Enqueued and monitored
        return Atoms._await;
    }
    else if (match == &sentinel_match_cancelled) {
        match_demonitor_and_free(ctx->env, ctx->local_state, &our_match);
        return Atoms._cancelled;
    }
    else {
        assert(match != &sentinel_match_done);

        if (atomic_compare_exchange_strong(match_ptr, &match, &sentinel_match_done)) {
            out->our_match = our_match;
            out->opposite_match = match;
            out->consume_slot = true;
            return Atoms._matched;
        }
        else {
            assert(match == &sentinel_match_cancelled);
            match_demonitor_and_free(ctx->env, ctx->local_state, &our_match);
            return Atoms._cancelled;
        }
    }
}

static bool batch_lookup(broker_t* broker, local_state_t* local_state, batch_id_t batch_id,
                         batch_handle_t* out_handle)
{
    batch_t* batch = NULL;

    if (cbroker_omap_lookup(local_state->batches, batch_id, (void**)&batch)) {
        out_handle->batch = batch;
        out_handle->found_locally = true;
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
            return true;
        }
        else {
            enif_mutex_unlock(broker->global_lock);
            return false;
        }
    }
}

static void batch_consume_local_slot(broker_t* broker, local_state_t* local_state, batch_t* batch)
{
    batch_handle_t handle;
    memset(&handle, 0, sizeof(batch_handle_t));

    handle.batch = batch;
    handle.found_locally = true;
    batch_consume_slot(broker, local_state, &handle);
}

static bool batch_consume_slot(broker_t* broker, local_state_t* local_state, batch_handle_t* handle)
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
        assert(consumed_count == batch->nr_of_cells);
        batch_lower_ref_count(broker, local_state, handle);
        return true;
    }
}

static void batch_lower_ref_count(broker_t* broker, local_state_t* local_state,
                                  batch_handle_t* handle)
{
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

        if (cbroker_omap_lookup(global_state->batches, batch_id, (void**)&batch)) {
            ref_count = atomic_load_explicit(&batch->ref_count, memory_order_acquire);
            assert(ref_count >= 1);

            if (ref_count == 1) {
                bool has_next = true;
                cbroker_omap_delete_and_next(global_state->batches, batch_id, &has_next, NULL,
                                             NULL);
                LOG("CONSUME: batch %u deleted", batch_id);
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
                }
            }
        }

        enif_mutex_unlock(broker->global_lock);
    }

    handle->batch = NULL;
}

static batch_t* batch_get_next(ask_ctx_t* ctx, const batch_id_t prev_batch_id)
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
            next_batch = batch_new(next_batch_id, broker->nr_of_cells_per_batch);
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

    return next_batch;
}

static void batch_preemptively_ensure_next(broker_t* broker, local_state_t* local_state,
                                           const batch_t* batch, const offset_t offset)
{
    /* Ensure next batch is allocated when we're halfway through enqueuing in previous
     * - this will reduce lock times once all other threads want to get the next batch,
     *   since it's already allocated and only needs a ref count increment.
     */
    const offset_t target_offset = batch->nr_of_cells >> 1;
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;

    if (offset != target_offset) {
        return;
    }

    if (cbroker_omap_next(local_state->batches, batch->id, NULL, NULL)) {
        // Already ensured
        return;
    }

    global_state_t* global_state = &broker->global_state;
    batch_id_t next_batch_id = 0;
    batch_t* next_batch = NULL;

    enif_mutex_lock(broker->global_lock);

    if (cbroker_omap_next(global_state->batches, batch->id, &next_batch_id, (void**)&next_batch)) {
        // Already ensured
        size_t ref_count =
            1 + atomic_fetch_add_explicit(&next_batch->ref_count, 1, memory_order_seq_cst);
        assert(ref_count >= 1);
    }
    else {
        next_batch_id = batch->id + 1;
        assert(next_batch_id > local_state->left_id);
        assert(next_batch_id > local_state->right_id);

        next_batch = batch_new(next_batch_id, broker->nr_of_cells_per_batch);
        atomic_store_explicit(&next_batch->ref_count, 2, memory_order_relaxed);

        map_res = cbroker_omap_insert(global_state->batches, next_batch_id, next_batch);
        assert(map_res == CBROKER_OMAP_OK);
    }

    enif_mutex_unlock(broker->global_lock);

    map_res = cbroker_omap_insert(local_state->batches, next_batch_id, next_batch);
    assert(map_res == CBROKER_OMAP_OK);
}

/*********************************************************************/

static void* match_alloc()
{
    match_t* match = enif_alloc(sizeof(match_t));
    match_clear(match);
    return match;
}

static match_t* match_new_monitored(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset)
{
    cmonitor_t* cmonitor =
        (cmonitor_t*)enif_alloc_resource(ResourceTypes.cmonitor, sizeof(cmonitor_t));
    memset(cmonitor, 0, sizeof(cmonitor_t));

    cmonitor->env = env_pool_get(ctx->local_state);
    cmonitor->broker_term = enif_make_copy(cmonitor->env, ctx->broker_term);
    cmonitor->batch_id = batch_id;
    cmonitor->offset = offset;
    cmonitor->side = ctx->side;

    /* We should never fail to monitor selves within the queue
     * as long as we're running as a regular NIF
     */
    int monitor_res = enif_monitor_process(ctx->env, cmonitor, &ctx->self, &cmonitor->mon);
    assert(monitor_res == 0);

    match_t* match = match_pool_get(ctx->local_state);

    ErlNifEnv* match_env = env_pool_get(ctx->local_state);
    match->side = ctx->side;
    match->pid = ctx->self;
    match->env = match_env;
    match->cmonitor_term = enif_make_resource(match_env, cmonitor);
    match->exchange_value = enif_make_copy(match_env, ctx->exchange_value);

    if (ctx->with_stats) {
        match->with_stats = true;
        match->enqueue_time = enif_monotonic_time(ERL_NIF_NSEC);
    }

    enif_release_resource(cmonitor);

    return match;
}

//static match_t* match_new_unmonitored(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset)
//{
//    match_t* match = match_pool_get(ctx->local_state);
//    memset(match, 0, sizeof(match_t));
//
//    ErlNifEnv* match_env = env_pool_get(ctx->local_state);
//    match->side = ctx->side;
//    match->pid = ctx->self;
//    match->env = match_env;
//    match->cmonitor_term = Atoms._none;
//    match->exchange_value = enif_make_copy(match_env, ctx->exchange_value);
//
//    if (ctx->with_stats) {
//        match->with_stats = true;
//        match->enqueue_time = enif_monotonic_time(ERL_NIF_NSEC);
//    }
//
//    return match;
//}

static void match_demonitor_and_free(ErlNifEnv* caller_env, local_state_t* local_state,
                                     match_t** match_ptr)
{
    match_t* match = *match_ptr;
    assert(match != NULL);

    cmonitor_t* cmonitor = NULL;

    if (get_cmonitor(caller_env, match->cmonitor_term, &cmonitor)) {
        enif_demonitor_process(caller_env, cmonitor, &cmonitor->mon);
    }
    else {
        assert(match->cmonitor_term == Atoms._none);
    }

    env_pool_return(local_state, match->env);
    match_pool_return(local_state, match);

    *match_ptr = NULL;
}

static void match_clear(void* match) { memset(match, 0, sizeof(match_t)); }

static void match_free(void* match) { enif_free((match_t*)match); }

/*********************************************************************/

static void notify_of_match(ask_ctx_t* ctx, ErlNifPid* pid, match_t** match_ptr,
                            const ERL_NIF_TERM side, const batch_id_t batch_id,
                            const offset_t offset, const ERL_NIF_TERM match_ref, bool with_stats,
                            ErlNifTime enqueue_time)
{
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

    ERL_NIF_TERM msg_content =
        (with_stats
             ? make_match_with_stats(match_env, msg_match_ref, match->exchange_value, enqueue_time)
             : make_match(match_env, msg_match_ref, match->exchange_value));

    ERL_NIF_TERM msg = enif_make_tuple2(match_env, tag, msg_content);
    notify_if_alive(caller_env, pid, match_env, msg);

    env_pool_return(local_state, match_env);
    match_pool_return(local_state, match);

    if (get_monitor_res) {
        enif_demonitor_process(caller_env, cmonitor, &cmonitor->mon);
    }
    else {
        assert(cmonitor_term == Atoms._none);
    }

    *match_ptr = NULL;
}

static void notify_of_cancellation(ErlNifEnv* env, const batch_id_t batch_id,
                                   const offset_t offset, match_t** match_in_cell_ptr,
                                   bool did_broker_stop)
{
    match_t* match_in_cell = *match_in_cell_ptr;
    assert(match_in_cell != NULL);
    assert(match_in_cell != &sentinel_match_cancelled);

    cmonitor_t* cmonitor = NULL;

    if (get_cmonitor(env, match_in_cell->cmonitor_term, &cmonitor)) {
        enif_demonitor_process(env, cmonitor, &cmonitor->mon);
    }
    else {
        assert(match_in_cell->cmonitor_term == Atoms._none);
    }

    match_in_cell->env = NULL;

    ERL_NIF_TERM tag = make_tag_simple(env, match_in_cell->side, batch_id, offset);
    ERL_NIF_TERM msg_content = (did_broker_stop ? Atoms._stopped : Atoms._cancelled);
    ERL_NIF_TERM msg = enif_make_tuple2(env, tag, msg_content);
    notify_if_alive(env, &match_in_cell->pid, NULL, msg);

    enif_free(match_in_cell);
    *match_in_cell_ptr = NULL;
}

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

/*********************************************************************/

static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out)
{
    return enif_get_resource(env, term, ResourceTypes.broker, (void**)out);
}

static int get_broker_opts(ErlNifEnv* env, ERL_NIF_TERM term, broker_opts_t* out)
{
    ERL_NIF_TERM head, tail, key, value;
    const ERL_NIF_TERM* tuple = NULL;
    int arity = 0;

    if (enif_is_empty_list(env, term)) {
        return 1;
    }
    else if (!enif_get_list_cell(env, term, &head, &tail)) {
        return 0;
    }

    //

    if (enif_is_atom(env, head)) {
        key = head;
        value = Atoms._true;
    }
    else if (enif_get_tuple(env, head, &arity, &tuple) && arity == 2 &&
             enif_is_atom(env, tuple[0])) {
        key = tuple[0];
        value = tuple[1];
    }
    else {
        return 0;
    }

    //

    if (key == Atoms._depends_on_creator) {
        if (value == Atoms._true) {
            out->depends_on_creator = true;
        }
        else if (value != Atoms._false) {
            return 0;
        }
    }
    else {
        return 0;
    }

    return get_broker_opts(env, tail, out);
}

static int get_cmonitor(ErlNifEnv* env, ERL_NIF_TERM term, cmonitor_t** out)
{
    return enif_get_resource(env, term, ResourceTypes.cmonitor, (void**)out);
}

static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM* out_side,
                   batch_id_t* out_batch_id, offset_t* out_offset)
{
    int arity = -1;
    const ERL_NIF_TERM* elements = NULL;
    batch_id_t batch_id;
    offset_t offset;

    LOG("ehhh");

    if (enif_get_tuple(env, term, &arity, &elements) && arity == 3 &&
        (elements[0] == Atoms._left || elements[0] == Atoms._right) &&
        enif_get_uint64(env, elements[1], &batch_id) &&
        enif_get_uint64(env, elements[2], &offset)) {
        *out_side = elements[0];
        *out_batch_id = batch_id;
        *out_offset = offset;
        return 1;
    }
    else {
        return 0;
    }
}

/*********************************************************************/

static ERL_NIF_TERM make_await(ErlNifEnv* env, ERL_NIF_TERM tag)
{
    return enif_make_tuple2(env, Atoms._await, tag);
}

static ERL_NIF_TERM make_badarg(ErlNifEnv* env, ERL_NIF_TERM term)
{
    ERL_NIF_TERM reason = enif_make_tuple2(env, Atoms._badarg, term);
    return enif_raise_exception(env, reason);
}

static ERL_NIF_TERM make_match(ErlNifEnv* env, ERL_NIF_TERM match_ref, ERL_NIF_TERM exchange_value)
{
    return enif_make_tuple3(env, Atoms._match, match_ref, exchange_value);
}

static ERL_NIF_TERM make_match_with_stats(ErlNifEnv* env, ERL_NIF_TERM match_ref,
                                          ERL_NIF_TERM exchange_value, ErlNifTime enqueue_time)
{
    int_fast64_t sojourn_time = enif_monotonic_time(ERL_NIF_NSEC) - enqueue_time;
    return enif_make_tuple4(env, Atoms._match, match_ref, exchange_value,
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

static ERL_NIF_TERM make_tag(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset)
{
    return make_tag_simple(ctx->env, ctx->side, batch_id, offset);
}

static ERL_NIF_TERM make_tag_simple(ErlNifEnv* env, ERL_NIF_TERM side, batch_id_t batch_id,
                                    offset_t offset)
{
    return enif_make_tuple3(env, side, enif_make_uint64(env, batch_id),
                            enif_make_uint64(env, offset));
}

/*********************************************************************/

static ErlNifEnv* env_pool_get(local_state_t* local_state)
{
    ErlNifEnv* env = (ErlNifEnv*)mempool_get(&local_state->env_pool);
    assert(env != NULL);
    return env;
}

static void env_pool_return(local_state_t* local_state, ErlNifEnv* env)
{
    assert(env != NULL);
    mempool_return(&local_state->env_pool, env);
}

static void* env_pool_alloc_env() { return (void*)enif_alloc_env(); }

static void env_pool_clear_env(void* env) { enif_clear_env((ErlNifEnv*)env); }

static void env_pool_free_env(void* env) { enif_free_env((ErlNifEnv*)env); }

/*********************************************************************/

static match_t* match_pool_get(local_state_t* local_state)
{
    match_t* match = (match_t*)mempool_get(&local_state->match_pool);
    assert(match != NULL);
    return match;
}

static void match_pool_return(local_state_t* local_state, match_t* match)
{
    assert(match != NULL);
    mempool_return(&local_state->match_pool, match);
}

/*********************************************************************/

static void mempool_init(mempool_t* pool)
{
    const size_t initial_size = TARGET_MEMPOOL_SIZE;
    pool->size = initial_size;
    pool->array = enif_alloc(pool->size * sizeof(void*));

    pool->count = pool->size;
    for (size_t i = 0; i < initial_size; i++) {
        pool->array[i] = pool->alloc_cb();
    }
}

static void* mempool_get(mempool_t* pool)
{
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

///////////////

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

static void broker_dtor(ErlNifEnv* caller_env, void* obj)
{
    LOG("destroying broker %p", obj);
    broker_t* broker = (broker_t*)obj;

    enif_mutex_destroy(broker->global_lock);

    for (thread_id_t thread_id = 0; thread_id < broker->nr_of_schedulers; thread_id++) {
        local_state_t* local_state = &broker->local_states[thread_id];
        cbroker_omap_destroy(local_state->batches, batch_free_assert_presence_in_global,
                             &broker->global_state);
        mempool_destroy(&local_state->env_pool);
        mempool_destroy(&local_state->match_pool);
    }

    cbroker_omap_destroy(broker->global_state.batches, batch_free, NULL);

    memset(broker, 0, sizeof_broker(broker->nr_of_schedulers));
}

static void batch_free_assert_presence_in_global(uint64_t key, void* value, void* ctx)
{
    // Doesn't free anything, just asserts that the batch is present in global state
    batch_id_t batch_id = key;
    global_state_t* global_state = (global_state_t*)ctx;
    assert(cbroker_omap_lookup(global_state->batches, batch_id, NULL));
}

static void batch_free(uint64_t key, void* value, void* ctx)
{
    batch_t* batch = (batch_t*)value;

    for (offset_t i = 0; i < batch->nr_of_cells; i++) {
        cell_t* cell = &batch->cells[i];
        match_t* match = cell->match;
        if (match != NULL && match != &sentinel_match_cancelled && match != &sentinel_match_done) {
            enif_free_env(match->env);
            match->env = NULL;
            enif_free(match);
            cell->match = NULL;
        }
    }

    enif_free(batch);
}

//

static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
    broker_t* broker = (broker_t*)obj;
    assert(broker->opts.depends_on_creator);

    global_state_t* global_state = &broker->global_state;

    //////////////////
    /// Get temporary, ref-counted copies of global batches

    enif_mutex_lock(broker->global_lock);

    // Close broker globally
    assert(!global_state->is_closed);
    global_state->is_closed = true;

    size_t nr_of_batches = cbroker_omap_size(global_state->batches);

    void** global_batches = cbroker_omap_values(global_state->batches);
    batch_t** copied_batches = enif_alloc(sizeof(batch_t*) * nr_of_batches);

    for (size_t i = 0; i < nr_of_batches; i++) {
        batch_t* batch = (batch_t*)global_batches[i];
        atomic_fetch_add_explicit(&batch->ref_count, 1, memory_order_relaxed);
        copied_batches[i] = batch;
    }

    enif_mutex_unlock(broker->global_lock);

    // Close broker in all local states (best effort)
    for (size_t i = 0; i < broker->nr_of_schedulers; i++) {
        local_state_t* local_state = &broker->local_states[i];
        local_state->is_closed = true;
    }

    //////////////////
    /// Cancel anyone enqueued

    for (size_t i = 0; i < nr_of_batches; i++) {
        batch_t* batch = copied_batches[i];
        const size_t nr_of_cells = batch->nr_of_cells;

        for (offset_t offset = 0; offset < nr_of_cells; offset++) {
            cell_t* cell = &batch->cells[offset];

            if (cell->match == &sentinel_match_cancelled || cell->match == &sentinel_match_done) {
                continue;
            }

            match_t* cancelled_match = NULL;

            if (cell->match == NULL) {
                cell->match = &sentinel_match_cancelled;
            }
            else if (cell->match != &sentinel_match_cancelled && cell->match != &sentinel_match_done) {
                cancelled_match = cell->match;
                cell->match = &sentinel_match_cancelled;
            }

            if (cancelled_match != NULL) {
                notify_of_cancellation(caller_env, batch->id, offset, &cancelled_match, true);
                assert(cancelled_match == NULL);
            }
        }
    }

    //////////////////
    /// Destroy temporary, ref-counted copies of global batches

    for (size_t i = 0; i < nr_of_batches; i++) {
        batch_handle_t handle;
        handle.batch = copied_batches[i];
        handle.found_locally = false; // even if it is, it doesn't matter in this context
        batch_lower_ref_count(broker, NULL, &handle);
    }

    enif_free(copied_batches);
}

/*********************************************************************/

static void cmonitor_dtor(ErlNifEnv* caller_env, void* obj)
{
    cmonitor_t* cmonitor = (cmonitor_t*)obj;

    if (cmonitor->env != NULL) {
        enif_free_env(cmonitor->env);
    }
}

static void cmonitor_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
    LOG("cdown: cmonitor=%p", obj);
    cmonitor_t* cmonitor = (cmonitor_t*)obj;
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

    if (!batch_lookup(broker, local_state, batch_id, &handle)) {
        // too late
        return;
    }

    batch_t* batch = handle.batch;

    LOG("cdown: cell offset is %u", offset);
    cell_t* cell = &batch->cells[offset];

    _Atomic(match_t*)* match_ptr = &cell->match;
    match_t* match = atomic_load(match_ptr);
    match_t* cancelled_match = NULL;
    bool batch_consumed = false;

    assert(match != NULL);

    if (match != &sentinel_match_done
        && match != &sentinel_match_done
        && atomic_compare_exchange_strong(match_ptr, &match, &sentinel_match_cancelled))
    {
        cancelled_match = match;
    }

    if (cancelled_match != NULL) {
        cancelled_match->cmonitor_term = Atoms._none;
        match_demonitor_and_free(caller_env, local_state, &cancelled_match);
        assert(cancelled_match = NULL);
        batch_consumed = batch_consume_slot(broker, local_state, &handle);
    }

    if (!batch_consumed && !handle.found_locally && handle.batch != NULL) {
        batch_lower_ref_count(broker, local_state, &handle);
    }

    //

    if (cmonitor->env != NULL) {
        env_pool_return(local_state, cmonitor->env);
        cmonitor->env = NULL;
    }
}
