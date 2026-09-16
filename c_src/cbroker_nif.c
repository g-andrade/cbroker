#include "erl_nif.h"

#include <assert.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "cbroker_omap.h"

/* memory_order_acq_rel is a GCC/Clang extension, missing from MSVC's
 * stdatomic.h. Orders are only hints, so seq_cst is always a valid stand-in. */
#if defined(_MSC_VER) && !defined(__clang__)
#define memory_order_acq_rel memory_order_seq_cst
#endif

/*********************************************************************/

#define BATCH_POOL_INITIAL_COUNT 1
#define BATCH_POOL_SIZE 4

#define MATCH_POOLS_INITIAL_COUNT 8
#define MATCH_POOLS_SIZE 8

#define ENV_POOLS_INITIAL_COUNT 8
#define ENV_POOLS_SIZE 8

#define TAG_POOLS_INITIAL_COUNT 8
#define TAG_POOLS_SIZE 8

//

#define MAX_ASK_RETRIES 10

//

/* The columns below are aligned on purpose. */
/* clang-format off */
#define ATOM_LIST \
    X(_async,                 "async") \
    X(_await,                 "await") \
    X(_badarg,                "badarg") \
    X(_badopt,                "badopt") \
    X(_badopts,               "badopts") \
    X(_batch_consumed,        "batch_consumed") \
    X(_batch_full,            "batch_full")  \
    X(_batch_pool,            "batch_pool")  \
    X(_batches,               "batches") \
    X(_broker_closed,         "broker_closed") \
    X(_broker_overloaded,     "broker_overloaded") \
    X(_cancelled,             "cancelled") \
    X(_cells,                 "cells") \
    X(_compute_from_nif,      "compute_from_nif") \
    X(_consumed_count,        "consumed_count") \
    X(_creator,               "creator") \
    X(_depends_on_creator,    "depends_on_creator") \
    X(_drop,                  "drop") \
    X(_dynamic,               "dynamic") \
    X(_empty,                 "empty") \
    X(_env_pool,              "env_pool") \
    X(_error,                 "error") \
    X(_false,                 "false") \
    X(_global_state,          "global_state") \
    X(_id,                    "id") \
    X(_left,                  "left")  \
    X(_left_count,            "left_count")  \
    X(_local_states,          "local_states")  \
    X(_match,                 "match") \
    X(_match_pool,            "match_pool") \
    X(_match_unavailable,     "match_unavailable") \
    X(_matched,               "matched") \
    X(_non_blocking,          "non_blocking") \
    X(_none,                  "none") \
    X(_nr_of_cells_per_batch, "nr_of_cells_per_batch") \
    X(_nr_of_schedulers,      "nr_of_schedulers") \
    X(_ref_count,             "ref_count") \
    X(_retry,                 "retry") \
    X(_right,                 "right") \
    X(_right_count,           "right_count") \
    X(_stopped,               "stopped") \
    X(_tag_batch_shift,       "tag_batch_shift") \
    X(_tag_offset_mask,       "tag_offset_mask") \
    X(_tag_pool,              "tag_pool") \
    X(_too_late,              "too_late") \
    X(_true,                  "true") \
    X(_unavailable,           "unavailable") \
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

#define USES_FLAT_SIZE (ERL_NIF_MAJOR_VERSION == 2 && ERL_NIF_MINOR_VERSION < 18)

/*********************************************************************/

//

typedef uint_fast64_t offset_t;

typedef offset_t batch_id_t;

//

typedef ptrdiff_t ref_count_t;

//

typedef struct {
    ErlNifTime enqueue_ts;
    ErlNifPid pid;
    ErlNifEnv* env;
    ERL_NIF_TERM offer;
    size_t offer_size;
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

typedef _Atomic(match_t*) cell_t;

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
    void** array;
    size_t count;
    size_t size;
    void* (*alloc_cb)(void*);
    void (*clear_cb)(void*);
    void (*free_cb)(void*);
} mempool_t;

//

typedef struct {
    size_t nr_of_cells;
} batch_pool_alloc_ctx_t;

//

typedef struct {
    atomic_bool is_closed;
    cbroker_omap_t* batches;
    mempool_t batch_pool;
} global_state_t;

//

typedef struct {
    bool is_closed;
    cbroker_omap_t* batches;
    batch_id_t left_id;
    batch_id_t right_id;
    //
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

typedef ptrdiff_t thread_id_t;

/*********************************************************************/

typedef enum {
    DROP_REASON_NONE,
    DROP_REASON_CANCELLED,
    DROP_REASON_NON_BLOCKING,
    DROP_REASON_TOO_MANY_RETRIES,
    DROP_REASON_CLOSED
} drop_reason_t;

//

typedef struct {
    batch_t* batch;
    bool found_locally;
    broker_t* broker;
    local_state_t* opt_local_state;
} lease_t;

//

typedef struct {
    ErlNifEnv* env;
    ErlNifTime enqueue_ts;
    int retry_nr;
    ErlNifPid self;
    ERL_NIF_TERM self_term;
    //
    ERL_NIF_TERM broker_term;
    ERL_NIF_TERM side;
    ERL_NIF_TERM offer;
    size_t offer_size;
    //
    broker_t* broker;
    bool is_left;
    bool is_async;
    bool is_non_blocking;
    local_state_t* local_state;
    size_t copied_bytes;
} ask_ctx_t;

//

typedef struct {
    batch_t* batch;
    offset_t offset;
    bool consume_slot;
    // optional, reuse to send match if we allocated it but ended up in 2nd place
    match_t* our_match;
    ERL_NIF_TERM our_tag;
    match_t* opposite_match;
    drop_reason_t drop_reason;
} ask_out_t;

/*********************************************************************/

static int on_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info);
static void init_atoms(ErlNifEnv* caller_env);
static void broker_resource_load(ErlNifEnv* caller_env);
static void tag_resource_load(ErlNifEnv* caller_env);

//

static ERL_NIF_TERM nif_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_debug_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//

static size_t new_broker_size(const size_t nr_of_schedulers);
static batch_t* global_state_init(global_state_t* global_state, const size_t nr_of_cells_per_batch);
static void global_state_close(global_state_t* global_state);
static ERL_NIF_TERM global_state_to_term(ErlNifEnv* env, global_state_t* global_state);

//

static void local_states_init(local_state_t local_states[], const size_t nr_of_schedulers,
                              batch_t* first_batch);

static void local_states_dirty_close(local_state_t local_states[], const size_t nr_of_schedulers);

static local_state_t* broker_local_state(broker_t* broker);

static batch_t* local_state_get_batch(local_state_t* local_state, const batch_id_t batch_id);

static ERL_NIF_TERM local_states_to_term(ErlNifEnv* env, local_state_t local_states[],
                                         const size_t nr_of_schedulers);

static thread_id_t get_or_assign_thread_id(const size_t nr_of_schedulers);

//

static ERL_NIF_TERM ask_loop(ask_ctx_t* ctx, ask_out_t* out);
static batch_t* ask_get_next_batch(ask_ctx_t* ctx, const batch_id_t prev_batch_id);
static ERL_NIF_TERM ask_batch(ask_ctx_t* ctx, batch_t* batch, ask_out_t* out);
static ERL_NIF_TERM ask_batch_offset(ask_ctx_t* ctx, batch_t* batch, offset_t offset,
                                     _Atomic(offset_t)* offset_counter, ask_out_t* out);

static match_t* ask_ensure_our_match(ask_ctx_t* ctx, const batch_id_t batch_id,
                                     const offset_t offset, ask_out_t* out);

static match_t* ask_prepare_our_match(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset);

static void ask_await_preemptively_fill_batch_pool(ask_ctx_t* ctx, ask_out_t* out);

//

static bool match_demonitor(ErlNifEnv* caller_env, match_t* match);
static void match_reclaim(match_t* match, bool tag_used, local_state_t* opt_local_state);
static bool match_demonitor_and_reclaim(ErlNifEnv* caller_env, match_t* match, bool tag_used,
                                        local_state_t* local_state);

//

static void tag_dtor(ErlNifEnv* caller_env, void* obj);
static void tag_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon);

//

static bool broker_checkout_batch(broker_t* broker, local_state_t* opt_local_state,
                                  batch_id_t batch_id, lease_t* out_lease);

static void broker_consume_batch_slot(broker_t* broker, local_state_t* local_state, batch_t* batch);

static void broker_cancel_all_batch_cells(ErlNifEnv* env, broker_t* broker,
                                          ERL_NIF_TERM broker_term, local_state_t* local_state,
                                          batch_t* batch, bool did_broker_close);

static void broker_checkout_all_batches(broker_t* broker, local_state_t* opt_local_state,
                                        lease_t** out_array, size_t* out_nr_of_batches);

static void broker_checkin_many_batches(broker_t* broker, lease_t** array_ptr,
                                        const size_t nr_of_batches);

static void broker_dtor(ErlNifEnv* caller_env, void* obj);
static void broker_dtor_cb_local_batch(batch_id_t key, void* obj, void* ctx);
static void broker_dtor_cb_global_batch(batch_id_t key, void* obj, void* ctx);

static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon);

//

static void lease_init(lease_t* lease, batch_t* batch, const bool found_locally, broker_t* broker,
                       local_state_t* opt_local_state);

static void lease_ref_count_dec(lease_t* lease);
static bool lease_consume_slot(lease_t* lease);

//

static void notify_other_of_match_v1(ask_ctx_t* ctx, match_t** our_match_ptr,
                                     match_t* opposite_match, ERL_NIF_TERM match_ref);

static void notify_other_of_match_v2(ask_ctx_t* ctx, match_t* opposite_match,
                                     ERL_NIF_TERM match_ref);

static void notify_self_of_match(ask_ctx_t* ctx, ERL_NIF_TERM our_tag, match_t* opposite_match,
                                 ERL_NIF_TERM match_ref);

static void notify_of_cancellation(ErlNifEnv* env, match_t* match, const drop_reason_t reason);
static ERL_NIF_TERM notify_of_faux_cancellation(ask_ctx_t* ctx, const drop_reason_t reason);

static void either_notify_or_assert_not_alive(ErlNifEnv* caller_env, ErlNifPid* pid,
                                              ErlNifEnv* msg_env, ERL_NIF_TERM msg);

//

static size_t batch_size(const size_t nr_of_cells);
static batch_t* batch_new(const batch_id_t id, const size_t nr_of_cells);
static void batch_init(batch_t* batch, const batch_id_t id);
static void batch_ref_count_inc(batch_t* batch);
static ERL_NIF_TERM batch_to_term(ErlNifEnv* env, const batch_t* batch);

//

static void ensure_one_entry_in_pool(mempool_t* pool, void* alloc_ctx);

static void batch_pool_init(mempool_t* pool, size_t nr_of_cells);
static batch_t* batch_pool_get(mempool_t* pool, size_t nr_of_cells);
static void* batch_pool_cb_alloc(void*);
static void batch_pool_cb_clear(void* obj);
static void batch_pool_cb_free(void* obj);

static void match_pool_init(mempool_t* pool);
static void* match_pool_cb_alloc(void*);
static void match_pool_cb_clear(void* obj);
static void match_pool_cb_free(void* obj);

static void tag_pool_init(mempool_t* pool);
static void* tag_pool_cb_alloc(void*);
static void tag_pool_cb_clear(void* obj);
static void tag_pool_cb_free(void* obj);

static void env_pool_init(mempool_t* pool);
static void* env_pool_cb_alloc(void*);
static void env_pool_cb_clear(void* obj);
static void env_pool_cb_free(void* obj);

//

static void mempool_init(mempool_t* pool, size_t initial_count, size_t size, void* alloc_ctx);
static void* mempool_get(mempool_t* pool, void* alloc_ctx);
static void mempool_return(mempool_t* pool, void* obj);
static void mempool_destroy(mempool_t* pool);
static ERL_NIF_TERM mempool_to_term(ErlNifEnv* env, mempool_t* pool);

//

static int get_boolean(ERL_NIF_TERM term, bool* out);
static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out_broker);
static int get_broker_opts(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM* out_bad_opt,
                           broker_opts_t* out_opts);

static int get_offer_size(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM offer, size_t* out_size);

#if USES_FLAT_SIZE
static int get_size_t(ErlNifEnv* env, ERL_NIF_TERM term, size_t* out);
#endif

static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, tag_t** out_tag);

//

static ERL_NIF_TERM make_badarg(ErlNifEnv* env, ERL_NIF_TERM term);
static ERL_NIF_TERM make_badopts(ErlNifEnv* env, ERL_NIF_TERM term);
static ERL_NIF_TERM make_badopt(ErlNifEnv* env, ERL_NIF_TERM term);
static ERL_NIF_TERM make_await(ErlNifEnv* env, ERL_NIF_TERM tag);
static ERL_NIF_TERM make_cancelled(ErlNifEnv* env, const int64_t sojourn_time);

static ERL_NIF_TERM make_drop(ErlNifEnv* env, const drop_reason_t reason,
                              const int64_t sojourn_time);

static ERL_NIF_TERM make_drop_reason(ErlNifEnv* env, const drop_reason_t reason);
static ERL_NIF_TERM make_error(ErlNifEnv* env, ERL_NIF_TERM reason);

static ERL_NIF_TERM make_match(ErlNifEnv* env, ERL_NIF_TERM match_ref, ERL_NIF_TERM offer,
                               ErlNifTime enqueue_ts);

static ERL_NIF_TERM raise_tuple2(ErlNifEnv* env, ERL_NIF_TERM reason_type,
                                 ERL_NIF_TERM reason_content);

//

static size_t term_size(ErlNifEnv* env, ERL_NIF_TERM term);
static inline void consume_timeslice(ErlNifEnv* env, const size_t copied_bytes);
static ErlNifTime monotonic_ts(void);
static unsigned ceil_log2(size_t value);

/*********************************************************************/

#define X(field, name) ERL_NIF_TERM field;
static struct {
    ATOM_LIST
} Atoms;
#undef X

//

static ErlNifFunc nif_funcs[] = {{"new", 0, nif_new, 0},
                                 {"new", 1, nif_new, 0},
                                 {"nif_ask", 7, nif_ask, 0},
                                 {"nif_cancel", 1, nif_cancel, 0},
                                 {"nif_debug_info", 1, nif_debug_info, 0}};

static struct {
    ErlNifResourceType* broker;
    ErlNifResourceType* tag;
} ResourceTypes;

static _Atomic(thread_id_t) next_thread_id = 0;
static _Thread_local thread_id_t my_thread_id = -1;

// Sentinel values used in batch cells
static match_t sentinel_match_cancelled;
static match_t sentinel_match_closed;
static match_t sentinel_match_success;

/*********************************************************************/

static int on_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info)
{
    init_atoms(caller_env);

    memset(&ResourceTypes, 0, sizeof(ResourceTypes));
    broker_resource_load(caller_env);
    tag_resource_load(caller_env);

    memset(&sentinel_match_cancelled, 0, sizeof(match_t));
    memset(&sentinel_match_closed, 0, sizeof(match_t));
    memset(&sentinel_match_success, 0, sizeof(match_t));

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

static void tag_resource_load(ErlNifEnv* caller_env)
{
    ErlNifResourceTypeInit callbacks = {tag_dtor, NULL, tag_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.tag =
        enif_init_resource_type(caller_env, "cbroker.tag", &callbacks, flags, &flags);
    assert(ResourceTypes.tag != NULL);
}

ERL_NIF_INIT(cbroker, nif_funcs, on_load, NULL, NULL, NULL);

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
    const size_t nr_of_schedulers = (size_t)sys_info.scheduler_threads;
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
    broker->tag_batch_shift = ceil_log2(broker->nr_of_cells_per_batch);
    broker->tag_offset_mask = (1ull << broker->tag_batch_shift) - 1;

    broker->global_lock = enif_mutex_create("cbroker.global_lock");
    batch_t* first_batch = global_state_init(&broker->global_state, broker->nr_of_cells_per_batch);
    local_states_init(broker->local_states, nr_of_schedulers, first_batch);

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

    if (enif_self(env, &ctx.self)) {
        ctx.self_term = enif_make_pid(env, &ctx.self);
    }
    else {
        return enif_make_badarg(env);
    }

    assert(argc == 7);

    ctx.broker_term = argv[0];
    ctx.side = argv[1];
    ctx.offer = argv[2];

    if (!get_offer_size(env, argv[3], ctx.offer, &ctx.offer_size)) {
        return make_badarg(env, argv[3]);
    }

    ERL_NIF_TERM ask_type = argv[4];
    ERL_NIF_TERM enqueue_ts_term = argv[5];
    ERL_NIF_TERM retry_nr_term = argv[6];

    //

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
    else if (ask_type == Atoms._non_blocking) {
        ctx.is_non_blocking = true;
    }
    else if (ask_type != Atoms._dynamic) {
        return make_badarg(env, ask_type);
    }

    if (!enif_get_int64(env, enqueue_ts_term, &ctx.enqueue_ts)) {
        return make_badarg(env, enqueue_ts_term);
    }
    LOG("[ask] enqueue_ts: %lld", ctx.enqueue_ts);

    if (!enif_get_int(env, retry_nr_term, &ctx.retry_nr) || ctx.retry_nr < 0) {
        return make_badarg(env, retry_nr_term);
    }

    /////////

    LOG("[ask] Getting local state");
    ctx.local_state = broker_local_state(ctx.broker);
    assert(ctx.local_state != NULL);

    if (ctx.local_state->is_closed) {
        return make_error(env, Atoms._broker_closed);
    }

    ensure_one_entry_in_pool(&ctx.local_state->match_pool, NULL);
    ensure_one_entry_in_pool(&ctx.local_state->env_pool, NULL);
    ensure_one_entry_in_pool(&ctx.local_state->tag_pool, NULL);

    ask_out_t out;
    memset(&out, 0, sizeof(ask_out_t));

    ERL_NIF_TERM match_res = ask_loop(&ctx, &out);

    ////

    //

    if (match_res == Atoms._await) {
        assert(!ctx.is_non_blocking);
        assert(out.batch != NULL);
        match_res = make_await(env, out.our_tag);
        out.our_match = NULL;
        out.our_tag = Atoms._none;

        /* TODO do it as well for non-blocking cancellations
         * that hit the very same offset
         */
        ask_await_preemptively_fill_batch_pool(&ctx, &out);
    }
    else if (match_res == Atoms._match) {
        match_t* our_match = out.our_match;
        match_t* opposite_match = out.opposite_match;
        out.opposite_match = NULL;

        assert(opposite_match != NULL);
        ERL_NIF_TERM match_ref = enif_make_ref(env);

        LOG("match: about to notify other");

        if (our_match != NULL) {
            notify_other_of_match_v1(&ctx, &our_match, opposite_match, match_ref);
        }
        else {
            notify_other_of_match_v2(&ctx, opposite_match, match_ref);
        }

        //

        if (ctx.is_async) {
            LOG("match: about to notify self");
            // If we didn't allocate a match, use a faux tag
            ERL_NIF_TERM our_tag = (our_match == NULL ? enif_make_ref(env) : out.our_tag);
            notify_self_of_match(&ctx, our_tag, opposite_match, match_ref);
            match_res = make_await(env, our_tag);
        }
        else {
            LOG("match: about to copy offer to return it");
            ERL_NIF_TERM opposite_offer = enif_make_copy(env, opposite_match->offer);
            ctx.copied_bytes += opposite_match->offer_size;
            match_res = make_match(env, match_ref, opposite_offer, ctx.enqueue_ts);
        }

        LOG("match: about to reclaim match");
        match_reclaim(opposite_match, true, ctx.local_state);
    }
    else if (match_res == Atoms._drop) {
        if (out.drop_reason == DROP_REASON_CLOSED) {
            match_res = make_error(env, Atoms._broker_closed);
        }
        else if (ctx.is_async) {
            ERL_NIF_TERM faux_tag = notify_of_faux_cancellation(&ctx, out.drop_reason);
            match_res = make_await(env, faux_tag);
        }
        else {
            int64_t sojourn_time = monotonic_ts() - ctx.enqueue_ts;
            match_res = make_drop(env, out.drop_reason, sojourn_time);
        }
    }
    else {
        assert(match_res == Atoms._retry);
    }

    //

    if (out.consume_slot) {
        LOG("match: about to consume slot");
        broker_consume_batch_slot(ctx.broker, ctx.local_state, out.batch);
    }

    //

    if (out.our_match != NULL) {
        bool demonitor_res =
            match_demonitor_and_reclaim(env, out.our_match, false, ctx.local_state);
        assert(demonitor_res);
        out.our_match = NULL;
    }

    //

    consume_timeslice(env, ctx.copied_bytes);

    if (match_res == Atoms._retry) {
        if (ctx.retry_nr >= MAX_ASK_RETRIES) {
            if (ctx.is_async) {
                ERL_NIF_TERM faux_tag =
                    notify_of_faux_cancellation(&ctx, DROP_REASON_TOO_MANY_RETRIES);
                return make_await(env, faux_tag);
            }
            else {
                int64_t sojourn_time = monotonic_ts() - ctx.enqueue_ts;
                return make_drop(env, DROP_REASON_TOO_MANY_RETRIES, sojourn_time);
            }
        }

        LOG_UNCOND("RETRYING");

        int retry_argc = 7;
        ERL_NIF_TERM retry_argv[7];
        memcpy(retry_argv, argv, 6 * sizeof(ERL_NIF_TERM));
        retry_argv[6] = enif_make_int(env, ctx.retry_nr + 1);
        return enif_schedule_nif(env, "nif_ask", 0, nif_ask, retry_argc, retry_argv);
    }
    else {
        return match_res;
    }
}

//

static ERL_NIF_TERM nif_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifPid self;
    tag_t* tag = NULL;

    if (!enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM tag_term = argv[0];

    LOG("[cancel] Resolving tag");
    if (!get_tag(env, tag_term, &tag)) {
        if (enif_is_ref(env, tag_term)) {
            // assume this to be a faux tag
            return Atoms._too_late;
        }
        return make_badarg(env, tag_term);
    }

    LOG("[cancel] demonitoring process");
    if (enif_demonitor_process(env, tag, &tag->mon) != 0) {
        // too late
        return Atoms._too_late;
    }

    match_t* match = tag->match;
    assert(match != NULL);
    assert(match->tag == tag);
    LOG("[cancel] Got match %p", match);
    LOG("[cancel] Match batch id: %llu", match->batch_id);
    LOG("[cancel] Match offset: %llu", match->offset);

    broker_t* broker = NULL;
    int get_broker_res = get_broker(match->env, match->broker_term, &broker);
    assert(get_broker_res);

    local_state_t* local_state = broker_local_state(broker);
    assert(local_state != NULL);

    lease_t lease;
    memset(&lease, 0, sizeof(lease_t));

    if (local_state->is_closed) {
        return Atoms._too_late;
    }

    LOG("[cancel] Checking out batch");
    if (!broker_checkout_batch(broker, local_state, match->batch_id, &lease)) {
        return Atoms._too_late;
    }

    batch_t* batch = lease.batch;
    ERL_NIF_TERM res;

    LOG("[cancel] asserting offset within bounds");
    assert(match->offset < batch->nr_of_cells);

    LOG("[cancel] Retrieving cell");
    cell_t* cell = &batch->cells[match->offset];

    //

    if (atomic_compare_exchange_strong(cell, &match, &sentinel_match_cancelled)) {
        LOG("[cancel] Consuming lease slot");
        lease_consume_slot(&lease);

        LOG("[cancel] Reclaiming match %p", match);
        ErlNifPid cancelled_pid = match->pid;
        ErlNifTime enqueue_ts = match->enqueue_ts;
        match_reclaim(match, true, local_state);
        match = NULL;

        LOG("enqueue_ts=%lld, monotonic_ts=%lld", enqueue_ts, monotonic_ts());

        int64_t sojourn_time = monotonic_ts() - enqueue_ts;
        res = make_cancelled(env, sojourn_time);

        if (enif_compare_pids(&cancelled_pid, &self)) {
            notify_of_cancellation(env, match, DROP_REASON_CANCELLED);
        }
    }
    else {
        assert(match == &sentinel_match_cancelled || match == &sentinel_match_closed ||
               match == &sentinel_match_success);
        res = Atoms._too_late;
    }

    //

    if (lease.batch != NULL && !lease.found_locally) {
        lease_ref_count_dec(&lease);
    }
    return res;
}

//

static ERL_NIF_TERM nif_debug_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    broker_t* broker = NULL;
    ERL_NIF_TERM broker_term = argv[0];

    if (!get_broker(env, broker_term, &broker)) {
        return make_badarg(env, broker_term);
    }

    local_state_t* local_state = broker_local_state(broker);
    assert(local_state != NULL);

    lease_t* leases = NULL;
    size_t nr_of_batches = 0;
    broker_checkout_all_batches(broker, local_state, &leases, &nr_of_batches);

    //

    ERL_NIF_TERM* batch_terms = enif_alloc(nr_of_batches * sizeof(ERL_NIF_TERM));

    for (size_t i = 0; i < nr_of_batches; i++) {
        lease_t* lease = &leases[i];
        batch_terms[i] = batch_to_term(env, lease->batch);
    }

    //

    broker_checkin_many_batches(broker, &leases, nr_of_batches);
    assert(leases == NULL);

    ERL_NIF_TERM batch_terms_list =
        enif_make_list_from_array(env, batch_terms, (unsigned)nr_of_batches);
    enif_free(batch_terms);

    ERL_NIF_TERM global_state_term = global_state_to_term(env, &broker->global_state);

    ERL_NIF_TERM local_state_terms_list =
        local_states_to_term(env, broker->local_states, broker->nr_of_schedulers);

    return enif_make_list8(
        env,
        //
        enif_make_tuple2(env, Atoms._creator, enif_make_pid(env, &broker->creator_pid)),
        //
        enif_make_tuple2(env, Atoms._nr_of_schedulers,
                         enif_make_uint64(env, broker->nr_of_schedulers)),
        //
        enif_make_tuple2(env, Atoms._nr_of_cells_per_batch,
                         enif_make_uint64(env, broker->nr_of_cells_per_batch)),
        //
        enif_make_tuple2(env, Atoms._tag_batch_shift,
                         enif_make_uint64(env, broker->tag_batch_shift)),
        //
        enif_make_tuple2(env, Atoms._tag_offset_mask,
                         enif_make_uint64(env, broker->tag_offset_mask)),
        //
        enif_make_tuple2(env, Atoms._global_state, global_state_term),
        //
        enif_make_tuple2(env, Atoms._local_states, local_state_terms_list),
        //
        enif_make_tuple2(env, Atoms._batches, batch_terms_list));
}

/*********************************************************************/

static size_t new_broker_size(const size_t nr_of_schedulers)
{
    return sizeof(broker_t) + (nr_of_schedulers * sizeof(local_state_t));
}

static batch_t* global_state_init(global_state_t* global_state, const size_t nr_of_cells_per_batch)
{
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;

    global_state->is_closed = false;
    global_state->batches = cbroker_omap_new();

    const batch_id_t first_batch_id = 1;
    batch_t* first_batch = batch_new(first_batch_id, nr_of_cells_per_batch);

    map_res = cbroker_omap_insert(global_state->batches, first_batch->id, first_batch);
    assert(map_res == CBROKER_OMAP_OK);

    batch_pool_init(&global_state->batch_pool, nr_of_cells_per_batch);

    return first_batch;
}

static void global_state_close(global_state_t* global_state)
{
    bool prev_value = atomic_exchange(&global_state->is_closed, true);
    assert(prev_value == false);
}

static ERL_NIF_TERM global_state_to_term(ErlNifEnv* env, global_state_t* global_state)
{
    return enif_make_list1(
        env,
        //
        enif_make_tuple2(env, Atoms._batch_pool, mempool_to_term(env, &global_state->batch_pool)));
}
/*********************************************************************/

static void local_states_init(local_state_t local_states[], const size_t nr_of_schedulers,
                              batch_t* first_batch)
{
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;
    assert(first_batch != NULL);

    for (size_t thread_id = 0; thread_id < nr_of_schedulers; thread_id++) {
        local_state_t* local_state = &local_states[thread_id];
        local_state->is_closed = false;
        local_state->batches = cbroker_omap_new();

        map_res = cbroker_omap_insert(local_state->batches, first_batch->id, first_batch);
        assert(map_res == CBROKER_OMAP_OK);
        batch_ref_count_inc(first_batch);

        LOG("[local_states_init] First batch is %llu", first_batch->id);
        local_state->left_id = first_batch->id;
        local_state->right_id = first_batch->id;

        match_pool_init(&local_state->match_pool);
        tag_pool_init(&local_state->tag_pool);
        env_pool_init(&local_state->env_pool);
    }
}

static void local_states_dirty_close(local_state_t local_states[], const size_t nr_of_schedulers)
{
    for (size_t thread_id = 0; thread_id < nr_of_schedulers; thread_id++) {
        local_state_t* local_state = &local_states[thread_id];
        local_state->is_closed = true; // dirty write
    }
}

static local_state_t* broker_local_state(broker_t* broker)
{
    const thread_id_t thread_id = get_or_assign_thread_id(broker->nr_of_schedulers);

    if (thread_id < 0) {
        return NULL;
    }

    assert((size_t)thread_id < broker->nr_of_schedulers);
    return &broker->local_states[thread_id];
}

static batch_t* local_state_get_batch(local_state_t* local_state, const batch_id_t batch_id)
{
    batch_t* batch = NULL;
    LOG("Looking up batch %llu", batch_id);
    cbroker_omap_lookup(local_state->batches, batch_id, (void**)&batch);
    return batch;
}

static ERL_NIF_TERM local_states_to_term(ErlNifEnv* env, local_state_t local_states[],
                                         const size_t nr_of_schedulers)
{
    ERL_NIF_TERM* local_state_terms = enif_alloc(nr_of_schedulers * sizeof(ERL_NIF_TERM));

    for (size_t thread_id = 0; thread_id < nr_of_schedulers; thread_id++) {
        local_state_t* local_state = &local_states[thread_id];

        ERL_NIF_TERM local_state_term = enif_make_list3(
            env,
            //
            enif_make_tuple2(env, Atoms._match_pool,
                             mempool_to_term(env, &local_state->match_pool)),
            //
            enif_make_tuple2(env, Atoms._env_pool, mempool_to_term(env, &local_state->env_pool)),
            //
            enif_make_tuple2(env, Atoms._tag_pool, mempool_to_term(env, &local_state->tag_pool)));

        local_state_terms[thread_id] = local_state_term;
    }

    ERL_NIF_TERM list =
        enif_make_list_from_array(env, local_state_terms, (unsigned)nr_of_schedulers);
    enif_free(local_state_terms);
    return list;
}

static thread_id_t get_or_assign_thread_id(const size_t nr_of_schedulers)
{
    if (my_thread_id == -1) {
        if (enif_thread_type() == ERL_NIF_THR_NORMAL_SCHEDULER) {
            my_thread_id = atomic_fetch_add_explicit(&next_thread_id, 1, memory_order_relaxed);
        }
        else {
            my_thread_id = -2;
        }
    }
    return my_thread_id;
}

/*********************************************************************/

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
            match_res = ask_batch(ctx, batch, out);

            if (match_res == Atoms._batch_full) {
                skipped_batch = batch;
                batch = NULL;
                match_res = Atoms._retry;
            }
            else if (match_res == Atoms._batch_consumed) {
                lease_t lease;
                lease_init(&lease, batch, true, ctx->broker, local_state);
                lease_ref_count_dec(&lease);
                batch = NULL;
                match_res = Atoms._retry;
            }
            else if (match_res == Atoms._retry) {
                continue;
            }
            else if (match_res != Atoms._drop) {
                out->batch = batch;
            }
        }

        if (batch == NULL) {
            if ((batch = ask_get_next_batch(ctx, batch_id)) == NULL) {
                match_res = Atoms._drop;
                out->drop_reason = DROP_REASON_CLOSED;
            }
        }

        batch_id = batch->id;

        if (skipped_batch != NULL) {
            batch_id_t opposite_id = (ctx->is_left ? local_state->right_id : local_state->left_id);
            if (opposite_id > skipped_batch->id) {
                lease_t skipped_lease;
                lease_init(&skipped_lease, skipped_batch, true, ctx->broker, local_state);
                lease_ref_count_dec(&skipped_lease);
            }
            skipped_batch = NULL;
        }
    }

    LOG("[ask_loop] match_res: %T", match_res);
    return match_res;
}

//

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
            next_batch =
                batch_pool_get(&global_state->batch_pool, ctx->broker->nr_of_cells_per_batch);
            assert(next_batch != NULL);
            batch_init(next_batch, next_batch_id);

            map_res = cbroker_omap_insert(global_state->batches, next_batch_id, next_batch);
            assert(map_res == CBROKER_OMAP_OK);
            atomic_store_explicit(&next_batch->ref_count, 2, memory_order_relaxed);
        }
        else {
            ref_count_t ref_count =
                1 + atomic_fetch_add_explicit(&next_batch->ref_count, 1, memory_order_seq_cst);
            LOG("ASC||| REF COUNT for batch %llu: %ll", next_batch_id, ref_count);
            assert(ref_count >= 1);
        }

        enif_mutex_unlock(broker->global_lock);

        map_res = cbroker_omap_insert(local_state->batches, next_batch_id, next_batch);
        assert(map_res == CBROKER_OMAP_OK);
    }

    //

    if (ctx->is_left) {
        local_state->left_id = next_batch_id;
    }
    else {
        local_state->right_id = next_batch_id;
    }

    return next_batch;
}

//

static ERL_NIF_TERM ask_batch(ask_ctx_t* ctx, batch_t* batch, ask_out_t* out)
{
    LOG("Asking batch %llu", batch->id);

    _Atomic(offset_t)* offset_counter = (ctx->is_left ? &batch->left_count : &batch->right_count);

    offset_t offset = atomic_fetch_add_explicit(offset_counter, 1, memory_order_relaxed);

    if (offset >= batch->nr_of_cells) {
        LOG("Batch %llu is full", batch->id);
        size_t consumed_count = atomic_load_explicit(&batch->consumed_count, memory_order_relaxed);

        if (consumed_count >= batch->nr_of_cells) {
            return Atoms._batch_consumed;
        }
        return Atoms._batch_full;
    }
    else {
        ERL_NIF_TERM match_res = ask_batch_offset(ctx, batch, offset, offset_counter, out);

        if (match_res == Atoms._retry || match_res == Atoms._drop) {
            return match_res;
        }
        else {
            out->offset = offset;
            return match_res;
        }
    }
}

//

static ERL_NIF_TERM ask_batch_offset(ask_ctx_t* ctx, batch_t* batch, offset_t offset,
                                     _Atomic(offset_t)* offset_counter, ask_out_t* out)
{
    LOG("[%T] Asking batch %llu, offset %llu", ctx->self_term, batch->id, offset);
    size_t cell_offset = offset % batch->nr_of_cells;
    LOG("[%T] cell_offset: %llu", ctx->self_term, cell_offset);

    cell_t* cell = &batch->cells[cell_offset];
    match_t* match = NULL;

    match = atomic_load(cell);

    // First

    if (match == NULL) {
        if (ctx->is_non_blocking) {
            offset_t expected_offset = offset + 1;
            if (atomic_compare_exchange_strong(offset_counter, &expected_offset, offset)) {
                // counter reverted to previous position
                out->drop_reason = DROP_REASON_NON_BLOCKING;
                return Atoms._drop;
            }
            else if (atomic_compare_exchange_strong(cell, &match, &sentinel_match_cancelled)) {
                // too late to revert counter, slot filled with cancellation instead
                out->consume_slot = true;
                out->drop_reason = DROP_REASON_NON_BLOCKING;
                return Atoms._drop;
            }
        }
        else {
            LOG("[%T] Allocating our own match", ctx->self_term);
            match_t* our_match = ask_ensure_our_match(ctx, batch->id, offset, out);

            LOG("[%T] Exchanging our own match expecting null", ctx->self_term);
            if (atomic_compare_exchange_strong(cell, &match, our_match)) {
                out->our_match = NULL;
                return Atoms._await;
            }
            LOG("[%T] Null-expecting exchange failed", ctx->self_term);
        }
    }

    // Second

    if (match == &sentinel_match_cancelled) {
        LOG("[%T] Match is too late: cancelled", ctx->self_term);
        return Atoms._retry;
    }
    else if (match == &sentinel_match_closed) {
        out->drop_reason = DROP_REASON_CLOSED;
        return Atoms._drop;
    }

    assert(match != &sentinel_match_success);
    LOG("[%T] Exchanging success expecting opposite match", ctx->self_term);

    if (atomic_compare_exchange_strong(cell, &match, &sentinel_match_success)) {
        LOG("[%T] Exchanging succeeded", ctx->self_term);
        tag_t* tag = (tag_t*)match->tag;
        assert(tag != NULL);

        if (match_demonitor(ctx->env, match)) {
            out->opposite_match = match;
            out->consume_slot = true;
            return Atoms._match;
        }
        else {
            // Too late, opposite monitor triggered or cancelled
            return Atoms._retry;
        }
    }

    // Cancelled

    assert(match == &sentinel_match_cancelled || match == &sentinel_match_closed);
    return Atoms._retry;
}

//

static match_t* ask_ensure_our_match(ask_ctx_t* ctx, const batch_id_t batch_id,
                                     const offset_t offset, ask_out_t* out)
{
    match_t* our_match = out->our_match;

    if (our_match == NULL) {
        out->our_match = ask_prepare_our_match(ctx, batch_id, offset);
        out->our_tag = enif_make_resource(ctx->env, out->our_match->tag);
    }
    else {
        LOG("[%T] Reusing already allocated match", ctx->self_term);
        our_match->batch_id = batch_id;
        our_match->offset = offset;
    }
    return out->our_match;
}

//

static match_t* ask_prepare_our_match(ask_ctx_t* ctx, batch_id_t batch_id, offset_t offset)
{
    LOG("[%T] [ask_prepare_our_match] New match!", ctx->self_term);
    local_state_t* local_state = ctx->local_state;

    LOG("[%T] [ask_prepare_our_match] Grabbing new match from local state", ctx->self_term);
    match_t* match = mempool_get(&local_state->match_pool, NULL);
    assert(match != NULL);
    LOG("[%T] match addr: %p", ctx->self_term, match);

    match->enqueue_ts = ctx->enqueue_ts;
    match->pid = ctx->self;
    match->env = mempool_get(&local_state->env_pool, NULL);

    LOG("[%T] [ask_prepare_our_match] Copying offer", ctx->self_term);
    match->offer = enif_make_copy(match->env, ctx->offer);
    match->offer_size = ctx->offer_size;
    ctx->copied_bytes += ctx->offer_size;

    match->broker_term = enif_make_copy(match->env, ctx->broker_term);
    ctx->copied_bytes += term_size(ctx->env, ctx->broker_term);

    match->batch_id = batch_id;
    match->offset = offset;

    tag_t* tag = mempool_get(&local_state->tag_pool, NULL);
    assert(tag != NULL);
    tag->match = match;
    match->tag = tag;

    LOG("[%T] [ask_prepare_our_match] Creating monitor", ctx->self_term);
    int mon_res = enif_monitor_process(ctx->env, tag, &match->pid, &tag->mon);
    assert(mon_res == 0);

    return match;
}

static void ask_await_preemptively_fill_batch_pool(ask_ctx_t* ctx, ask_out_t* out)
{
    broker_t* broker = ctx->broker;
    global_state_t* global_state = &broker->global_state;

    const batch_t* batch = out->batch;
    assert(batch != NULL);
    const offset_t offset = out->offset;

    if (offset == (batch->nr_of_cells >> 2) &&
        global_state->batch_pool.count <= BATCH_POOL_INITIAL_COUNT // dirty read
    ) {
        batch_t* new_batch = batch_new(0, ctx->broker->nr_of_cells_per_batch);
        enif_mutex_lock(broker->global_lock);
        mempool_return(&global_state->batch_pool, new_batch);
        enif_mutex_unlock(broker->global_lock);
    }
}

/*********************************************************************/

static bool match_demonitor(ErlNifEnv* caller_env, match_t* match)
{
    tag_t* tag = match->tag;
    assert(tag != NULL);

    if (enif_demonitor_process(caller_env, tag, &tag->mon) == 0) {
        return true;
    }
    else {
        enif_release_resource(tag);
        return false;
    }
}

static void match_reclaim(match_t* match, bool tag_used, local_state_t* opt_local_state)
{
    assert(match != NULL);

    ErlNifEnv* env = match->env;
    assert(env != NULL);
    match->env = NULL;

    tag_t* tag = match->tag;
    assert(tag != NULL);
    assert(tag->match == match);
    tag->match = NULL;
    match->tag = NULL;

    if (opt_local_state != NULL) {
        mempool_return(&opt_local_state->match_pool, match);
        mempool_return(&opt_local_state->env_pool, env);

        if (tag_used) {
            enif_release_resource(tag);
        }
        else {
            mempool_return(&opt_local_state->tag_pool, tag);
        }
    }
    else {
        enif_free(match);
        enif_free_env(env);
        enif_release_resource(tag);
    }
}

static bool match_demonitor_and_reclaim(ErlNifEnv* caller_env, match_t* match, bool tag_used,
                                        local_state_t* local_state)
{
    if (match_demonitor(caller_env, match)) {
        match_reclaim(match, tag_used, local_state);
        return true;
    }
    return false;
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

//

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

    local_state_t* opt_local_state = broker_local_state(broker);
    lease_t lease;
    memset(&lease, 0, sizeof(lease_t));

    if (broker_checkout_batch(broker, opt_local_state, batch_id, &lease)) {
        LOG("[match DOWN %T] batch found", pid_term, match->batch_id);
        batch_t* batch = lease.batch;
        assert(offset < batch->nr_of_cells);

        cell_t* cell = &batch->cells[offset];

        if (atomic_compare_exchange_strong(cell, &match, &sentinel_match_cancelled)) {
            LOG("[match DOWN %T] match cancelled", pid_term, match->batch_id);
            match_reclaim(match, true, opt_local_state);
            lease_consume_slot(&lease);
        }
        else {
            LOG("[match DOWN %T] Too late to cancel match", pid_term);
            assert((match == &sentinel_match_cancelled) || (match == &sentinel_match_closed) ||
                   (match == &sentinel_match_success));
        }

        if (lease.batch != NULL && !lease.found_locally) {
            lease_ref_count_dec(&lease);
        }
    }
}

/*********************************************************************/

static bool broker_checkout_batch(broker_t* broker, local_state_t* opt_local_state,
                                  batch_id_t batch_id, lease_t* out_lease)
{
    batch_t* batch = NULL;

    if (opt_local_state != NULL && (batch = local_state_get_batch(opt_local_state, batch_id))) {
        out_lease->batch = batch;
        out_lease->found_locally = true;
        out_lease->broker = broker;
        out_lease->opt_local_state = opt_local_state;
        return true;
    }
    else {
        global_state_t* global_state = &broker->global_state;
        enif_mutex_lock(broker->global_lock);

        if (cbroker_omap_lookup(global_state->batches, batch_id, (void**)&batch)) {
            atomic_fetch_add_explicit(&batch->ref_count, 1, memory_order_relaxed);
            enif_mutex_unlock(broker->global_lock);
            out_lease->batch = batch;
            out_lease->found_locally = false;
            out_lease->broker = broker;
            out_lease->opt_local_state = opt_local_state;
            return true;
        }
        else {
            enif_mutex_unlock(broker->global_lock);
            return false;
        }
    }
}

//

static void broker_consume_batch_slot(broker_t* broker, local_state_t* local_state, batch_t* batch)
{
    lease_t lease;
    memset(&lease, 0, sizeof(lease_t));

    lease.batch = batch;
    lease.found_locally = true;
    lease.broker = broker;
    lease.opt_local_state = local_state;
    lease_consume_slot(&lease);
}

//

static void broker_cancel_all_batch_cells(ErlNifEnv* env, broker_t* broker,
                                          ERL_NIF_TERM broker_term, local_state_t* local_state,
                                          batch_t* batch, bool did_broker_close)
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
        match_t* match = atomic_load(cell);

        if (match == &sentinel_match_cancelled || match == &sentinel_match_success) {
            continue;
        }

        assert(match != &sentinel_match_closed);

        if (atomic_compare_exchange_strong(cell, &match, &sentinel_match_closed)) {
            if (match != NULL) {
                tag_t* tag = (tag_t*)match->tag;
                assert(tag != NULL);

                if (enif_demonitor_process(env, tag, &tag->mon) == 0) {
                    notify_of_cancellation(env, match, DROP_REASON_CLOSED);
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

//

static void broker_checkout_all_batches(broker_t* broker, local_state_t* opt_local_state,
                                        lease_t** out_array, size_t* out_nr_of_batches)
{
    global_state_t* global_state = &broker->global_state;
    enif_mutex_lock(broker->global_lock);

    const size_t nr_of_batches = cbroker_omap_size(global_state->batches);
    const size_t array_size = nr_of_batches * sizeof(lease_t);
    lease_t* array = enif_alloc(array_size);
    memset(array, 0, array_size);

    batch_t** batches = (batch_t**)cbroker_omap_values(global_state->batches);

    for (size_t i = 0; i < nr_of_batches; i++) {
        batch_t* batch = batches[i];
        const batch_id_t batch_id = batch->id;

        lease_t* lease = &array[i];
        lease->batch = batch;

        lease->found_locally = (opt_local_state != NULL &&
                                cbroker_omap_lookup(opt_local_state->batches, batch_id, NULL));

        lease->broker = broker;
        lease->opt_local_state = opt_local_state;

        if (!lease->found_locally) {
            batch_ref_count_inc(batch);
        }
    }

    enif_mutex_unlock(broker->global_lock);

    *out_array = array;
    *out_nr_of_batches = nr_of_batches;
}

//

static void broker_checkin_many_batches(broker_t* broker, lease_t** array_ptr,
                                        const size_t nr_of_batches)
{
    lease_t* array = *array_ptr;

    for (size_t i = 0; i < nr_of_batches; i++) {
        lease_t* lease = &array[i];
        if (!lease->found_locally) {
            lease_ref_count_dec(lease);
        }
    }

    enif_free(array);
    *array_ptr = NULL;
}

//

static void broker_dtor(ErlNifEnv* caller_env, void* obj)
{
    broker_t* broker = (broker_t*)obj;
    enif_mutex_destroy(broker->global_lock);

    global_state_t* global_state = &broker->global_state;
    mempool_destroy(&global_state->batch_pool);

    //

    for (size_t thread_id = 0; thread_id < broker->nr_of_schedulers; thread_id++) {
        local_state_t* local_state = &broker->local_states[thread_id];
        void* destroy_ctx = global_state;
        cbroker_omap_destroy(local_state->batches, broker_dtor_cb_local_batch, destroy_ctx);
        local_state->batches = NULL;

        mempool_destroy(&local_state->match_pool);
        mempool_destroy(&local_state->env_pool);
        mempool_destroy(&local_state->tag_pool);
    }

    //

    cbroker_omap_destroy(global_state->batches, broker_dtor_cb_global_batch, NULL);
    global_state->batches = NULL;
}

static void broker_dtor_cb_local_batch(batch_id_t key, void* obj, void* ctx)
{
    // We don't actually free the batch here, we just make sure that it's present in global state
    global_state_t* global_state = (global_state_t*)ctx;
    batch_t* batch = (batch_t*)obj;
    bool present_in_global_state = cbroker_omap_lookup(global_state->batches, batch->id, NULL);
    assert(present_in_global_state);
}

static void broker_dtor_cb_global_batch(batch_id_t key, void* obj, void* ctx)
{
    assert(ctx == NULL);
    batch_t* batch = (batch_t*)obj;

    for (offset_t i = 0; i < batch->nr_of_cells; i++) {
        cell_t* cell = &batch->cells[i];
        match_t* match = atomic_load(cell);
        assert(match == NULL || match == &sentinel_match_cancelled ||
               match == &sentinel_match_closed || match == &sentinel_match_success);
    }

    enif_free(batch);
}

//

static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
    broker_t* broker = (broker_t*)obj;

    if (!broker->opts.depends_on_creator) {
        return;
    }

    global_state_close(&broker->global_state);
    local_states_dirty_close(broker->local_states, broker->nr_of_schedulers);

    local_state_t* local_state = broker_local_state(broker);
    lease_t* leases = NULL;
    size_t nr_of_batches = 0;
    broker_checkout_all_batches(broker, local_state, &leases, &nr_of_batches);

    //

    ERL_NIF_TERM broker_term = enif_make_resource(caller_env, broker);

    for (size_t i = 0; i < nr_of_batches; i++) {
        lease_t* lease = &leases[i];
        batch_t* batch = lease->batch;
        broker_cancel_all_batch_cells(caller_env, broker, broker_term, local_state, batch, true);

        if (lease->found_locally) {
            batch_t* taken_batch = NULL;
            cbroker_omap_take(local_state->batches, batch->id, (void**)&taken_batch);
            assert(taken_batch != NULL);
            assert(taken_batch == batch);
            lease->found_locally = false;
        }
    }

    //

    broker_checkin_many_batches(broker, &leases, nr_of_batches);
    assert(leases == NULL);
}

/*********************************************************************/

static void lease_init(lease_t* lease, batch_t* batch, const bool found_locally, broker_t* broker,
                       local_state_t* opt_local_state)
{
    memset(lease, 0, sizeof(lease_t));
    lease->batch = batch;
    lease->found_locally = found_locally;
    lease->broker = broker;
    lease->opt_local_state = opt_local_state;
}

//

static void lease_ref_count_dec(lease_t* lease)
{
    batch_t* batch = lease->batch;
    assert(batch != NULL);

    broker_t* broker = lease->broker;
    local_state_t* opt_local_state = lease->opt_local_state;

    batch_id_t batch_id = batch->id;
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;

    /* acq_rel, not relaxed: release so this thread's cell writes precede the
     * free below; acquire so the thread that observes 1 sees every other
     * dropper's writes
     */
    ref_count_t ref_count =
        (atomic_fetch_sub_explicit(&batch->ref_count, 1, memory_order_acq_rel) - 1);
    LOG("DESC REF COUNT for batch %u: %u", batch_id, ref_count);
    assert(ref_count >= 1);

    if (lease->found_locally) {
        assert(opt_local_state != NULL);
        cbroker_omap_delete_and_next(opt_local_state->batches, batch_id, NULL, NULL, NULL);
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
                    mempool_return(&global_state->batch_pool, batch);
                }
                else {
                    // We reuse the batch right away, ensuring batch IDs are not reused
                    // by a thread that lagged behind
                    const batch_id_t next_batch_id = batch_id + 1;
                    batch_init(batch, next_batch_id);
                    map_res = cbroker_omap_insert(global_state->batches, next_batch_id, batch);
                    assert(map_res == CBROKER_OMAP_OK);
                    batch = NULL;
                }
            }
        }

        enif_mutex_unlock(broker->global_lock);
    }

    lease->batch = NULL;
}

//

static bool lease_consume_slot(lease_t* lease)
{
    batch_t* batch = lease->batch;

    /* acq_rel: the incrementer that reaches nr_of_cells triggers teardown, so
     * this behaves as a reference release. See `batch_lower_ref_count`. */
    size_t consumed_count =
        1 + atomic_fetch_add_explicit(&batch->consumed_count, 1, memory_order_acq_rel);

    if (consumed_count < batch->nr_of_cells) {
        return false;
    }
    else {
        lease_ref_count_dec(lease);
        return true;
    }
}

/*********************************************************************/

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

    ctx->copied_bytes += (term_size(ctx->env, match_ref) + ((5 + 3) * sizeof(ERL_NIF_TERM)));

    either_notify_or_assert_not_alive(ctx->env, &opposite_match->pid, msg_env, msg);
}

static void notify_other_of_match_v2(ask_ctx_t* ctx, match_t* opposite_match,
                                     ERL_NIF_TERM match_ref)
{
    assert(opposite_match->tag != NULL);

    ErlNifEnv* env = ctx->env;

    ERL_NIF_TERM msg_content = make_match(env, match_ref, ctx->offer, opposite_match->enqueue_ts);
    ERL_NIF_TERM tag = enif_make_resource(env, opposite_match->tag);
    ERL_NIF_TERM msg = enif_make_tuple2(env, tag, msg_content);

    either_notify_or_assert_not_alive(ctx->env, &opposite_match->pid, NULL, msg);
}

static void notify_self_of_match(ask_ctx_t* ctx, ERL_NIF_TERM our_tag, match_t* opposite_match,
                                 ERL_NIF_TERM match_ref)
{
    ErlNifEnv* env = ctx->env;

    ERL_NIF_TERM offer_copy = enif_make_copy(ctx->env, opposite_match->offer);
    ERL_NIF_TERM msg_content = make_match(env, match_ref, offer_copy, ctx->enqueue_ts);
    ERL_NIF_TERM msg = enif_make_tuple2(env, our_tag, msg_content);

    either_notify_or_assert_not_alive(ctx->env, &ctx->self, NULL, msg);
}

static void notify_of_cancellation(ErlNifEnv* env, match_t* match, const drop_reason_t reason)
{
    assert(match->tag != NULL);

    ERL_NIF_TERM tag = enif_make_resource(env, match->tag);
    int64_t sojourn_time = monotonic_ts() - match->enqueue_ts;
    ERL_NIF_TERM cancelled = make_drop(env, reason, sojourn_time);
    ERL_NIF_TERM msg = enif_make_tuple2(env, tag, cancelled);
    either_notify_or_assert_not_alive(env, &match->pid, NULL, msg);
}

static ERL_NIF_TERM notify_of_faux_cancellation(ask_ctx_t* ctx, const drop_reason_t reason)
{
    ErlNifEnv* env = ctx->env;
    int64_t sojourn_time = monotonic_ts() - ctx->enqueue_ts;
    ERL_NIF_TERM faux_tag = enif_make_ref(env);
    ERL_NIF_TERM cancelled = make_drop(env, reason, sojourn_time);
    ERL_NIF_TERM msg = enif_make_tuple2(env, faux_tag, cancelled);
    either_notify_or_assert_not_alive(env, &ctx->self, NULL, msg);
    return faux_tag;
}

//

static void either_notify_or_assert_not_alive(ErlNifEnv* caller_env, ErlNifPid* pid,
                                              ErlNifEnv* msg_env, ERL_NIF_TERM msg)
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

static size_t batch_size(const size_t nr_of_cells)
{
    return sizeof(batch_t) + (nr_of_cells * sizeof(cell_t));
}

static batch_t* batch_new(const batch_id_t id, const size_t nr_of_cells)
{
    const size_t size = batch_size(nr_of_cells);
    batch_t* batch = enif_alloc(size);
    batch->nr_of_cells = nr_of_cells;
    batch_init(batch, id);
    return batch;
}

static void batch_init(batch_t* batch, const batch_id_t id)
{
    const size_t nr_of_cells = batch->nr_of_cells;
    memset(batch, 0, batch_size(nr_of_cells));
    batch->id = id;
    atomic_store(&batch->ref_count, 1);
    atomic_store(&batch->left_count, 0);
    atomic_store(&batch->right_count, 0);
    atomic_store(&batch->consumed_count, 0);
    batch->nr_of_cells = nr_of_cells;
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
        const match_t* match = atomic_load(cell);

        if (match == NULL) {
            cell_terms[i] = Atoms._empty;
        }
        else if (match == &sentinel_match_cancelled) {
            cell_terms[i] = Atoms._cancelled;
        }
        else if (match == &sentinel_match_closed) {
            cell_terms[i] = Atoms._broker_closed;
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

    ERL_NIF_TERM cell_terms_list =
        enif_make_list_from_array(env, cell_terms, (unsigned)nr_of_cells);
    enif_free(cell_terms);

    return enif_make_list6(
        env,
        //
        enif_make_tuple2(env, Atoms._id, enif_make_uint64(env, batch->id)),
        //
        enif_make_tuple2(env, Atoms._ref_count, enif_make_int64(env, ref_count)),
        //
        enif_make_tuple2(env, Atoms._left_count, enif_make_uint64(env, left_count)),
        //
        enif_make_tuple2(env, Atoms._right_count, enif_make_uint64(env, right_count)),
        //
        enif_make_tuple2(env, Atoms._consumed_count, enif_make_uint64(env, consumed_count)),
        //
        enif_make_tuple2(env, Atoms._cells, cell_terms_list));
}

/*********************************************************************/

static void ensure_one_entry_in_pool(mempool_t* pool, void* alloc_ctx)
{
    if (pool->count == 0) {
        assert(pool->size > 0);
        pool->array[pool->count++] = pool->alloc_cb(alloc_ctx);
    }
}

//

static void batch_pool_init(mempool_t* pool, size_t nr_of_cells)
{
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = batch_pool_cb_alloc;
    pool->clear_cb = batch_pool_cb_clear;
    pool->free_cb = batch_pool_cb_free;

    batch_pool_alloc_ctx_t alloc_ctx;
    memset(&alloc_ctx, 0, sizeof(batch_pool_alloc_ctx_t));
    alloc_ctx.nr_of_cells = nr_of_cells;

    mempool_init(pool, BATCH_POOL_INITIAL_COUNT, BATCH_POOL_SIZE, &nr_of_cells);
}

static batch_t* batch_pool_get(mempool_t* pool, size_t nr_of_cells)
{
    batch_pool_alloc_ctx_t alloc_ctx;
    memset(&alloc_ctx, 0, sizeof(batch_pool_alloc_ctx_t));
    alloc_ctx.nr_of_cells = nr_of_cells;
    return mempool_get(pool, &alloc_ctx);
}

static void* batch_pool_cb_alloc(void* alloc_ctx)
{
    batch_pool_alloc_ctx_t* ctx = (batch_pool_alloc_ctx_t*)alloc_ctx;
    const size_t size = batch_size(ctx->nr_of_cells);

    batch_t* batch = enif_alloc(size);
    memset(batch, 0, size);

    batch->nr_of_cells = ctx->nr_of_cells;
    return batch;
}

static void batch_pool_cb_clear(void* obj)
{
    batch_t* batch = (batch_t*)obj;
    const size_t nr_of_cells = batch->nr_of_cells;
    memset(batch, 0, sizeof(batch_t));
    batch->nr_of_cells = nr_of_cells;
}

static void batch_pool_cb_free(void* obj) { enif_free(obj); }

//

static void match_pool_init(mempool_t* pool)
{
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = match_pool_cb_alloc;
    pool->clear_cb = match_pool_cb_clear;
    pool->free_cb = match_pool_cb_free;
    mempool_init(pool, MATCH_POOLS_INITIAL_COUNT, MATCH_POOLS_SIZE, NULL);
}

static void* match_pool_cb_alloc(void* ctx)
{
    assert(ctx == NULL);
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

//

static void tag_pool_init(mempool_t* pool)
{
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = tag_pool_cb_alloc;
    pool->clear_cb = tag_pool_cb_clear;
    pool->free_cb = tag_pool_cb_free;
    mempool_init(pool, TAG_POOLS_INITIAL_COUNT, TAG_POOLS_SIZE, NULL);
}

static void* tag_pool_cb_alloc(void* ctx)
{
    assert(ctx == NULL);
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

//

static void env_pool_init(mempool_t* pool)
{
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = env_pool_cb_alloc;
    pool->clear_cb = env_pool_cb_clear;
    pool->free_cb = env_pool_cb_free;
    mempool_init(pool, ENV_POOLS_INITIAL_COUNT, ENV_POOLS_SIZE, NULL);
}

static void* env_pool_cb_alloc(void* ctx)
{
    assert(ctx == NULL);
    return enif_alloc_env();
}

static void env_pool_cb_clear(void* obj)
{
    ErlNifEnv* env = (ErlNifEnv*)obj;
    enif_clear_env(env);
}

static void env_pool_cb_free(void* obj) { enif_free_env(obj); }

/*********************************************************************/

static void mempool_init(mempool_t* pool, size_t initial_count, size_t size, void* alloc_ctx)
{
    assert(initial_count <= size);
    assert(size > 0);
    pool->count = initial_count;
    pool->size = size;
    pool->array = enif_alloc(pool->size * sizeof(void*));

    for (size_t i = 0; i < pool->count; i++) {
        pool->array[i] = pool->alloc_cb(alloc_ctx);
    }
}

static void* mempool_get(mempool_t* pool, void* alloc_ctx)
{
    void* obj = NULL;

    if (pool->count == 0) {
        obj = pool->alloc_cb(alloc_ctx);
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

static ERL_NIF_TERM mempool_to_term(ErlNifEnv* env, mempool_t* pool)
{
    return enif_make_uint64(env, pool->count);
}

/*********************************************************************/

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

static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out_broker)
{
    return enif_get_resource(env, term, ResourceTypes.broker, (void**)out_broker);
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

//

static int get_offer_size(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM offer, size_t* out_size)
{
#if USES_FLAT_SIZE
    size_t size_in_words = 0;

    if (get_size_t(env, term, &size_in_words)) {
        *out_size = size_in_words * sizeof(ERL_NIF_TERM);
        return 1;
    }
#else
    if (term == Atoms._compute_from_nif) {
        *out_size = enif_term_size(offer);
        return 1;
    }
#endif

    return 0;
}

//

#if USES_FLAT_SIZE
static int get_size_t(ErlNifEnv* env, ERL_NIF_TERM term, size_t* out)
{
    ErlNifUInt64 value;

    if (!enif_get_uint64(env, term, &value)) {
        return 0;
    }
#if SIZE_MAX < UINT64_MAX
    if (value > SIZE_MAX) {
        return 0;
    }
#endif
    *out = (size_t)value;
    return 1;
}
#endif

//

static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, tag_t** out_tag)
{
    return enif_get_resource(env, term, ResourceTypes.tag, (void**)out_tag);
}

/*********************************************************************/

static ERL_NIF_TERM make_await(ErlNifEnv* env, ERL_NIF_TERM tag)
{
    return enif_make_tuple2(env, Atoms._await, tag);
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

static ERL_NIF_TERM make_cancelled(ErlNifEnv* env, const int64_t sojourn_time)
{
    return enif_make_tuple2(env, Atoms._cancelled, enif_make_int64(env, sojourn_time));
}

static ERL_NIF_TERM make_drop(ErlNifEnv* env, const drop_reason_t reason, int64_t sojourn_time)
{
    ERL_NIF_TERM reason_term = make_drop_reason(env, reason);
    return enif_make_tuple3(env, Atoms._drop, reason_term, enif_make_int64(env, sojourn_time));
}

static ERL_NIF_TERM make_drop_reason(ErlNifEnv* env, const drop_reason_t reason)
{
    switch (reason) {
    case DROP_REASON_CANCELLED:
        return Atoms._cancelled;

    case DROP_REASON_NON_BLOCKING:
        return Atoms._match_unavailable;

    case DROP_REASON_TOO_MANY_RETRIES:
        return Atoms._broker_overloaded;

    default:
        assert(reason == DROP_REASON_CLOSED);
        return Atoms._broker_closed;
    }
}

static ERL_NIF_TERM make_error(ErlNifEnv* env, ERL_NIF_TERM reason)
{
    return enif_make_tuple2(env, Atoms._error, reason);
}

static ERL_NIF_TERM make_match(ErlNifEnv* env, ERL_NIF_TERM match_ref, ERL_NIF_TERM offer,
                               ErlNifTime enqueue_ts)
{
    int_fast64_t sojourn_time = monotonic_ts() - enqueue_ts;
    return enif_make_tuple4(env, Atoms._match, match_ref, offer,
                            enif_make_int64(env, sojourn_time));
}

static ERL_NIF_TERM raise_tuple2(ErlNifEnv* env, ERL_NIF_TERM reason_type,
                                 ERL_NIF_TERM reason_content)
{
    ERL_NIF_TERM reason = enif_make_tuple2(env, reason_type, reason_content);
    return enif_raise_exception(env, reason);
}

/*********************************************************************/

static size_t term_size(ErlNifEnv* env, ERL_NIF_TERM term)
{
#if USES_FLAT_SIZE
    if (enif_is_ref(env, term)) {
        return 7 * sizeof(ERL_NIF_TERM);
    }
    else {
        return 0;
    }
#else
    return enif_term_size(term);
#endif
}

//

static inline void consume_timeslice(ErlNifEnv* env, const size_t copied_bytes)
{
    if (copied_bytes == 0) {
        return;
    }

    // in memory words
    size_t copy_size = copied_bytes / sizeof(ERL_NIF_TERM);

    // ERTS_MSG_COPY_WORDS_PER_REDUCTION
    const size_t magic_v1 = 64;
    // CONTEXT_REDS
    const size_t magic_v2 = 4000;

    const size_t msg_copy_reds = copy_size / magic_v1;
    int percent = (int)MAX(1, MIN(100, (100 * msg_copy_reds) / magic_v2));

    if (percent != 0) {
        enif_consume_timeslice(env, percent);
    }
}

static ErlNifTime monotonic_ts() { return enif_monotonic_time(ERL_NIF_NSEC); }

static unsigned ceil_log2(size_t value)
{
    unsigned shift = 0;
    while (((size_t)1 << shift) < value) {
        shift++;
    }
    return shift;
}
