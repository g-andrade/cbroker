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

#define BATCH_POOL_SIZE 4
#define BATCH_POOL_INITIAL_COUNT 1

#define MATCH_POOLS_SIZE 8
#define MATCH_POOLS_INITIAL_COUNT 8

#define ENV_POOLS_SIZE 8
#define ENV_POOLS_INITIAL_COUNT 8

#define TAG_POOLS_SIZE 8
#define TAG_POOLS_INITIAL_COUNT TAG_POOLS_SIZE

//

#define MAX_ASK_RETRIES 10 // FIXME

//

/* The columns below are aligned on purpose. */
/* clang-format off */
#define ATOM_LIST \
    X(_approx_avg,            "approx_avg") \
    X(_async,                 "async") \
    X(_avg,                   "avg") \
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
    X(_count,                 "count") \
    X(_creator,               "creator") \
    X(_credits_left,          "credits_left") \
    X(_depends_on_creator,    "depends_on_creator") \
    X(_drop,                  "drop") \
    X(_dynamic,               "dynamic") \
    X(_empty,                 "empty") \
    X(_error,                 "error") \
    X(_false,                 "false") \
    X(_global_state,          "global_state") \
    X(_id,                    "id") \
    X(_left,                  "left")  \
    X(_left_count,            "left_count")  \
    X(_local_states,          "local_states")  \
    X(_match,                 "match") \
    X(_match_unavailable,     "match_unavailable") \
    X(_matched,               "matched") \
    X(_non_blocking,          "non_blocking") \
    X(_none,                  "none") \
    X(_nr_of_cells_per_batch, "nr_of_cells_per_batch") \
    X(_nr_of_schedulers,      "nr_of_schedulers") \
    X(_ref_count,             "ref_count") \
    X(_request_pool,          "request_pool") \
    X(_retry,                 "retry") \
    X(_right,                 "right") \
    X(_right_count,           "right_count") \
    X(_stats,                 "stats") \
    X(_stopped,               "stopped") \
    X(_sum,                   "sum") \
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
    ErlNifEnv* env;
    //
    ErlNifTime enqueue_ts;
    ErlNifPid pid;
    ERL_NIF_TERM offer;
    size_t offer_size;
    //
    ERL_NIF_TERM broker_term;
    batch_id_t batch_id;
    offset_t offset;
    void* tag;
} request_t;

//

typedef struct {
    ErlNifMonitor mon;
    request_t* request;
} tag_t;

//

typedef _Atomic(request_t*) cell_t;

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
    ErlNifMutex* lock;
    atomic_bool is_closed;
    cbroker_omap_t* batches;
    mempool_t batch_pool;
} global_state_t;

//

typedef struct {
    bool is_closed;
    cbroker_omap_t* batches;
    batch_id_t left_tail_id;
    batch_id_t right_tail_id;
    //
    mempool_t request_pool;
    mempool_t tag_pool;
} local_state_t;

//

typedef struct {
    bool depends_on_creator;
} broker_opts_t;

//

#define ROLLING_AVG_SIZE 128

typedef struct {
    atomic_size_t count;
    _Atomic(int64_t) sum;
    _Atomic(int64_t) samples[ROLLING_AVG_SIZE];
} rolling_avg_t;

typedef struct {
    rolling_avg_t credits_left;
} stats_t;

//

typedef struct {
    broker_opts_t opts;
    ErlNifPid creator_pid;
    ErlNifMonitor creator_mon;
    //
    size_t nr_of_schedulers;
    size_t nr_of_cells_per_batch;
    //
    global_state_t global_state;
    stats_t stats;
    //
    local_state_t local_states[];
} broker_t;

//

typedef struct {
    int nr;
    ErlNifTime enqueue_ts;
    ERL_NIF_TERM ask_type;
    request_t* request;
    ERL_NIF_TERM tag_term;
} retry_t;

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

typedef enum {
    ASK_STATE_INITIAL = 0,
    ASK_STATE_FOUND_BATCH = 1,
    ASK_STATE_HAVE_BATCH = 2,
    ASK_STATE_HAVE_OFFSET = 3,
    ASK_STATE_BATCH_FULL = 4,
    ASK_STATE_BATCH_CONSUMED = 5,
    ASK_STATE_DONE = 10
} ask_state_t;

//

typedef enum {
    ASK_RESULT_NONE = 0,
    ASK_RESULT_SKIP_BATCH,
    ASK_RESULT_AWAIT,
    ASK_RESULT_MATCHED,
    ASK_RESULT_NO_MATCH_AVAILABLE,
    ASK_RESULT_CLOSED,
    ASK_RESULT_OUT_OF_CREDITS
} ask_result_t;

//

typedef enum {
    MATCH_STATE_AWAIT = 1,
    MATCH_STATE_MATCHED = 2,
    MATCH_STATE_CLOSED = 4
} match_state_t;

//

typedef struct {
    ErlNifEnv* env;
    const ERL_NIF_TERM* argv;
    ErlNifTime enqueue_ts;
    ErlNifPid self;
    ERL_NIF_TERM self_term;
    //
    ERL_NIF_TERM broker_term;
    ERL_NIF_TERM side;
    ERL_NIF_TERM offer;
    ptrdiff_t offer_size; // negative when it hasn't been computed yet
    retry_t* retry;
    //
    broker_t* broker;
    bool is_left;
    bool is_async;
    bool is_non_blocking;
    global_state_t* global_state;
    batch_id_t* tail_id_ptr;
    batch_id_t* opposite_tail_id_ptr;
    local_state_t* local_state;
    size_t copied_bytes;
    //
    //
    // ask_state_t ask_state;
    int credits;
    batch_t* batch;
    _Atomic(offset_t)* offset_counter;
    offset_t offset;
    //
    lease_t lease;
    request_t* request;
    ERL_NIF_TERM tag_term;
    request_t* counter_request;
    bool consume_slot;
    ERL_NIF_TERM term_res;
} ask_ctx_t;

//

typedef struct {
    batch_t* batch;
    offset_t offset;
    bool consume_slot;
    // optional, reuse to notify counter-party if we allocated it but ended up in 2nd place
    request_t* our_request;
    ERL_NIF_TERM our_tag;
    request_t* opposite_request;
    drop_reason_t drop_reason;
} ask_out_t;

/*********************************************************************/

static int on_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info);
static void init_atoms(ErlNifEnv* caller_env);
static void load_broker_resource(ErlNifEnv* caller_env);
static void load_tag_resource(ErlNifEnv* caller_env);
static void load_retry_resource(ErlNifEnv* caller_env);

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

static ask_result_t ask_loop(ask_ctx_t* ctx);

static void ask_loop_tail_get(ask_ctx_t* ctx);
static ask_result_t ask_loop_tail_ask(ask_ctx_t* ctx);
static ask_result_t ask_loop_tail_offset_ask(ask_ctx_t* ctx, lease_t* lease, const offset_t offset,
                                             _Atomic(offset_t)* offset_counter);
static void ask_loop_tail_skip(ask_ctx_t* ctx, const batch_id_t batch_id);

static request_t* ask_loop_request_prepare(ask_ctx_t* ctx, const batch_id_t batch_id,
                                           const offset_t offset);
static void ask_loop_request_new(ask_ctx_t* ctx);

static void ask_reply_await(ask_ctx_t* ctx);
static void ask_reply_match(ask_ctx_t* ctx);
static void ask_reply_match_notify_other(ask_ctx_t* ctx, ERL_NIF_TERM match_ref);
static ERL_NIF_TERM ask_reply_match_self(ask_ctx_t* ctx, ERL_NIF_TERM match_ref);
static void ask_reply_nomatch(ask_ctx_t* ctx);
static void ask_reply_closed(ask_ctx_t* ctx);

static bool ask_retry_can(ask_ctx_t* ctx);
static void ask_retry(ask_ctx_t* ctx, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM ask_retry_schedule(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static void ask_retry_clear(ask_ctx_t* ctx, const bool expect_no_request);
static void ask_drop(ask_ctx_t* ctx, drop_reason_t reason);

//

static bool request_demonitor(ErlNifEnv* caller_env, request_t* request);
static void request_reclaim(request_t* request, bool tag_used, local_state_t* opt_local_state);
static bool request_demonitor_and_reclaim(ErlNifEnv* caller_env, request_t* request, bool tag_used,
                                          local_state_t* local_state);

//

static void tag_dtor(ErlNifEnv* caller_env, void* obj);
static void tag_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon);

//

static void retry_dtor(ErlNifEnv* caller_env, void* obj);

//

static bool broker_checkout_batch(broker_t* broker, local_state_t* opt_local_state,
                                  batch_id_t batch_id, lease_t* out_lease);

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

static void notify_of_cancellation(ErlNifEnv* env, request_t* request, const drop_reason_t reason);

static bool either_notify_or_assert_not_alive(ErlNifEnv* caller_env, ErlNifPid* pid,
                                              ErlNifEnv* msg_env, ERL_NIF_TERM msg);

//

static size_t batch_size(const size_t nr_of_cells);
static batch_t* batch_new(const batch_id_t id, const size_t nr_of_cells);
static void batch_init(batch_t* batch, const batch_id_t id);
static void batch_ref_count_inc(batch_t* batch);
static bool batch_is_consumed(batch_t* batch);
static ERL_NIF_TERM batch_to_term(ErlNifEnv* env, const batch_t* batch);

//

static void ensure_one_entry_in_pool(mempool_t* pool, void* alloc_ctx);

static void batch_pool_init(mempool_t* pool, size_t nr_of_cells);
static batch_t* batch_pool_get(mempool_t* pool, size_t nr_of_cells);
static void* batch_pool_cb_alloc(void*);
static void batch_pool_cb_clear(void* obj);
static void batch_pool_cb_free(void* obj);

static void request_pool_init(mempool_t* pool);
static void* request_pool_cb_alloc(void*);
static void request_pool_cb_clear(void* obj);
static void request_pool_cb_free(void* obj);

static void tag_pool_init(mempool_t* pool);
static void* tag_pool_cb_alloc(void*);
static void tag_pool_cb_clear(void* obj);
static void tag_pool_cb_free(void* obj);

//

static void mempool_init(mempool_t* pool, size_t initial_count, size_t size, void* alloc_ctx);
static void* mempool_get(mempool_t* pool, void* alloc_ctx);
static void mempool_return(mempool_t* pool, void* obj);
static void mempool_destroy(mempool_t* pool);
static ERL_NIF_TERM mempool_to_term(ErlNifEnv* env, mempool_t* pool);

//

static void stats_push_after_ask(stats_t* stats, int credits_left);
static ERL_NIF_TERM stats_to_term(ErlNifEnv* env, stats_t* stats);
static void rolling_avg_push(rolling_avg_t* rolling_avg, int64_t sample);
static ERL_NIF_TERM rolling_avg_to_term(ErlNifEnv* env, rolling_avg_t* rolling_avg);

//

static int get_boolean(ERL_NIF_TERM term, bool* out);
static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out_broker);
static int get_broker_opts(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM* out_bad_opt,
                           broker_opts_t* out_opts);

static int get_offer_size(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM offer,
                          ptrdiff_t* out_size);
static int get_retry(ErlNifEnv* env, ERL_NIF_TERM term, retry_t** out_retry);

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
                               int64_t sojourn_time);

static ERL_NIF_TERM raise_tuple2(ErlNifEnv* env, ERL_NIF_TERM reason_type,
                                 ERL_NIF_TERM reason_content);

//

static size_t term_size(ErlNifEnv* env, ERL_NIF_TERM term);
static inline void consume_timeslice(ErlNifEnv* env, const size_t copied_bytes);
static ErlNifTime monotonic_ts(void);

/*********************************************************************/

#define X(field, name) ERL_NIF_TERM field;
static struct {
    ATOM_LIST
} Atoms;
#undef X

//

static ErlNifFunc nif_funcs[] = {{"new", 1, nif_new, 0},
                                 {"ask", 5, nif_ask, 0},
                                 {"cancel", 1, nif_cancel, 0},
                                 {"debug_info", 1, nif_debug_info, 0}};

static struct {
    ErlNifResourceType* broker;
    ErlNifResourceType* tag;
    ErlNifResourceType* retry;
} ResourceTypes;

static _Atomic(thread_id_t) next_thread_id = 0;
static _Thread_local thread_id_t my_thread_id = -1;

// Sentinel values used in batch cells
static request_t sentinel_request_cancelled;
static request_t sentinel_request_matched;

/*********************************************************************/

static int on_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info)
{
    init_atoms(caller_env);

    memset(&ResourceTypes, 0, sizeof(ResourceTypes));
    load_broker_resource(caller_env);
    load_tag_resource(caller_env);
    load_retry_resource(caller_env);

    memset(&sentinel_request_cancelled, 0, sizeof(request_t));
    memset(&sentinel_request_matched, 0, sizeof(request_t));

    return 0;
}

static void init_atoms(ErlNifEnv* caller_env)
{
    memset(&Atoms, 0, sizeof(Atoms));
#define X(field, name) Atoms.field = enif_make_atom(caller_env, name);
    ATOM_LIST
#undef X
}

static void load_broker_resource(ErlNifEnv* caller_env)
{
    ErlNifResourceTypeInit callbacks = {broker_dtor, NULL, broker_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.broker =
        enif_init_resource_type(caller_env, "cbroker", &callbacks, flags, &flags);
    assert(ResourceTypes.broker != NULL);
}

static void load_tag_resource(ErlNifEnv* caller_env)
{
    ErlNifResourceTypeInit callbacks = {tag_dtor, NULL, tag_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.tag =
        enif_init_resource_type(caller_env, "cbroker.tag", &callbacks, flags, &flags);
    assert(ResourceTypes.tag != NULL);
}

static void load_retry_resource(ErlNifEnv* caller_env)
{
    ErlNifResourceTypeInit callbacks = {retry_dtor, NULL, NULL, 1, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.retry =
        enif_init_resource_type(caller_env, "cbroker.retry", &callbacks, flags, &flags);
    assert(ResourceTypes.retry != NULL);
}

ERL_NIF_INIT(cbroker_nif, nif_funcs, on_load, NULL, NULL, NULL);

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
    ctx.tag_term = Atoms._none;

    ctx.env = env;
    ctx.argv = argv;

    if (enif_self(env, &ctx.self)) {
        ctx.self_term = enif_make_pid(env, &ctx.self);
    }
    else {
        return enif_make_badarg(env);
    }

    assert(argc == 5);

    ctx.broker_term = argv[0];
    ctx.side = argv[1];
    ctx.offer = argv[2];

    if (!get_offer_size(env, argv[3], ctx.offer, &ctx.offer_size)) {
        return make_badarg(env, argv[3]);
    }

    ERL_NIF_TERM ask_type_arg = argv[4];
    ERL_NIF_TERM ask_type = Atoms._none;

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

    //

    if (get_retry(env, ask_type_arg, &ctx.retry)) {
        ctx.enqueue_ts = ctx.retry->enqueue_ts;
        ask_type = ctx.retry->ask_type;
    }
    else {
        ctx.enqueue_ts = monotonic_ts();
        ask_type = ask_type_arg;
    }

    if (ask_type == Atoms._async) {
        ctx.is_async = true;
    }
    else if (ask_type == Atoms._non_blocking) {
        ctx.is_non_blocking = true;
    }
    else if (ask_type != Atoms._dynamic) {
        return make_badarg(env, ask_type_arg);
    }

    /////////

    ctx.global_state = &ctx.broker->global_state;

    LOG("[ask] Getting local state");
    ctx.local_state = broker_local_state(ctx.broker);
    assert(ctx.local_state != NULL);

    if (ctx.local_state->is_closed) {
        return make_error(env, Atoms._broker_closed);
    }

    if (ctx.is_left) {
        ctx.tail_id_ptr = &ctx.local_state->left_tail_id;
        ctx.opposite_tail_id_ptr = &ctx.local_state->right_tail_id;
    }
    else {
        ctx.tail_id_ptr = &ctx.local_state->right_tail_id;
        ctx.opposite_tail_id_ptr = &ctx.local_state->left_tail_id;
    }

    ensure_one_entry_in_pool(&ctx.local_state->request_pool, NULL);
    ensure_one_entry_in_pool(&ctx.local_state->tag_pool, NULL);

    ctx.credits = 400;
    ctx.term_res = Atoms._none;

    ask_result_t ask_res = ask_loop(&ctx);
    stats_push_after_ask(&ctx.broker->stats, ctx.credits);

    //

    if (ctx.consume_slot) {
        lease_consume_slot(&ctx.lease);
    }

    if (ctx.counter_request != NULL) {
        request_reclaim(ctx.counter_request, true, ctx.local_state);
        ctx.counter_request = NULL;
    }

    //

    if (ask_res == ASK_RESULT_OUT_OF_CREDITS) {
        if (ask_retry_can(&ctx)) {
            ask_retry(&ctx, argc, argv);
        }
        else {
            ask_retry_clear(&ctx, false);
            ask_drop(&ctx, DROP_REASON_TOO_MANY_RETRIES);
        }
    }
    else {
        ask_retry_clear(&ctx, true);
    }

    if (ctx.request != NULL) {
        bool demonitor_res =
            request_demonitor_and_reclaim(env, ctx.request, false, ctx.local_state);
        assert(demonitor_res);
        ctx.request = NULL;
        ctx.tag_term = Atoms._none;
    }

    consume_timeslice(env, ctx.copied_bytes);

    assert(ctx.term_res != Atoms._none);
    return ctx.term_res;
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

    request_t* request = tag->request;
    assert(request != NULL);
    assert(request->tag == tag);
    LOG("[cancel] Got request %p", request);
    LOG("[cancel] Request batch id: %llu", request->batch_id);
    LOG("[cancel] Request offset: %llu", request->offset);

    broker_t* broker = NULL;
    int get_broker_res = get_broker(request->env, request->broker_term, &broker);
    assert(get_broker_res);

    local_state_t* local_state = broker_local_state(broker);
    assert(local_state != NULL);

    lease_t lease;
    memset(&lease, 0, sizeof(lease_t));

    if (local_state->is_closed) {
        return Atoms._too_late;
    }

    LOG("[cancel] Checking out batch");
    if (!broker_checkout_batch(broker, local_state, request->batch_id, &lease)) {
        return Atoms._too_late;
    }

    batch_t* batch = lease.batch;
    ERL_NIF_TERM res;

    LOG("[cancel] asserting offset within bounds");
    assert(request->offset < batch->nr_of_cells);

    LOG("[cancel] Retrieving cell");
    cell_t* cell = &batch->cells[request->offset];

    //

    if (atomic_compare_exchange_strong(cell, &request, &sentinel_request_cancelled)) {
        LOG("[cancel] Consuming lease slot");
        lease_consume_slot(&lease);

        LOG("[cancel] Reclaiming request %p", request);
        ErlNifPid cancelled_pid = request->pid;
        ErlNifTime enqueue_ts = request->enqueue_ts;
        request_reclaim(request, true, local_state);
        request = NULL;

        LOG("enqueue_ts=%lld, monotonic_ts=%lld", enqueue_ts, monotonic_ts());

        int64_t sojourn_time = monotonic_ts() - enqueue_ts;
        res = make_cancelled(env, sojourn_time);

        if (enif_compare_pids(&cancelled_pid, &self)) {
            notify_of_cancellation(env, request, DROP_REASON_CANCELLED);
        }
    }
    else {
        assert(request == &sentinel_request_cancelled || request == &sentinel_request_matched);
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

    ERL_NIF_TERM stats_term = stats_to_term(env, &broker->stats);

    return enif_make_list7(
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
        enif_make_tuple2(env, Atoms._global_state, global_state_term),
        //
        enif_make_tuple2(env, Atoms._local_states, local_state_terms_list),
        //
        enif_make_tuple2(env, Atoms._stats, stats_term),
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

    global_state->lock = enif_mutex_create("cbroker.global_state.lock");
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
        local_states->left_tail_id = first_batch->id;
        local_states->right_tail_id = first_batch->id;

        request_pool_init(&local_state->request_pool);
        tag_pool_init(&local_state->tag_pool);
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

        ERL_NIF_TERM local_state_term = enif_make_list2(
            env,
            //
            enif_make_tuple2(env, Atoms._request_pool,
                             mempool_to_term(env, &local_state->request_pool)),
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

static ask_result_t ask_loop(ask_ctx_t* ctx)
{
    broker_t* broker = ctx->broker;
    local_state_t* local_state = ctx->local_state;
    ask_result_t ask_res = 0;

    lease_t* lease = &ctx->lease;
    lease_init(lease, NULL, false, broker, local_state);

    while (ctx->credits > 0) {
        if (lease->batch == NULL) {
            ask_loop_tail_get(ctx);
            assert(lease->batch != NULL);
        }
        assert(lease->found_locally);

        ask_res = ask_loop_tail_ask(ctx);

        switch (ask_res) {
        case ASK_RESULT_SKIP_BATCH:
            ask_loop_tail_skip(ctx, ctx->lease.batch->id);
            continue;
        //
        case ASK_RESULT_AWAIT:
            ask_reply_await(ctx);
            break;
        //
        case ASK_RESULT_MATCHED:
            ask_reply_match(ctx);
            break;
        //
        case ASK_RESULT_NO_MATCH_AVAILABLE:
            ask_reply_nomatch(ctx);
            break;
        //
        case ASK_RESULT_CLOSED:
            ask_reply_closed(ctx);
            break;
        //
        default:
            break;
        }
        return ask_res;
    }

    return ASK_RESULT_OUT_OF_CREDITS;
}

static void ask_loop_tail_get(ask_ctx_t* ctx)
{
    lease_t* lease = &ctx->lease;
    local_state_t* local_state = ctx->local_state;

    batch_id_t tail_id = *(ctx->tail_id_ptr);
    batch_t* batch = NULL;

    if (cbroker_omap_lookup(local_state->batches, tail_id, (void**)&batch)) {
        assert(batch != NULL);
        lease->batch = batch;
        lease->found_locally = true;
    }
    else {
        ask_loop_tail_skip(ctx, tail_id);
    }
}

static ask_result_t ask_loop_tail_ask(ask_ctx_t* ctx)
{
    lease_t* lease = &ctx->lease;
    batch_t* batch = lease->batch;
    assert(batch != NULL);

    _Atomic(offset_t)* offset_counter = (ctx->is_left ? &batch->left_count : &batch->right_count);

    while (ctx->credits-- > 0) {
        const offset_t offset = atomic_fetch_add_explicit(offset_counter, 1, memory_order_relaxed);

        if (offset >= batch->nr_of_cells) {
            return ASK_RESULT_SKIP_BATCH;
        }

        ask_result_t ask_res = ask_loop_tail_offset_ask(ctx, lease, offset, offset_counter);

        if (ask_res) {
            return ask_res;
        }
        else if (ctx->local_state->is_closed) {
            return ASK_RESULT_CLOSED;
        }
    }

    return ASK_RESULT_OUT_OF_CREDITS;
}

static ask_result_t ask_loop_tail_offset_ask(ask_ctx_t* ctx, lease_t* lease, const offset_t offset,
                                             _Atomic(offset_t)* offset_counter)
{
    batch_t* batch = lease->batch;
    assert(batch != NULL);
    assert(offset < batch->nr_of_cells);

    cell_t* cell = &batch->cells[offset];
    request_t* counter_request = atomic_load(cell);

    // Are we first?

    if (counter_request == NULL) {
        if (ctx->is_non_blocking) {
            offset_t expected_offset_counter = offset + 1;

            if (atomic_compare_exchange_strong(offset_counter, &expected_offset_counter, offset)) {
                // We managed to revert the counter
                return ASK_RESULT_NO_MATCH_AVAILABLE;
            }
            else if (atomic_compare_exchange_strong(cell, &counter_request,
                                                    &sentinel_request_cancelled)) {
                // We managed to cancel this slot
                ctx->consume_slot = true;
                return ASK_RESULT_NO_MATCH_AVAILABLE;
            }
            // too late
        }
        else {
            request_t* request = ask_loop_request_prepare(ctx, batch->id, offset);

            if (atomic_compare_exchange_strong(cell, &counter_request, request)) {
                // Enqueued
                ctx->request = NULL;
                return ASK_RESULT_AWAIT;
            }
        }
    }

    // We're definitely second

    if (counter_request == &sentinel_request_cancelled) {
        // if (ctx->is_non_blocking) {
        //     return ASK_RESULT_NO_MATCH_AVAILABLE;
        // }
        return ASK_RESULT_NONE;
    }

    assert(counter_request != &sentinel_request_matched);

    if (atomic_compare_exchange_strong(cell, &counter_request, &sentinel_request_matched)) {
        assert(counter_request != NULL);

        if (request_demonitor(ctx->env, counter_request)) {
            // Matched!
            assert(counter_request->offer_size >= 0);
            ctx->counter_request = counter_request;
            ctx->consume_slot = true;
            return ASK_RESULT_MATCHED;
        }
        return ASK_RESULT_NONE;
    }

    assert(counter_request == &sentinel_request_cancelled);
    return ASK_RESULT_NONE;
}

static void ask_loop_tail_skip(ask_ctx_t* ctx, const batch_id_t batch_id)
{
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;
    lease_t* lease = &ctx->lease;
    broker_t* broker = ctx->broker;
    local_state_t* local_state = ctx->local_state;

    assert(lease->batch == NULL || lease->batch->id == batch_id);

    batch_t* next_batch = NULL;

    //

    if (!cbroker_omap_next(local_state->batches, batch_id, NULL, (void**)&next_batch)) {
        global_state_t* global_state = ctx->global_state;
        enif_mutex_lock(global_state->lock);

        batch_t** all_next = NULL;
        batch_t* one_of_next = NULL;

        size_t all_next_count =
            cbroker_omap_all_next(global_state->batches, batch_id, NULL, (void***)&all_next);

        size_t all_next_size = all_next_count * sizeof(batch_t*);
        batch_t** batches_to_checkout = enif_alloc(all_next_size);
        size_t checkout_amount = 0;

        for (size_t i = 0; i < all_next_count; i++) {
            one_of_next = all_next[i];
            assert(one_of_next != NULL);
            assert(one_of_next->id > batch_id);
            if (!batch_is_consumed(one_of_next)) {
                batch_ref_count_inc(one_of_next);
                batches_to_checkout[checkout_amount++] = one_of_next;
            }
        }

        if (checkout_amount == 0) {
            batch_id_t tail_id =
                (all_next_count == 0 ? batch_id + 1 : all_next[all_next_count - 1]->id + 1);

            next_batch = batch_pool_get(&global_state->batch_pool, broker->nr_of_cells_per_batch);
            batch_init(next_batch, tail_id);
            map_res = cbroker_omap_insert(global_state->batches, tail_id, next_batch);
            assert(map_res == CBROKER_OMAP_OK);
            batch_ref_count_inc(next_batch);

            enif_mutex_unlock(global_state->lock);

            map_res = cbroker_omap_insert(local_state->batches, next_batch->id, next_batch);
            assert(map_res == CBROKER_OMAP_OK);
        }
        else {
            enif_mutex_unlock(global_state->lock);

            for (size_t i = 0; i < checkout_amount; i++) {
                one_of_next = batches_to_checkout[i];
                map_res = cbroker_omap_insert(local_state->batches, one_of_next->id, one_of_next);
                assert(map_res == CBROKER_OMAP_OK);
            }

            next_batch = batches_to_checkout[checkout_amount - 1];
            enif_free(batches_to_checkout);
        }

        assert(next_batch != NULL);
        assert(next_batch->id > batch_id);
    }

    *(ctx->tail_id_ptr) = next_batch->id;

    if (*(ctx->opposite_tail_id_ptr) > batch_id && lease->batch != NULL) {
        lease_ref_count_dec(lease);
    }

    lease->batch = next_batch;
    lease->found_locally = true;
}

//

static request_t* ask_loop_request_prepare(ask_ctx_t* ctx, const batch_id_t batch_id,
                                           const offset_t offset)
{
    request_t* request = ctx->request;
    ERL_NIF_TERM tag_term = Atoms._none;

    if (request == NULL) {
        retry_t* retry = ctx->retry;

        if (retry != NULL) {
            request = retry->request;
            tag_term = retry->tag_term;
            retry->request = NULL;
            retry->tag_term = Atoms._none;
        }

        if (request == NULL) {
            ask_loop_request_new(ctx);
        }
        else {
            ctx->tag_term = tag_term;
        }

        request = ctx->request;
        tag_term = ctx->tag_term;
    }

    assert(request != NULL);
    assert(tag_term != Atoms._none);

    request->batch_id = batch_id;
    request->offset = offset;
    return request;
}

static void ask_loop_request_new(ask_ctx_t* ctx)
{
    assert(ctx->request == NULL);
    assert(ctx->tag_term == Atoms._none);

    local_state_t* local_state = ctx->local_state;
    request_t* request = mempool_get(&local_state->request_pool, NULL);

    request->enqueue_ts = ctx->enqueue_ts;
    request->pid = ctx->self;

    if (ctx->offer_size >= 0) {
        request->offer_size = (size_t)ctx->offer_size;
    }
    else {
        assert(!USES_FLAT_SIZE);
        request->offer_size = (size_t)term_size(ctx->env, ctx->offer);
        ctx->offer_size = (ptrdiff_t)request->offer_size;
    }

    request->broker_term = enif_make_copy(request->env, ctx->broker_term);

    if (ctx->offer_size == 0) {
        // immediate term
        request->offer = ctx->offer;
        ctx->copied_bytes += term_size(request->env, ctx->broker_term);
    }
    else {
        request->offer = enif_make_copy(request->env, ctx->offer);
        ctx->copied_bytes += (request->offer_size + term_size(request->env, ctx->broker_term));
    }

    //

    tag_t* tag = mempool_get(&local_state->tag_pool, NULL);

    bool mon_res = enif_monitor_process(ctx->env, tag, &ctx->self, &tag->mon);
    assert(mon_res == 0);

    tag->request = request;
    request->tag = tag;

    //

    ctx->request = request;
    ctx->tag_term = enif_make_resource(ctx->env, tag);
}

//

static void ask_reply_await(ask_ctx_t* ctx)
{
    assert(!ctx->is_non_blocking);
    assert(ctx->request == NULL);
    assert(ctx->tag_term != Atoms._none);
    assert(ctx->counter_request == NULL);
    assert(!ctx->consume_slot);

    ctx->term_res = make_await(ctx->env, ctx->tag_term);
}

static void ask_reply_match(ask_ctx_t* ctx)
{
    request_t* request = ctx->request;
    ERL_NIF_TERM tag_term = ctx->tag_term;
    assert((request == NULL) == (tag_term == Atoms._none));

    request_t* counter_request = ctx->counter_request;
    assert(counter_request != NULL);

    bool we_go_first = (ctx->is_async && !ctx->is_left);
    ERL_NIF_TERM match_ref = enif_make_ref(ctx->env);

    if (we_go_first) {
        ctx->term_res = ask_reply_match_self(ctx, match_ref);
        ask_reply_match_notify_other(ctx, match_ref);
    }
    else {
        ask_reply_match_notify_other(ctx, match_ref);
        ctx->term_res = ask_reply_match_self(ctx, match_ref);
    }
}

static void ask_reply_match_notify_other(ask_ctx_t* ctx, ERL_NIF_TERM match_ref)
{
    request_t* request = ctx->request;
    request_t* counter_request = ctx->counter_request;
    assert(counter_request != NULL);

    const int64_t sojourn_time = monotonic_ts() - counter_request->enqueue_ts;

    if (request != NULL) {
        // We reuse our own request's env, which already contains our offer
        ErlNifEnv* msg_env = request->env;
        ERL_NIF_TERM tag = enif_make_resource(msg_env, counter_request->tag);
        ERL_NIF_TERM ref = enif_make_copy(msg_env, match_ref);
        ERL_NIF_TERM match = make_match(msg_env, ref, request->offer, sojourn_time);
        ERL_NIF_TERM msg = enif_make_tuple2(msg_env, tag, match);
        either_notify_or_assert_not_alive(ctx->env, &counter_request->pid, msg_env, msg);
    }
    else {
        ERL_NIF_TERM tag = enif_make_resource(ctx->env, counter_request->tag);
        ERL_NIF_TERM match = make_match(ctx->env, match_ref, ctx->offer, sojourn_time);
        ERL_NIF_TERM msg = enif_make_tuple2(ctx->env, tag, match);
        either_notify_or_assert_not_alive(ctx->env, &counter_request->pid, NULL, msg);
    }
}

static ERL_NIF_TERM ask_reply_match_self(ask_ctx_t* ctx, ERL_NIF_TERM match_ref)
{
    request_t* counter_request = ctx->counter_request;
    assert(counter_request != NULL);

    const int64_t sojourn_time = monotonic_ts() - ctx->enqueue_ts;

    if (ctx->is_async) {
        ERL_NIF_TERM faux_tag_term = enif_make_ref(ctx->env);

        // We reuse the counter request's env, which already contains their offer
        ErlNifEnv* msg_env = counter_request->env;
        ERL_NIF_TERM msg_tag = enif_make_copy(msg_env, faux_tag_term);
        ERL_NIF_TERM msg_ref = enif_make_copy(msg_env, match_ref);
        ERL_NIF_TERM msg_match = make_match(msg_env, msg_ref, counter_request->offer, sojourn_time);
        ERL_NIF_TERM msg = enif_make_tuple2(msg_env, msg_tag, msg_match);
        either_notify_or_assert_not_alive(ctx->env, &ctx->self, msg_env, msg);

        return make_await(ctx->env, faux_tag_term);
    }
    else {
        ERL_NIF_TERM counter_offer;

        if (counter_request->offer_size == 0) {
            // immediate term
            counter_offer = counter_request->offer;
        }
        else {
            counter_offer = enif_make_copy(ctx->env, counter_request->offer);
            ctx->copied_bytes += counter_request->offer_size;
        }

        return make_match(ctx->env, match_ref, counter_offer, sojourn_time);
    }
}

static void ask_reply_nomatch(ask_ctx_t* ctx)
{
    assert(ctx->request == NULL);
    assert(ctx->tag_term == Atoms._none);
    assert(ctx->counter_request == NULL);

    int64_t sojourn_time = monotonic_ts() - ctx->enqueue_ts;
    ctx->term_res = make_drop(ctx->env, DROP_REASON_TOO_MANY_RETRIES, sojourn_time);
}

static void ask_reply_closed(ask_ctx_t* ctx)
{
    ctx->term_res = make_error(ctx->env, Atoms._broker_closed);
}

//

static bool ask_retry_can(ask_ctx_t* ctx)
{
    retry_t* retry = ctx->retry;
    int retry_nr = (retry == NULL ? 1 : retry->nr + 1);

    if (retry_nr > MAX_ASK_RETRIES) {
        return false;
    }
    return true;
}

static void ask_retry(ask_ctx_t* ctx, int argc, const ERL_NIF_TERM argv[])
{
    size_t retry_idx = ((size_t)argc) - 1;
    retry_t* retry = ctx->retry;

    request_t* request = ctx->request;
    ERL_NIF_TERM tag_term = ctx->tag_term;

    ctx->request = NULL;
    ctx->tag_term = Atoms._none;

    if (retry != NULL) {
        assert(!enif_is_atom(ctx->env, argv[retry_idx]));
        retry->nr++;

        if (retry->request == NULL) {
            assert(retry->tag_term == Atoms._none);
            retry->request = request;
            retry->tag_term = tag_term;
        }
        else {
            assert(retry->tag_term != Atoms._none);
            assert(request == NULL);
            assert(tag_term == Atoms._none);
        }

        ctx->term_res = ask_retry_schedule(ctx->env, argc, argv);
    }
    else {
        assert(enif_is_atom(ctx->env, argv[retry_idx]));
        retry = enif_alloc_resource(ResourceTypes.retry, sizeof(retry_t));
        memset(retry, 0, sizeof(retry_t));

        retry->nr = 1;
        retry->enqueue_ts = ctx->enqueue_ts;
        retry->ask_type = argv[retry_idx];
        retry->request = request;
        retry->tag_term = tag_term;

        ERL_NIF_TERM retry_term = enif_make_resource(ctx->env, retry);
        enif_release_resource(retry);

        ERL_NIF_TERM* retry_argv = enif_alloc(((size_t)argc) * sizeof(ERL_NIF_TERM));
        memcpy(retry_argv, argv, retry_idx * sizeof(ERL_NIF_TERM));
        retry_argv[retry_idx] = retry_term;

        ERL_NIF_TERM res = ask_retry_schedule(ctx->env, argc, retry_argv);
        enif_free(retry_argv);
        ctx->term_res = res;
    }
}

static ERL_NIF_TERM ask_retry_schedule(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return enif_schedule_nif(env, "nif_ask", 0, nif_ask, argc, argv);
}

static void ask_retry_clear(ask_ctx_t* ctx, const bool expect_no_request)
{
    retry_t* retry = ctx->retry;
    if (retry == NULL) {
        return;
    }

    request_t* request = retry->request;

    if (expect_no_request) {
        assert(request == NULL);
    }

    if (request != NULL) {
        assert(retry->tag_term != Atoms._none);

        bool demonitor_res =
            request_demonitor_and_reclaim(ctx->env, request, false, ctx->local_state);
        assert(demonitor_res);
        retry->request = NULL;
        retry->tag_term = Atoms._none;
    }
    else {
        assert(retry->tag_term == Atoms._none);
    }
}

static void ask_drop(ask_ctx_t* ctx, drop_reason_t reason)
{
    int64_t sojourn_time = monotonic_ts() - ctx->enqueue_ts;
    ctx->term_res = make_drop(ctx->env, reason, sojourn_time);
}

/*********************************************************************/

static bool request_demonitor(ErlNifEnv* caller_env, request_t* request)
{
    tag_t* tag = request->tag;
    assert(tag != NULL);

    if (enif_demonitor_process(caller_env, tag, &tag->mon) == 0) {
        return true;
    }
    else {
        enif_release_resource(tag);
        return false;
    }
}

static void request_reclaim(request_t* request, bool tag_used, local_state_t* opt_local_state)
{
    assert(request != NULL);

    tag_t* tag = request->tag;
    assert(tag != NULL);
    assert(tag->request == request);
    tag->request = NULL;
    request->tag = NULL;

    if (opt_local_state != NULL) {
        mempool_return(&opt_local_state->request_pool, request);

        if (tag_used) {
            enif_release_resource(tag);
        }
        else {
            mempool_return(&opt_local_state->tag_pool, tag);
        }
    }
    else {
        enif_free_env(request->env);
        enif_free(request);
        enif_release_resource(tag);
    }
}

static bool request_demonitor_and_reclaim(ErlNifEnv* caller_env, request_t* request, bool tag_used,
                                          local_state_t* local_state)
{
    if (request_demonitor(caller_env, request)) {
        request_reclaim(request, tag_used, local_state);
        return true;
    }
    return false;
}

/*********************************************************************/

static void tag_dtor(ErlNifEnv* caller_env, void* obj)
{
    tag_t* tag = (tag_t*)obj;
    request_t* request = tag->request;

    if (request != NULL) {
        ErlNifEnv* env = request->env;
        assert(env != NULL);

        enif_free_env(env);
        enif_free(request);
        tag->request = NULL;
    }

    memset(tag, 0, sizeof(tag_t));
}

//

static void tag_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
    ERL_NIF_TERM pid_term = enif_make_pid(caller_env, pid);

    tag_t* tag = (tag_t*)obj;

    request_t* request = tag->request;
    assert(request != NULL);
    assert(request->tag == tag);

    LOG_UNCOND("[request DOWN %T] batch %llu, offset %llu", pid_term, request->batch_id,
               request->offset);

    broker_t* broker = NULL;
    int res = get_broker(request->env, request->broker_term, &broker);
    assert(res);

    const batch_id_t batch_id = request->batch_id;
    const offset_t offset = request->offset;

    local_state_t* opt_local_state = broker_local_state(broker);
    lease_t lease;
    memset(&lease, 0, sizeof(lease_t));

    if (broker_checkout_batch(broker, opt_local_state, batch_id, &lease)) {
        LOG("[request DOWN %T] batch found", pid_term, request->batch_id);
        batch_t* batch = lease.batch;
        assert(offset < batch->nr_of_cells);

        cell_t* cell = &batch->cells[offset];

        if (atomic_compare_exchange_strong(cell, &request, &sentinel_request_cancelled)) {
            LOG("[request DOWN %T] request cancelled", pid_term, request->batch_id);
            request_reclaim(request, true, opt_local_state);
            lease_consume_slot(&lease);
        }
        else {
            LOG("[request DOWN %T] Too late to cancel request", pid_term);
            assert((request == &sentinel_request_cancelled) ||
                   (request == &sentinel_request_matched));
        }

        if (lease.batch != NULL && !lease.found_locally) {
            lease_ref_count_dec(&lease);
        }
    }
}

/*********************************************************************/

static void retry_dtor(ErlNifEnv* caller_env, void* obj)
{
    retry_t* retry = (retry_t*)obj;
    request_t* request = retry->request;

    if (request != NULL) {
        tag_t* tag = request->tag;
        assert(tag != NULL);

        // whether demonitoring succeeds doesn't matter in this case
        enif_demonitor_process(caller_env, tag, &tag->mon);
        enif_release_resource(tag);
        retry->request = NULL;
    }

    memset(retry, 0, sizeof(retry_t));
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
        enif_mutex_lock(global_state->lock);

        if (cbroker_omap_lookup(global_state->batches, batch_id, (void**)&batch)) {
            atomic_fetch_add_explicit(&batch->ref_count, 1, memory_order_relaxed);
            enif_mutex_unlock(global_state->lock);
            out_lease->batch = batch;
            out_lease->found_locally = false;
            out_lease->broker = broker;
            out_lease->opt_local_state = opt_local_state;
            return true;
        }
        else {
            enif_mutex_unlock(global_state->lock);
            return false;
        }
    }
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
        request_t* request = atomic_load(cell);

        if (request == &sentinel_request_cancelled || request == &sentinel_request_matched) {
            continue;
        }

        if (atomic_compare_exchange_strong(cell, &request, &sentinel_request_cancelled)) {
            if (request != NULL) {
                tag_t* tag = (tag_t*)request->tag;
                assert(tag != NULL);

                if (enif_demonitor_process(env, tag, &tag->mon) == 0) {
                    notify_of_cancellation(env, request, DROP_REASON_CLOSED);
                    tag->request = NULL;

                    enif_free_env(request->env);
                    enif_free(request);
                }
                enif_release_resource(tag);
            }
        }
        else {
            assert(request == &sentinel_request_matched);
        }
    }
}

//

static void broker_checkout_all_batches(broker_t* broker, local_state_t* opt_local_state,
                                        lease_t** out_array, size_t* out_nr_of_batches)
{
    global_state_t* global_state = &broker->global_state;
    enif_mutex_lock(global_state->lock);

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

    enif_mutex_unlock(global_state->lock);

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

    global_state_t* global_state = &broker->global_state;
    enif_mutex_destroy(global_state->lock);
    global_state->lock = NULL;

    mempool_destroy(&global_state->batch_pool);

    //

    for (size_t thread_id = 0; thread_id < broker->nr_of_schedulers; thread_id++) {
        local_state_t* local_state = &broker->local_states[thread_id];
        void* destroy_ctx = global_state;
        cbroker_omap_destroy(local_state->batches, broker_dtor_cb_local_batch, destroy_ctx);
        local_state->batches = NULL;

        mempool_destroy(&local_state->request_pool);
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
        request_t* request = atomic_load(cell);
        assert(request == NULL || request == &sentinel_request_cancelled ||
               request == &sentinel_request_matched);
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

    batch_id_t next_batch_id = 0;

    if (ref_count == 1) {
        global_state_t* global_state = &broker->global_state;
        enif_mutex_lock(global_state->lock);

        if (cbroker_omap_lookup(global_state->batches, batch_id, (void**)&batch)) {
            ref_count = atomic_load_explicit(&batch->ref_count, memory_order_acquire);
            assert(ref_count >= 1);

            if (ref_count == 1) {
                bool has_next = true;
                cbroker_omap_delete_and_next(global_state->batches, batch_id, &has_next,
                                             &next_batch_id, NULL);

                LOG("CONSUME: batch %u removed", batch_id);

                if (has_next) {
                    mempool_return(&global_state->batch_pool, batch);
                }
                else {
                    // We reuse the batch right away, ensuring batch IDs are not reused
                    // by a thread that lagged behind
                    next_batch_id = batch_id + 1;
                    batch_init(batch, next_batch_id);
                    map_res = cbroker_omap_insert(global_state->batches, next_batch_id, batch);
                    assert(map_res == CBROKER_OMAP_OK);
                    batch = NULL;
                }
            }
        }

        enif_mutex_unlock(global_state->lock);
    }

    lease->batch = NULL;
    lease->found_locally = false;
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

static void notify_of_cancellation(ErlNifEnv* env, request_t* request, const drop_reason_t reason)
{
    assert(request->tag != NULL);

    ERL_NIF_TERM tag = enif_make_resource(env, request->tag);
    int64_t sojourn_time = monotonic_ts() - request->enqueue_ts;
    ERL_NIF_TERM cancelled = make_drop(env, reason, sojourn_time);
    ERL_NIF_TERM msg = enif_make_tuple2(env, tag, cancelled);
    either_notify_or_assert_not_alive(env, &request->pid, NULL, msg);
}

//

static bool either_notify_or_assert_not_alive(ErlNifEnv* caller_env, ErlNifPid* pid,
                                              ErlNifEnv* msg_env, ERL_NIF_TERM msg)
{
    if (!enif_send(caller_env, pid, msg_env, msg)) {
        /* We assert that the recipient is no longer alive
         * to ensure we're not running from a dirty NIF.
         *
         * Otherwise, the recipient may never be notified
         * of a cancellation (or a match) after the caller
         * had been killed while running the NIF - which
         * would be Very Bad.
         */
        assert(!enif_is_process_alive(caller_env, pid));
        return false;
    }
    return true;
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

static bool batch_is_consumed(batch_t* batch)
{
    return (atomic_load_explicit(&batch->consumed_count, memory_order_relaxed) >=
            batch->nr_of_cells);
}

static ERL_NIF_TERM batch_to_term(ErlNifEnv* env, const batch_t* batch)
{
    const size_t nr_of_cells = batch->nr_of_cells;
    ERL_NIF_TERM* cell_terms = enif_alloc(nr_of_cells * sizeof(ERL_NIF_TERM));

    for (offset_t i = 0; i < nr_of_cells; i++) {
        const cell_t* cell = &batch->cells[i];
        const request_t* request = atomic_load(cell);

        if (request == NULL) {
            cell_terms[i] = Atoms._empty;
        }
        else if (request == &sentinel_request_cancelled) {
            cell_terms[i] = Atoms._cancelled;
        }
        else if (request == &sentinel_request_matched) {
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

static void request_pool_init(mempool_t* pool)
{
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = request_pool_cb_alloc;
    pool->clear_cb = request_pool_cb_clear;
    pool->free_cb = request_pool_cb_free;
    mempool_init(pool, MATCH_POOLS_INITIAL_COUNT, MATCH_POOLS_SIZE, NULL);
}

static void* request_pool_cb_alloc(void* ctx)
{
    assert(ctx == NULL);
    request_t* request = enif_alloc(sizeof(request_t));
    memset(request, 0, sizeof(request_t));
    request->env = enif_alloc_env();
    return request;
}

static void request_pool_cb_clear(void* obj)
{
    request_t* request = (request_t*)obj;
    ErlNifEnv* env = request->env;
    assert(env != NULL);

    memset(request, 0, sizeof(request_t));
    enif_clear_env(env);
    request->env = env;
}

static void request_pool_cb_free(void* obj)
{
    request_t* request = (request_t*)obj;
    ErlNifEnv* env = request->env;
    assert(env != NULL);

    enif_free_env(request->env);
    enif_free(obj);
}

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

static void stats_push_after_ask(stats_t* stats, int credits_left)
{
    rolling_avg_push(&stats->credits_left, credits_left);
}

static ERL_NIF_TERM stats_to_term(ErlNifEnv* env, stats_t* stats)
{
    return enif_make_list1(
        env,
        //
        enif_make_tuple2(env, Atoms._credits_left, rolling_avg_to_term(env, &stats->credits_left)));
}

static void rolling_avg_push(rolling_avg_t* rolling_avg, int64_t sample)
{
    size_t prev_count = atomic_fetch_add_explicit(&rolling_avg->count, 1, memory_order_relaxed);
    size_t idx = prev_count % ROLLING_AVG_SIZE;
    int64_t prev_sample = atomic_exchange(&rolling_avg->samples[idx], sample);
    atomic_fetch_add_explicit(&rolling_avg->sum, prev_sample, memory_order_relaxed);
}

static ERL_NIF_TERM rolling_avg_to_term(ErlNifEnv* env, rolling_avg_t* rolling_avg)
{
    size_t count = atomic_load_explicit(&rolling_avg->count, memory_order_relaxed);
    int64_t sum = atomic_load_explicit(&rolling_avg->sum, memory_order_relaxed);

    ERL_NIF_TERM avg_key = (count > ROLLING_AVG_SIZE + 20) ? Atoms._avg : Atoms._approx_avg;
    double avg = (double)sum / (double)count;

    return enif_make_list3(env,
                           //
                           enif_make_tuple2(env, Atoms._count, enif_make_uint64(env, count)),
                           //
                           enif_make_tuple2(env, Atoms._sum, enif_make_int64(env, sum)),
                           //
                           enif_make_tuple2(env, avg_key, enif_make_double(env, avg)));
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

static int get_retry(ErlNifEnv* env, ERL_NIF_TERM term, retry_t** out_retry)
{
    return enif_get_resource(env, term, ResourceTypes.retry, (void**)out_retry);
}

//

static int get_offer_size(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM offer,
                          ptrdiff_t* out_size)
{
#if USES_FLAT_SIZE
    size_t size_in_words = 0;

    if (get_size_t(env, term, &size_in_words)) {
        *out_size = (ptrdiff_t)(size_in_words * sizeof(ERL_NIF_TERM));
        return 1;
    }
#else
    if (term == Atoms._compute_from_nif) {
        //*out_size = enif_term_size(offer); // only compute it when needed
        *out_size = -1;
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
                               int64_t sojourn_time)
{
    // assert(sojourn_time >= 0);
    if (sojourn_time < 0) {
        LOG_UNCOND("NEGATIVE SOJOURN TIME: %lld", sojourn_time);
    }

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
