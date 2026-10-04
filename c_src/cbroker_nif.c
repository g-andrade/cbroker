/* Copyright (c) 2026 Guilherme Andrade
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

/* The algorithm is explained in INTERNALS.md. */

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

#define ASK_DEFAULT_CREDITS 400
#define ASK_DEFAULT_MAX_TRIES 10

#define BATCH_POOL_SIZE 4
#define BATCH_POOL_INITIAL_COUNT 1

#define REQUEST_POOL_DEFAULT_SIZE 8
#define REQUEST_POOL_DEFAULT_INITIAL_COUNT 0

#define TICKET_POOL_DEFAULT_SIZE 8
#define TICKET_POOL_DEFAULT_INITIAL_COUNT 0

#define SHENV_POOL_MAX_COUNT 16

// in bytes
#define SHARED_ENV_DEFAULT_BUDGET (16 * 1024)

//

/* The columns below are aligned on purpose. */
/* clang-format off */
#define ATOM_LIST \
    X(_ask_credits,           "ask_credits") \
    X(_ask_max_tries,         "ask_max_tries") \
    X(_async,                 "async") \
    X(_await,                 "await") \
    X(_badarg,                "badarg") \
    X(_badopt,                "badopt") \
    X(_badopts,               "badopts") \
    X(_batch_pool,            "batch_pool")  \
    X(_batches,               "batches") \
    X(_blocks,                "blocks") \
    X(_brokers,               "brokers") \
    X(_cancelled,             "cancelled") \
    X(_cells,                 "cells") \
    X(_cells_per_batch,       "cells_per_batch") \
    X(_closed,                "closed") \
    X(_compute_from_nif,      "compute_from_nif") \
    X(_consumed_count,        "consumed_count") \
    X(_creator,               "creator") \
    X(_depends_on_creator,    "depends_on_creator") \
    X(_drop,                  "drop") \
    X(_dynamic,               "dynamic") \
    X(_empty,                 "empty") \
    X(_envs,                  "envs") \
    X(_error,                 "error") \
    X(_false,                 "false") \
    X(_full_lane,             "full_lane") \
    X(_global_state,          "global_state") \
    X(_id,                    "id") \
    X(_initial_count,         "initial_count") \
    X(_left,                  "left")  \
    X(_left_tail,             "left_tail")  \
    X(_local_states,          "local_states")  \
    X(_match,                 "match") \
    X(_match_not_found,       "match_not_found") \
    X(_matched,               "matched") \
    X(_max_queue_len,         "max_queue_len") \
    X(_max_right_balance,     "max_right_balance") \
    X(_min_left_balance,      "min_left_balance") \
    X(_non_blocking,          "non_blocking") \
    X(_none,                  "none") \
    X(_ok,                    "ok") \
    X(_opts,                  "opts") \
    X(_queue_balance,         "queue_balance") \
    X(_ref_count,             "ref_count") \
    X(_request_pool,          "request_pool") \
    X(_retries,               "retries") \
    X(_right,                 "right") \
    X(_right_tail,            "right_tail") \
    X(_schedulers,            "schedulers") \
    X(_shared_env_budget,     "shared_env_budget") \
    X(_shutdowns,             "shutdowns") \
    X(_size,                  "size") \
    X(_stats,                 "stats") \
    X(_ticket,                "ticket") \
    X(_ticket_pool,           "ticket_pool") \
    X(_tickets,               "tickets") \
    X(_too_late,              "too_late") \
    X(_too_many_tries,        "too_many_tries") \
    X(_true,                  "true") \
    X(_unavailable,           "unavailable") \
    X(_unlimited,             "unlimited") \
    X(_waiting,               "waiting")
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

#if defined(_MSC_VER) && !defined(__clang__)
/* MSVC's ATOMIC_POINTER_LOCK_FREE isn't 2, although its atomics are lock-free
 * for "objects with sizes <= 8 and exactly equal to a power of two". That is
 * only documented for x86 and x64:
 * https://devblogs.microsoft.com/cppblog/c11-atomics-in-visual-studio-2022-version-17-5-preview-2/
 */
#if !defined(_M_IX86) && (!defined(_M_X64) || defined(_M_ARM64EC))
#error "cbroker hasn't confirmed lock-free atomics for MSVC on this architecture"
#endif
_Static_assert(sizeof(void*) <= 8, "cbroker needs lock-free pointer atomics");
#else
_Static_assert(ATOMIC_POINTER_LOCK_FREE == 2, "cbroker needs lock-free pointer atomics");
#endif

_Static_assert(sizeof(size_t) <= sizeof(void*) && sizeof(ptrdiff_t) <= sizeof(void*),
               "cbroker needs its atomic integers to be no wider than a pointer");

typedef ptrdiff_t thread_id_t;
typedef uint_fast64_t batch_id_t;
typedef size_t offset_t;
typedef ptrdiff_t ref_count_t;

//

typedef ptrdiff_t queue_balance_t;
// #define QUEUE_BALANCE_MIN PTRDIFF_MIN
#define QUEUE_BALANCE_MAX PTRDIFF_MAX

#define QUEUE_BALANCE_UNLIMITED_LEFT +1
#define QUEUE_BALANCE_UNLIMITED_RIGHT -1

//

typedef ptrdiff_t budget_counter_t;
#define BUDGET_COUNTER_MAX PTRDIFF_MAX

//

typedef struct {
    _Atomic(ref_count_t) request_count;
    _Atomic(bool) ditched_by_writer;
    _Atomic(bool) env_cleared;
    //
    ErlNifEnv* env;
    budget_counter_t bytes_left;
    //
    size_t copied_bytes;
    size_t copied_requests;
} shenv_t;

//

typedef struct {
    size_t size;
    size_t count;
    shenv_t** array;
} shenv_pool_t;

//

typedef struct {
    shenv_t* shenv;
    //
    ErlNifTime enqueue_ts;
    ErlNifPid pid;
    ERL_NIF_TERM offer;
    size_t offer_size;
    queue_balance_t weight;
    ERL_NIF_TERM reply_ref;
    //
    void* broker;
    batch_id_t batch_id;
    offset_t offset;
    void* ticket;
} request_t;

//

typedef struct {
    ErlNifMonitor mon;
    request_t* request;
} ticket_t;

//

typedef _Atomic(request_t*) cell_t;

//

typedef struct {
    batch_id_t id;
    _Atomic(ref_count_t) ref_count;
    _Atomic(offset_t) left_tail;
    _Atomic(offset_t) right_tail;
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
    cbroker_omap_t* batches;
    batch_id_t left_tail_id;
    batch_id_t right_tail_id;
    //
    mempool_t request_pool;
    mempool_t ticket_pool;
    shenv_pool_t shenv_pool;
} local_state_t;

//

typedef struct {
    size_t size;
    size_t initial_count;
} pool_opts_t;

//

typedef struct {
    int ask_credits;
    size_t ask_max_tries;
    pool_opts_t batch_pool;
    size_t cells_per_batch;
    bool depends_on_creator;
    queue_balance_t max_right_balance; // positive, or -1 to signal unlimited
    queue_balance_t min_left_balance;  // negative, or +1 to signal unlimited
    pool_opts_t request_pool;
    size_t shared_env_budget;
    pool_opts_t ticket_pool;
} broker_opts_t;

//

typedef struct {
    _Atomic(queue_balance_t) queue_balance;
} stats_t;

//

typedef struct {
    broker_opts_t opts;
    ErlNifPid creator_pid;
    ErlNifMonitor creator_mon;
    //
    global_state_t global_state;
    stats_t stats;
    //
    size_t schedulers;
    local_state_t local_states[];
} broker_t;

//

typedef struct {
    int nr;
    ErlNifTime enqueue_ts;
    ERL_NIF_TERM ask_type;
    request_t* request;
    ERL_NIF_TERM ticket_term;
} retry_t;

//

typedef enum {
    SHUTDOWN_PHASE_NONE = 0,
    SHUTDOWN_PHASE_REGULAR = 1,
    SHUTDOWN_PHASE_BACKUP = 2
} shutdown_phase_t;

//

typedef enum {
    SHUTDOWN_CONTINUE = 0,
    SHUTDOWN_PAUSE_NEXT_PHASE = 1,
    SHUTDOWN_PAUSE_YIELD = 2
} shutdown_pause_t;

//

typedef struct {
    ErlNifPid caller_pid;
    ErlNifMonitor caller_mon;
    broker_t* broker;
    //
    size_t batch_idx;
    offset_t cell_offset;
    //
    shutdown_phase_t phase;
    size_t yield_copied_bytes;
} shutdown_t;

/*********************************************************************/

typedef enum {
    DROP_REASON_NONE,
    DROP_REASON_CANCELLED,
    DROP_REASON_NON_BLOCKING,
    DROP_REASON_TOO_MANY_RETRIES,
    DROP_REASON_LANE_FULL,
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
    ASK_RESULT_NONE = 0,
    ASK_RESULT_SKIP_BATCH,
    ASK_RESULT_AWAIT,
    ASK_RESULT_MATCHED,
    ASK_RESULT_NO_MATCH_AVAILABLE,
    ASK_RESULT_FULL,
    ASK_RESULT_CLOSED,
    ASK_RESULT_OUT_OF_CREDITS
} ask_result_t;

//

typedef struct {
    ErlNifEnv* env;
    const ERL_NIF_TERM* argv;
    ErlNifTime enqueue_ts;
    ErlNifPid self;
    ERL_NIF_TERM self_term;
    //
    ERL_NIF_TERM lane;
    ERL_NIF_TERM offer;
    ptrdiff_t offer_size; // negative when it hasn't been computed yet
    ERL_NIF_TERM reply_ref;
    queue_balance_t request_weight;
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
    int credits;
    batch_t* batch;
    _Atomic(offset_t)* offset_counter;
    offset_t offset;
    //
    lease_t lease;
    request_t* request;
    ERL_NIF_TERM ticket_term;
    request_t* counter_request;
    bool consume_slot;
    bool demonitoring_failed;
    ERL_NIF_TERM nif_res;
} ask_ctx_t;

//

typedef struct {
    batch_t* batch;
    offset_t offset;
    bool consume_slot;
    // optional, reuse to notify counter-party if we allocated it but ended up in 2nd place
    request_t* our_request;
    ERL_NIF_TERM our_ticket;
    request_t* opposite_request;
    drop_reason_t drop_reason;
} ask_out_t;

/*********************************************************************/

static int on_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info);
static void init_atoms(ErlNifEnv* caller_env);
static void load_broker_resource(ErlNifEnv* caller_env);
static void load_ticket_resource(ErlNifEnv* caller_env);
static void load_retry_resource(ErlNifEnv* caller_env);
static void load_shutdown_pauseource(ErlNifEnv* caller_env);

//

static ERL_NIF_TERM nif_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_debug_info(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_alloc_perfcounters(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

//

static void broker_opts_init(broker_opts_t* opts, const size_t schedulers);
static size_t new_broker_size(const size_t schedulers);
static batch_t* global_state_init(global_state_t* global_state, const broker_opts_t* opts);
static bool global_state_is_closed_relaxed(global_state_t* global_state);
static ERL_NIF_TERM global_state_to_term(ErlNifEnv* env, global_state_t* global_state);

//

static void local_states_init(local_state_t local_states[], const size_t schedulers,
                              const broker_opts_t* opts, batch_t* first_batch);

static local_state_t* broker_local_state(broker_t* broker);

static batch_t* local_state_get_batch(local_state_t* local_state, const batch_id_t batch_id);

static ERL_NIF_TERM local_states_to_term(ErlNifEnv* env, local_state_t local_states[],
                                         const size_t schedulers);

static thread_id_t get_or_assign_thread_id(const size_t schedulers);

//

static ask_result_t ask_loop(ask_ctx_t* ctx);

static bool ask_loop_tail_get(ask_ctx_t* ctx);
static ask_result_t ask_loop_tail_ask(ask_ctx_t* ctx);
static ask_result_t ask_loop_tail_offset_ask(ask_ctx_t* ctx, lease_t* lease, const offset_t offset,
                                             _Atomic(offset_t)* offset_counter);
static bool ask_loop_tail_skip(ask_ctx_t* ctx, const batch_id_t batch_id);
static void ask_loop_tail_skip_consumed_opposite(ask_ctx_t* ctx, batch_id_t next_id);

static request_t* ask_loop_request_prepare(ask_ctx_t* ctx, const batch_id_t batch_id,
                                           const offset_t offset);
static void ask_loop_request_new(ask_ctx_t* ctx);
static size_t ask_loop_request_ensure_offer_size(ask_ctx_t* ctx);

static void ask_reply_await(ask_ctx_t* ctx);
static void ask_reply_match(ask_ctx_t* ctx);

static void ask_reply_match_notify_other(ask_ctx_t* ctx, ERL_NIF_TERM match_ref,
                                         ERL_NIF_TERM counter_tag);

static ERL_NIF_TERM ask_reply_match_self(ask_ctx_t* ctx, ERL_NIF_TERM match_ref,
                                         ERL_NIF_TERM counter_offer);

static void ask_reply_closed(ask_ctx_t* ctx);

static bool ask_retry_can(ask_ctx_t* ctx);
static void ask_retry(ask_ctx_t* ctx, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM ask_retry_schedule(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static void ask_retry_clear(ask_ctx_t* ctx);
static void ask_drop(ask_ctx_t* ctx, drop_reason_t reason);

//

static bool request_demonitor(ErlNifEnv* caller_env, request_t* request);

static void request_reclaim(request_t* request, bool ticket_used, local_state_t* opt_local_state,
                            const broker_opts_t* broker_opts, bool release_broker);

static bool request_demonitor_and_reclaim(ErlNifEnv* caller_env, request_t* request,
                                          bool ticket_used, local_state_t* local_state,
                                          const broker_opts_t* broker_opts, bool release_broker);

//

static void ticket_dtor(ErlNifEnv* caller_env, void* obj);
static void ticket_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon);

//

static void retry_dtor(ErlNifEnv* caller_env, void* obj);

//

static bool broker_checkout_batch(broker_t* broker, local_state_t* opt_local_state,
                                  batch_id_t batch_id, lease_t* out_lease);

static void broker_checkout_all_batches(broker_t* broker, local_state_t* opt_local_state,
                                        lease_t** out_array, size_t* out_nr_of_batches);

static void broker_checkin_many_batches(broker_t* broker, lease_t** array_ptr,
                                        const size_t nr_of_batches);

static void broker_dtor(ErlNifEnv* caller_env, void* obj);
static void broker_dtor_cb_local_batch(batch_id_t key, void* obj, void* ctx);
static void broker_dtor_cb_global_batch(batch_id_t key, void* obj, void* ctx);

static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon);

//

static ERL_NIF_TERM shutdown_start(ErlNifEnv* env, broker_t* broker, ErlNifPid self);
static ERL_NIF_TERM shutdown_continue(ErlNifEnv* env, shutdown_t* shutdown);

static void shutdown_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon);
static void shutdown_dtor(ErlNifEnv* caller_env, void* obj);

static shutdown_pause_t shutdown_batch(ErlNifEnv* env, shutdown_t* shutdown, batch_t* batch);
static shutdown_pause_t shutdown_cell(ErlNifEnv* env, shutdown_t* shutdown, cell_t* cell);
static shutdown_pause_t shutdown_cell_notify(ErlNifEnv* env, shutdown_t* shutdown,
                                             request_t* request);

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

static void batch_pool_init(mempool_t* pool, const pool_opts_t* opts, const size_t nr_of_cells);
static batch_t* batch_pool_get(mempool_t* pool, const size_t nr_of_cells);
static void* batch_pool_cb_alloc(void*);
static void batch_pool_cb_clear(void* obj);
static void batch_pool_cb_free(void* obj);

static void request_pool_init(mempool_t* pool, const pool_opts_t*);
static void* request_pool_cb_alloc(void*);
static void request_pool_cb_clear(void* obj);
static void request_pool_cb_free(void* obj);

static void ticket_pool_init(mempool_t* pool, const pool_opts_t*);
static void* ticket_pool_cb_alloc(void*);
static void ticket_pool_cb_clear(void* obj);
static void ticket_pool_cb_free(void* obj);

//

static void mempool_init(mempool_t* pool, const pool_opts_t*, void* alloc_ctx);
static void* mempool_get(mempool_t* pool, void* alloc_ctx);
static void mempool_return(mempool_t* pool, void* obj);
static void mempool_destroy(mempool_t* pool);
static ERL_NIF_TERM mempool_to_term(ErlNifEnv* env, mempool_t* pool);

//

static void shenv_pool_init(shenv_pool_t* pool);
static shenv_t* shenv_pool_get(shenv_pool_t* pool, size_t* out_idx);
static void shenv_pool_add(shenv_pool_t* pool, shenv_t* shenv);
static void shenv_pool_remove(shenv_pool_t* pool, const size_t idx);
static void shenv_pool_write(shenv_pool_t* shenv_pool, request_t* request, ask_ctx_t* ask_ctx);
static void shenv_pool_destroy(shenv_pool_t* pool);

//

static void shenv_write(shenv_t** shenv_ptr, request_t* request, ask_ctx_t* ask_ctx);

static void shenv_read(request_t* counter_request, ask_ctx_t* ask_ctx, ERL_NIF_TERM* out_tag,
                       ERL_NIF_TERM* out_counter_offer);

static void shenv_reclaim(shenv_t** shenv_ptr, local_state_t* opt_local_state,
                          const broker_opts_t* broker_opts);

static void shenv_destroy(shenv_t** shenv_ptr);

//

static queue_balance_t stats_queue_balance_add(stats_t* stats, queue_balance_t weight);
static void stats_queue_balance_sub(stats_t* stats, queue_balance_t weight);

static ERL_NIF_TERM stats_to_term(ErlNifEnv* env, stats_t* stats);

//

static int get_boolean(ERL_NIF_TERM term, bool* out);
static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out_broker);

static int get_broker_opt(ErlNifEnv* env, ERL_NIF_TERM key, ERL_NIF_TERM value,
                          broker_opts_t* out_opts);

static ERL_NIF_TERM get_broker_opts(ErlNifEnv* env, ERL_NIF_TERM term, broker_opts_t* out_opts);

static int get_offer_size(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM offer,
                          ptrdiff_t* out_size);

static int get_pool_opts(ErlNifEnv* env, ERL_NIF_TERM term, pool_opts_t* out_opts);

static int get_retry(ErlNifEnv* env, ERL_NIF_TERM term, retry_t** out_retry);

static int get_ptrdiff_t(ErlNifEnv* env, ERL_NIF_TERM term, ptrdiff_t* out);

static int get_shutdown(ErlNifEnv* env, ERL_NIF_TERM term, shutdown_t** out_shutdown);

static int get_size_t(ErlNifEnv* env, ERL_NIF_TERM term, size_t* out);

static int get_ticket(ErlNifEnv* env, ERL_NIF_TERM term, ticket_t** out_ticket);

//

static ERL_NIF_TERM make_await(ErlNifEnv* env, ERL_NIF_TERM ticket);
static ERL_NIF_TERM make_badarg(ErlNifEnv* env, ERL_NIF_TERM term);
static ERL_NIF_TERM make_badopts(ErlNifEnv* env, ERL_NIF_TERM term);
static ERL_NIF_TERM make_badopt(ErlNifEnv* env, ERL_NIF_TERM term);
static ERL_NIF_TERM make_boolean(int value);
static ERL_NIF_TERM make_broker_opts(ErlNifEnv* env, const broker_opts_t* opts);
static ERL_NIF_TERM make_cancelled(ErlNifEnv* env, const int64_t sojourn_time);
static ERL_NIF_TERM make_drop(ErlNifEnv* env, const drop_reason_t reason,
                              const int64_t sojourn_time);

static ERL_NIF_TERM make_drop_reason(ErlNifEnv* env, const drop_reason_t reason);
static ERL_NIF_TERM make_error(ErlNifEnv* env, ERL_NIF_TERM reason);

static ERL_NIF_TERM make_match(ErlNifEnv* env, ERL_NIF_TERM match_ref, ERL_NIF_TERM offer,
                               int64_t sojourn_time);

#ifdef CBROKER_COUNT_ALLOCS
static ERL_NIF_TERM make_perfcounter(ErlNifEnv* env, ERL_NIF_TERM key, _Atomic(int64_t)* counter);
#endif

static ERL_NIF_TERM make_pool_opts(ErlNifEnv* env, const pool_opts_t* opts);

static ERL_NIF_TERM make_reply(ErlNifEnv* env, ERL_NIF_TERM tag, ERL_NIF_TERM reply);

static ERL_NIF_TERM make_reply_drop(ErlNifEnv* env, request_t* request, const drop_reason_t reason);

static ERL_NIF_TERM raise_tuple2(ErlNifEnv* env, ERL_NIF_TERM reason_type,
                                 ERL_NIF_TERM reason_content);

//

static size_t term_size(ErlNifEnv* env, ERL_NIF_TERM term);
static inline int consume_timeslice(ErlNifEnv* env, const size_t copied_bytes);
static ErlNifTime monotonic_ts(void);

/*********************************************************************/

#define X(field, name) ERL_NIF_TERM field;
static struct {
    ATOM_LIST
} Atoms;
#undef X

//

static ErlNifFunc nif_funcs[] = {{"new", 1, nif_new, 0},
                                 {"ask", 6, nif_ask, 0},
                                 {"cancel", 1, nif_cancel, 0},
                                 {"close", 1, nif_close, 0},
                                 {"debug_info", 1, nif_debug_info, 0},
                                 {"alloc_perfcounters", 0, nif_alloc_perfcounters, 0}};

static struct {
    ErlNifResourceType* broker;
    ErlNifResourceType* ticket;
    ErlNifResourceType* retry;
    ErlNifResourceType* shutdown;
} ResourceTypes;

static _Atomic(thread_id_t) next_thread_id = 0;
static _Thread_local thread_id_t my_thread_id = -1;

// Sentinel values used in batch cells
static request_t sentinel_request_cancelled;
static request_t sentinel_request_matched;

//

/* Allocation counting, built only with -DCBROKER_COUNT_ALLOCS (see `make
 * test-sanitized`), where `alloc_perfcounters/0` reports what is alive so the tests
 * can assert nothing leaked; refcounted resources count allocations against
 * destructor calls. Without it there is nothing left of this: the wrappers below
 * are macros for the plain ERTS calls, and `alloc_perfcounters/0` says
 * `unavailable`. */
#ifdef CBROKER_COUNT_ALLOCS

static _Atomic(int64_t) nr_of_live_blocks = 0;
static _Atomic(int64_t) nr_of_live_envs = 0;
static _Atomic(int64_t) nr_of_live_brokers = 0;
static _Atomic(int64_t) nr_of_live_tickets = 0;
static _Atomic(int64_t) nr_of_live_retries = 0;
static _Atomic(int64_t) nr_of_live_shutdowns = 0;

static void* cbroker_alloc(size_t size)
{
    atomic_fetch_add_explicit(&nr_of_live_blocks, 1, memory_order_relaxed);
    return enif_alloc(size);
}

static void* cbroker_realloc(void* ptr, size_t size)
{
    if (ptr == NULL) {
        atomic_fetch_add_explicit(&nr_of_live_blocks, 1, memory_order_relaxed);
    }
    return enif_realloc(ptr, size);
}

static void cbroker_free(void* ptr)
{
    atomic_fetch_sub_explicit(&nr_of_live_blocks, 1, memory_order_relaxed);
    enif_free(ptr);
}

static ErlNifEnv* cbroker_alloc_env(void)
{
    atomic_fetch_add_explicit(&nr_of_live_envs, 1, memory_order_relaxed);
    return enif_alloc_env();
}

static void cbroker_free_env(ErlNifEnv* env)
{
    atomic_fetch_sub_explicit(&nr_of_live_envs, 1, memory_order_relaxed);
    enif_free_env(env);
}

static void* cbroker_alloc_resource(_Atomic(int64_t)* counter, ErlNifResourceType* type,
                                    size_t size)
{
    atomic_fetch_add_explicit(counter, 1, memory_order_relaxed);
    return enif_alloc_resource(type, size);
}

static void cbroker_count_dtor(_Atomic(int64_t)* counter)
{
    atomic_fetch_sub_explicit(counter, 1, memory_order_relaxed);
}

#else

#define cbroker_alloc(size) enif_alloc((size))
#define cbroker_realloc(ptr, size) enif_realloc((ptr), (size))
#define cbroker_free(ptr) enif_free((ptr))
#define cbroker_alloc_env() enif_alloc_env()
#define cbroker_free_env(env) enif_free_env((env))
#define cbroker_alloc_resource(counter, type, size) enif_alloc_resource((type), (size))
#define cbroker_count_dtor(counter) ((void)0)

#endif

/*********************************************************************/

static int on_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info)
{
    init_atoms(caller_env);

    memset(&ResourceTypes, 0, sizeof(ResourceTypes));
    load_broker_resource(caller_env);
    load_ticket_resource(caller_env);
    load_retry_resource(caller_env);
    load_shutdown_pauseource(caller_env);

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

static void load_ticket_resource(ErlNifEnv* caller_env)
{
    ErlNifResourceTypeInit callbacks = {ticket_dtor, NULL, ticket_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.ticket =
        enif_init_resource_type(caller_env, "cbroker.ticket", &callbacks, flags, &flags);
    assert(ResourceTypes.ticket != NULL);
}

static void load_retry_resource(ErlNifEnv* caller_env)
{
    ErlNifResourceTypeInit callbacks = {retry_dtor, NULL, NULL, 1, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.retry =
        enif_init_resource_type(caller_env, "cbroker.retry", &callbacks, flags, &flags);
    assert(ResourceTypes.retry != NULL);
}

static void load_shutdown_pauseource(ErlNifEnv* caller_env)
{
    ErlNifResourceTypeInit callbacks = {shutdown_dtor, NULL, shutdown_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.shutdown =
        enif_init_resource_type(caller_env, "cbroker.shutdown", &callbacks, flags, &flags);
    assert(ResourceTypes.shutdown != NULL);
}

ERL_NIF_INIT(cbroker_nif, nif_funcs, on_load, NULL, NULL, NULL);

/*********************************************************************/

static ERL_NIF_TERM nif_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifPid self;

    if (!enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    ErlNifSysInfo sys_info;
    enif_system_info(&sys_info, sizeof(sys_info));
    const size_t schedulers = (size_t)sys_info.scheduler_threads;
    assert(schedulers > 0);

    broker_opts_t opts;
    broker_opts_init(&opts, schedulers);

    if (argc > 0) {
        ERL_NIF_TERM opts_res = get_broker_opts(env, argv[0], &opts);
        if (opts_res != Atoms._ok) {
            return opts_res;
        }
    }

    const size_t broker_size = new_broker_size(schedulers);
    broker_t* broker =
        cbroker_alloc_resource(&nr_of_live_brokers, ResourceTypes.broker, broker_size);
    assert(broker != NULL);
    memset(broker, 0, broker_size);

    memcpy(&broker->opts, &opts, sizeof(broker_opts_t));
    broker->creator_pid = self;
    int mon_res = enif_monitor_process(env, broker, &broker->creator_pid, &broker->creator_mon);
    assert(mon_res == 0);

    batch_t* first_batch = global_state_init(&broker->global_state, &broker->opts);

    broker->schedulers = schedulers;
    local_states_init(broker->local_states, schedulers, &broker->opts, first_batch);

    ERL_NIF_TERM broker_term = enif_make_resource(env, broker);
    enif_release_resource(broker);
    return broker_term;
}

//

static ERL_NIF_TERM nif_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ask_ctx_t ctx;
    memset(&ctx, 0, sizeof(ask_ctx_t));
    ctx.ticket_term = Atoms._none;

    ctx.env = env;
    ctx.argv = argv;

    if (enif_self(env, &ctx.self)) {
        ctx.self_term = enif_make_pid(env, &ctx.self);
    }
    else {
        return enif_make_badarg(env);
    }

    assert(argc == 6);

    ERL_NIF_TERM broker_term = argv[0];
    ERL_NIF_TERM lane = argv[1];
    ctx.offer = argv[2];

    if (!get_offer_size(env, argv[3], ctx.offer, &ctx.offer_size)) {
        return make_badarg(env, argv[3]);
    }

    ERL_NIF_TERM reply_ref = argv[4];
    ERL_NIF_TERM ask_type_arg = argv[5];
    ERL_NIF_TERM ask_type = Atoms._none;

    //

    if (!get_broker(env, broker_term, &ctx.broker)) {
        return make_badarg(env, broker_term);
    }

    if (lane == Atoms._left) {
        ctx.is_left = true;
        ctx.request_weight = -1;
    }
    else if (lane == Atoms._right) {
        ctx.request_weight = +1;
    }
    else {
        return make_badarg(env, lane);
    }

    //

    if (!(reply_ref == Atoms._ticket || enif_is_ref(env, reply_ref))) {
        return make_badarg(env, reply_ref);
    }
    else {
        ctx.reply_ref = reply_ref;
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

    if (global_state_is_closed_relaxed(ctx.global_state)) {
        return make_error(env, Atoms._closed);
    }

    LOG("[ask] Getting local state");
    ctx.local_state = broker_local_state(ctx.broker);
    assert(ctx.local_state != NULL);

    if (ctx.is_left) {
        ctx.tail_id_ptr = &ctx.local_state->left_tail_id;
        ctx.opposite_tail_id_ptr = &ctx.local_state->right_tail_id;
    }
    else {
        ctx.tail_id_ptr = &ctx.local_state->right_tail_id;
        ctx.opposite_tail_id_ptr = &ctx.local_state->left_tail_id;
    }

    ctx.credits = ctx.broker->opts.ask_credits;
    ctx.nif_res = Atoms._none;

    ask_result_t ask_res = ask_loop(&ctx);

    //

    if (ctx.consume_slot) {
        lease_consume_slot(&ctx.lease);
    }

    if (ctx.counter_request != NULL) {
        if (ctx.demonitoring_failed) {
            // Can't reclaim the request, it's tied to the ticket
            enif_release_resource(ctx.counter_request->ticket);
        }
        else {
            request_reclaim(ctx.counter_request, true, ctx.local_state, &ctx.broker->opts, true);
        }
        ctx.counter_request = NULL;
    }

    //

    if (ask_res == ASK_RESULT_OUT_OF_CREDITS) {
        if (ask_retry_can(&ctx)) {
            ask_retry(&ctx, argc, argv);
        }
        else {
            ask_retry_clear(&ctx);
            ask_drop(&ctx, DROP_REASON_TOO_MANY_RETRIES);
        }
    }
    else {
        ask_retry_clear(&ctx);

        if (ask_res == ASK_RESULT_FULL) {
            const drop_reason_t drop_reason =
                (ctx.is_non_blocking ? DROP_REASON_NON_BLOCKING : DROP_REASON_LANE_FULL);
            ask_drop(&ctx, drop_reason);
        }
    }

    //

    if (ctx.request != NULL) {
        bool demonitor_res = request_demonitor_and_reclaim(env, ctx.request, false, ctx.local_state,
                                                           &ctx.broker->opts, true);
        assert(demonitor_res);
        ctx.request = NULL;
        ctx.ticket_term = Atoms._none;
    }

    consume_timeslice(env, ctx.copied_bytes);

    assert(ctx.nif_res != Atoms._none);
    return ctx.nif_res;
}

//

static ERL_NIF_TERM nif_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifPid self;
    ticket_t* ticket = NULL;

    if (!enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    ERL_NIF_TERM ticket_term = argv[0];

    LOG("[cancel] Resolving ticket");
    if (!get_ticket(env, ticket_term, &ticket)) {
        if (enif_is_ref(env, ticket_term)) {
            // assume this to be a faux ticket
            return Atoms._too_late;
        }
        return make_badarg(env, ticket_term);
    }

    LOG("[cancel] demonitoring process");
    if (enif_demonitor_process(env, ticket, &ticket->mon) != 0) {
        // too late
        return Atoms._too_late;
    }

    request_t* request = ticket->request;
    assert(request != NULL);
    assert(request->ticket == ticket);
    LOG("[cancel] Got request %p", request);
    LOG("[cancel] Request batch id: %llu", request->batch_id);
    LOG("[cancel] Request offset: %llu", request->offset);

    shenv_t* shenv = request->shenv;
    assert(shenv != NULL);

    broker_t* broker = (broker_t*)request->broker;
    assert(broker != NULL);

    local_state_t* local_state = broker_local_state(broker);
    assert(local_state != NULL);

    lease_t lease;
    memset(&lease, 0, sizeof(lease_t));
    bool was_cell_swapped = false;
    bool too_late = false;

    LOG("[cancel] Checking out batch");
    if (broker_checkout_batch(broker, local_state, request->batch_id, &lease)) {
        batch_t* batch = lease.batch;

        LOG("[cancel] asserting offset within bounds");
        assert(request->offset < batch->nr_of_cells);

        LOG("[cancel] Retrieving cell");
        cell_t* cell = &batch->cells[request->offset];
        request_t* cell_request = request;

        if (atomic_compare_exchange_strong(cell, &cell_request, &sentinel_request_cancelled)) {
            LOG("[cancel] Consuming lease slot");
            stats_queue_balance_sub(&broker->stats, request->weight);
            lease_consume_slot(&lease);
            was_cell_swapped = true;
        }
        else if (cell_request == &sentinel_request_matched) {
            too_late = true;
        }
        else {
            assert(cell_request == &sentinel_request_cancelled);
        }

        if (lease.batch != NULL && !lease.found_locally) {
            lease_ref_count_dec(&lease);
        }
    }
    else {
        too_late = true;
    }

    //

    ErlNifPid cancelled_pid = request->pid;
    ErlNifTime enqueue_ts = request->enqueue_ts;

    if (too_late) {
        return Atoms._too_late;
    }
    else if (was_cell_swapped) {
        if (enif_compare_pids(&cancelled_pid, &self)) {
            notify_of_cancellation(env, request, DROP_REASON_CANCELLED);
        }

        LOG("[cancel] Reclaiming request %p", request);
        request_reclaim(request, true, local_state, &broker->opts, true);
        request = NULL;
    }

    int64_t sojourn_time = monotonic_ts() - enqueue_ts;
    return make_cancelled(env, sojourn_time);
}

//

static ERL_NIF_TERM nif_close(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifPid self;

    if (!enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    broker_t* broker = NULL;
    shutdown_t* shutdown = NULL;

    ERL_NIF_TERM arg0 = argv[0];

    if (!(get_shutdown(env, arg0, &shutdown) || get_broker(env, arg0, &broker))) {
        return make_badarg(env, arg0);
    }
    else if (shutdown != NULL) {
        return shutdown_continue(env, shutdown);
    }
    else {
        assert(broker != NULL);
        return shutdown_start(env, broker, self);
    }
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

    ERL_NIF_TERM* batch_terms = cbroker_alloc(nr_of_batches * sizeof(ERL_NIF_TERM));

    for (size_t i = 0; i < nr_of_batches; i++) {
        lease_t* lease = &leases[i];
        batch_terms[i] = batch_to_term(env, lease->batch);
    }

    //

    broker_checkin_many_batches(broker, &leases, nr_of_batches);
    assert(leases == NULL);

    ERL_NIF_TERM batch_terms_list =
        enif_make_list_from_array(env, batch_terms, (unsigned)nr_of_batches);
    cbroker_free(batch_terms);

    ERL_NIF_TERM global_state_term = global_state_to_term(env, &broker->global_state);

    ERL_NIF_TERM local_state_terms_list =
        local_states_to_term(env, broker->local_states, broker->schedulers);

    ERL_NIF_TERM stats_term = stats_to_term(env, &broker->stats);

    return enif_make_list7(
        env,
        //
        enif_make_tuple2(env, Atoms._creator, enif_make_pid(env, &broker->creator_pid)),
        //
        enif_make_tuple2(env, Atoms._opts, make_broker_opts(env, &broker->opts)),
        //
        enif_make_tuple2(env, Atoms._global_state, global_state_term),
        //
        enif_make_tuple2(env, Atoms._schedulers, enif_make_uint64(env, broker->schedulers)),
        //
        enif_make_tuple2(env, Atoms._local_states, local_state_terms_list),
        //
        enif_make_tuple2(env, Atoms._stats, stats_term),
        //
        enif_make_tuple2(env, Atoms._batches, batch_terms_list));
}

//

static ERL_NIF_TERM nif_alloc_perfcounters(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
#ifdef CBROKER_COUNT_ALLOCS
    return enif_make_list6(env,
                           //
                           make_perfcounter(env, Atoms._blocks, &nr_of_live_blocks),
                           make_perfcounter(env, Atoms._envs, &nr_of_live_envs),
                           make_perfcounter(env, Atoms._brokers, &nr_of_live_brokers),
                           make_perfcounter(env, Atoms._tickets, &nr_of_live_tickets),
                           make_perfcounter(env, Atoms._retries, &nr_of_live_retries),
                           make_perfcounter(env, Atoms._shutdowns, &nr_of_live_shutdowns));
#else
    return Atoms._unavailable;
#endif
}

/*********************************************************************/

static void broker_opts_init(broker_opts_t* opts, const size_t schedulers)
{
    memset(opts, 0, sizeof(broker_opts_t));

    opts->cells_per_batch = 32 * schedulers;
    opts->min_left_balance = QUEUE_BALANCE_UNLIMITED_LEFT;
    opts->max_right_balance = QUEUE_BALANCE_UNLIMITED_RIGHT;
    opts->ask_credits = ASK_DEFAULT_CREDITS;
    opts->ask_max_tries = ASK_DEFAULT_MAX_TRIES;

    opts->batch_pool.size = BATCH_POOL_SIZE;
    opts->batch_pool.initial_count = BATCH_POOL_INITIAL_COUNT;

    opts->request_pool.size = REQUEST_POOL_DEFAULT_SIZE;
    opts->request_pool.initial_count = REQUEST_POOL_DEFAULT_INITIAL_COUNT;

    opts->ticket_pool.size = TICKET_POOL_DEFAULT_SIZE;
    opts->ticket_pool.initial_count = TICKET_POOL_DEFAULT_INITIAL_COUNT;

    opts->shared_env_budget = SHARED_ENV_DEFAULT_BUDGET;
}

static size_t new_broker_size(const size_t schedulers)
{
    return sizeof(broker_t) + (schedulers * sizeof(local_state_t));
}

static batch_t* global_state_init(global_state_t* global_state, const broker_opts_t* opts)
{
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;

    global_state->lock = enif_mutex_create("cbroker.global_state.lock");
    global_state->is_closed = false;
    global_state->batches = cbroker_omap_new();

    const batch_id_t first_batch_id = 1;
    batch_t* first_batch = batch_new(first_batch_id, opts->cells_per_batch);

    map_res = cbroker_omap_insert(global_state->batches, first_batch->id, first_batch);
    assert(map_res == CBROKER_OMAP_OK);

    batch_pool_init(&global_state->batch_pool, &opts->batch_pool, opts->cells_per_batch);

    return first_batch;
}

static bool global_state_is_closed_relaxed(global_state_t* global_state)
{
    return atomic_load_explicit(&global_state->is_closed, memory_order_relaxed);
}

static ERL_NIF_TERM global_state_to_term(ErlNifEnv* env, global_state_t* global_state)
{
    return enif_make_list1(
        env,
        //
        enif_make_tuple2(env, Atoms._batch_pool, mempool_to_term(env, &global_state->batch_pool)));
}
/*********************************************************************/

static void local_states_init(local_state_t local_states[], const size_t schedulers,
                              const broker_opts_t* opts, batch_t* first_batch)
{
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;
    assert(first_batch != NULL);

    for (size_t thread_id = 0; thread_id < schedulers; thread_id++) {
        local_state_t* local_state = &local_states[thread_id];
        local_state->batches = cbroker_omap_new();

        map_res = cbroker_omap_insert(local_state->batches, first_batch->id, first_batch);
        assert(map_res == CBROKER_OMAP_OK);
        batch_ref_count_inc(first_batch);

        LOG("[local_states_init] First batch is %llu", first_batch->id);
        local_state->left_tail_id = first_batch->id;
        local_state->right_tail_id = first_batch->id;

        request_pool_init(&local_state->request_pool, &opts->request_pool);
        ticket_pool_init(&local_state->ticket_pool, &opts->ticket_pool);
        shenv_pool_init(&local_state->shenv_pool);
    }
}

static local_state_t* broker_local_state(broker_t* broker)
{
    const thread_id_t thread_id = get_or_assign_thread_id(broker->schedulers);

    if (thread_id < 0) {
        return NULL;
    }

    assert((size_t)thread_id < broker->schedulers);
    local_state_t* local_state = &broker->local_states[thread_id];
    return local_state;
}

static batch_t* local_state_get_batch(local_state_t* local_state, const batch_id_t batch_id)
{
    batch_t* batch = NULL;
    LOG("Looking up batch %llu", batch_id);
    cbroker_omap_lookup(local_state->batches, batch_id, (void**)&batch);
    return batch;
}

static ERL_NIF_TERM local_states_to_term(ErlNifEnv* env, local_state_t local_states[],
                                         const size_t schedulers)
{
    ERL_NIF_TERM* local_state_terms = cbroker_alloc(schedulers * sizeof(ERL_NIF_TERM));

    for (size_t thread_id = 0; thread_id < schedulers; thread_id++) {
        local_state_t* local_state = &local_states[thread_id];

        ERL_NIF_TERM local_state_term =
            enif_make_list2(env,
                            //
                            enif_make_tuple2(env, Atoms._request_pool,
                                             mempool_to_term(env, &local_state->request_pool)),
                            //
                            enif_make_tuple2(env, Atoms._ticket_pool,
                                             mempool_to_term(env, &local_state->ticket_pool)));

        local_state_terms[thread_id] = local_state_term;
    }

    ERL_NIF_TERM list = enif_make_list_from_array(env, local_state_terms, (unsigned)schedulers);
    cbroker_free(local_state_terms);
    return list;
}

static thread_id_t get_or_assign_thread_id(const size_t schedulers)
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
    request_t* counter_request = NULL;

    //

    const broker_opts_t* broker_opts = &broker->opts;
    stats_t* stats = &ctx->broker->stats;
    queue_balance_t queue_balance = stats_queue_balance_add(stats, ctx->request_weight);

    if ((broker_opts->min_left_balance <= 0 && queue_balance < broker_opts->min_left_balance) ||
        (broker_opts->max_right_balance >= 0 && queue_balance > broker_opts->max_right_balance)) {
        stats_queue_balance_sub(stats, ctx->request_weight);
        return ASK_RESULT_FULL;
    }

    //

    lease_t* lease = &ctx->lease;
    lease_init(lease, NULL, false, broker, local_state);

    while (ctx->credits > 0) {
        if (lease->batch == NULL) {
            if (ask_loop_tail_get(ctx)) {
                assert(lease->batch != NULL);
            }
            else {
                ask_res = ASK_RESULT_CLOSED;
                goto ask_loop_closed;
            }
        }
        assert(lease->found_locally);

        //

        ask_res = ask_loop_tail_ask(ctx);

        switch (ask_res) {
        case ASK_RESULT_SKIP_BATCH:
            if (ask_loop_tail_skip(ctx, ctx->lease.batch->id)) {
                continue;
            }
            else {
                ask_res = ASK_RESULT_CLOSED;
                goto ask_loop_closed;
            }
        //
        case ASK_RESULT_AWAIT:
            ask_reply_await(ctx);
            break;
        //
        case ASK_RESULT_MATCHED:
            counter_request = ctx->counter_request;
            assert(counter_request != NULL);
            stats_queue_balance_sub(stats, ctx->request_weight + counter_request->weight);
            ask_reply_match(ctx);
            break;
        //
        case ASK_RESULT_NO_MATCH_AVAILABLE:
            stats_queue_balance_sub(stats, ctx->request_weight);
            assert(ctx->is_non_blocking);
            assert(!ctx->is_async);
            ask_drop(ctx, DROP_REASON_NON_BLOCKING);
            break;
        //
        case ASK_RESULT_CLOSED:
        ask_loop_closed:
            stats_queue_balance_sub(stats, ctx->request_weight);
            ask_reply_closed(ctx);
            break;
        //
        default:
            stats_queue_balance_sub(stats, ctx->request_weight);
            break;
        }
        return ask_res;
    }

    stats_queue_balance_sub(stats, ctx->request_weight);
    return ASK_RESULT_OUT_OF_CREDITS;
}

static bool ask_loop_tail_get(ask_ctx_t* ctx)
{
    lease_t* lease = &ctx->lease;
    local_state_t* local_state = ctx->local_state;

    batch_id_t tail_id = *(ctx->tail_id_ptr);
    batch_t* batch = NULL;

    if (cbroker_omap_lookup(local_state->batches, tail_id, (void**)&batch)) {
        assert(batch != NULL);
        lease->batch = batch;
        lease->found_locally = true;
        return true;
    }
    else {
        return ask_loop_tail_skip(ctx, tail_id);
    }
}

static ask_result_t ask_loop_tail_ask(ask_ctx_t* ctx)
{
    lease_t* lease = &ctx->lease;
    batch_t* batch = lease->batch;
    assert(batch != NULL);

    _Atomic(offset_t)* offset_counter = (ctx->is_left ? &batch->left_tail : &batch->right_tail);

    while (ctx->credits-- > 0) {
        const offset_t offset = atomic_fetch_add_explicit(offset_counter, 1, memory_order_relaxed);

        if (offset >= batch->nr_of_cells) {
            return ASK_RESULT_SKIP_BATCH;
        }

        ask_result_t ask_res = ask_loop_tail_offset_ask(ctx, lease, offset, offset_counter);

        if (ask_res) {
            return ask_res;
        }
        else if (global_state_is_closed_relaxed(ctx->global_state)) {
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

        // Matched!
        ctx->demonitoring_failed = !request_demonitor(ctx->env, counter_request);
        assert(counter_request->offer_size >= 0);
        ctx->counter_request = counter_request;
        ctx->consume_slot = true;
        return ASK_RESULT_MATCHED;
    }

    assert(counter_request == &sentinel_request_cancelled);
    return ASK_RESULT_NONE;
}

static bool ask_loop_tail_skip(ask_ctx_t* ctx, const batch_id_t batch_id)
{
    cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;
    lease_t* lease = &ctx->lease;
    broker_t* broker = ctx->broker;
    local_state_t* local_state = ctx->local_state;

    /* No batch in the lease means this local state already dropped the tail's
     * batch, upon consuming its last cell, with the tail still pointing at it. */
    assert(lease->batch == NULL || lease->batch->id == batch_id);

    batch_t* next_batch = NULL;
    global_state_t* global_state = ctx->global_state;

    if (!cbroker_omap_next(local_state->batches, batch_id, NULL, (void**)&next_batch)) {
        enif_mutex_lock(global_state->lock);

        /* Checked under the lock that closing takes its snapshot of batches
         * with: either we got here first and the batch we may be about to add
         * is in that snapshot, or we see the flag. */
        if (atomic_load(&global_state->is_closed)) {
            enif_mutex_unlock(global_state->lock);
            return false;
        }

        batch_t** all_next = NULL;
        batch_t* one_of_next = NULL;

        size_t all_next_count =
            cbroker_omap_all_next(global_state->batches, batch_id, NULL, (void***)&all_next);

        size_t all_next_size = all_next_count * sizeof(batch_t*);
        batch_t** batches_to_checkout = cbroker_alloc(all_next_size);
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

            next_batch = batch_pool_get(&global_state->batch_pool, broker->opts.cells_per_batch);
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

            next_batch = batches_to_checkout[0];
        }

        cbroker_free(batches_to_checkout);

        assert(next_batch != NULL);
        assert(next_batch->id > batch_id);
    }

    //

    *(ctx->tail_id_ptr) = next_batch->id;

    if (*(ctx->opposite_tail_id_ptr) > batch_id) {
        if (lease->batch != NULL) {
            lease_ref_count_dec(lease);
        }
    }
    else {
        ask_loop_tail_skip_consumed_opposite(ctx, next_batch->id);
    }

    lease->batch = next_batch;
    lease->found_locally = true;
    return true;
}

static void ask_loop_tail_skip_consumed_opposite(ask_ctx_t* ctx, batch_id_t next_id)
{
    broker_t* broker = ctx->broker;
    local_state_t* local_state = ctx->local_state;
    batch_id_t opposite_tail_id = 0;
    batch_t* batch = NULL;
    lease_t lease;

    while ((opposite_tail_id = *(ctx->opposite_tail_id_ptr)) < next_id) {
        if (cbroker_omap_lookup(local_state->batches, opposite_tail_id, (void**)&batch)) {
            assert(batch != NULL);

            if (batch_is_consumed(batch)) {
                lease_init(&lease, batch, true, broker, local_state);
                bool res =
                    cbroker_omap_next(local_state->batches, opposite_tail_id, NULL, (void**)&batch);
                assert(res);
                assert(batch != NULL);
                lease_ref_count_dec(&lease);
                *(ctx->opposite_tail_id_ptr) = batch->id;
            }
            else {
                break;
            }
        }
        else {
            bool res =
                cbroker_omap_next(local_state->batches, opposite_tail_id, NULL, (void**)&batch);
            assert(res);
            assert(batch != NULL);
            *(ctx->opposite_tail_id_ptr) = batch->id;
        }
    }

    assert(*(ctx->opposite_tail_id_ptr) <= next_id);
}

//

static request_t* ask_loop_request_prepare(ask_ctx_t* ctx, const batch_id_t batch_id,
                                           const offset_t offset)
{
    request_t* request = ctx->request;
    ERL_NIF_TERM ticket_term = ctx->ticket_term;

    if (request == NULL) {
        retry_t* retry = ctx->retry;

        if (retry != NULL) {
            request = retry->request;
            ticket_term = retry->ticket_term;
            retry->request = NULL;
            retry->ticket_term = Atoms._none;
        }

        if (request == NULL) {
            ask_loop_request_new(ctx);
        }
        else {
            ctx->ticket_term = ticket_term;
        }

        request = ctx->request;
        ticket_term = ctx->ticket_term;
    }

    assert(request != NULL);
    assert(ticket_term != Atoms._none);

    request->batch_id = batch_id;
    request->offset = offset;
    return request;
}

static void ask_loop_request_new(ask_ctx_t* ctx)
{
    assert(ctx->request == NULL);
    assert(ctx->ticket_term == Atoms._none);

    local_state_t* local_state = ctx->local_state;
    request_t* request = mempool_get(&local_state->request_pool, NULL);

    request->enqueue_ts = ctx->enqueue_ts;
    request->pid = ctx->self;
    request->offer_size = ask_loop_request_ensure_offer_size(ctx);

    shenv_pool_write(&ctx->local_state->shenv_pool, request, ctx);

    request->broker = (void*)ctx->broker;
    enif_keep_resource(request->broker);

    request->weight = ctx->request_weight;

    ticket_t* ticket = mempool_get(&local_state->ticket_pool, NULL);

    bool mon_res = enif_monitor_process(ctx->env, ticket, &ctx->self, &ticket->mon);
    assert(mon_res == 0);

    ticket->request = request;
    request->ticket = ticket;

    //

    ctx->request = request;
    ctx->ticket_term = enif_make_resource(ctx->env, ticket);
}

static size_t ask_loop_request_ensure_offer_size(ask_ctx_t* ctx)
{
    if (ctx->offer_size < 0) {
        assert(!USES_FLAT_SIZE);
        size_t offer_size = term_size(ctx->env, ctx->offer);
        ctx->offer_size = (ptrdiff_t)offer_size;
        return offer_size;
    }
    return (size_t)ctx->offer_size;
}

//

static void ask_reply_await(ask_ctx_t* ctx)
{
    assert(!ctx->is_non_blocking);
    assert(ctx->request == NULL);
    assert(ctx->ticket_term != Atoms._none);
    assert(ctx->counter_request == NULL);
    assert(!ctx->consume_slot);

    ctx->nif_res = make_await(ctx->env, ctx->ticket_term);
}

static void ask_reply_match(ask_ctx_t* ctx)
{
    request_t* request = ctx->request;
    ERL_NIF_TERM ticket_term = ctx->ticket_term;
    assert((request == NULL) == (ticket_term == Atoms._none));

    request_t* counter_request = ctx->counter_request;
    assert(counter_request != NULL);

    bool we_go_first = (ctx->is_async && !ctx->is_left);
    ERL_NIF_TERM match_ref = enif_make_ref(ctx->env);

    ERL_NIF_TERM counter_tag, counter_offer;
    shenv_read(counter_request, ctx, &counter_tag, &counter_offer);

    if (we_go_first) {
        ctx->nif_res = ask_reply_match_self(ctx, match_ref, counter_offer);
        ask_reply_match_notify_other(ctx, match_ref, counter_tag);
    }
    else {
        ask_reply_match_notify_other(ctx, match_ref, counter_tag);
        ctx->nif_res = ask_reply_match_self(ctx, match_ref, counter_offer);
    }
}

static void ask_reply_match_notify_other(ask_ctx_t* ctx, ERL_NIF_TERM match_ref,
                                         ERL_NIF_TERM counter_tag)
{
    request_t* counter_request = ctx->counter_request;
    assert(counter_request != NULL);

    const int64_t sojourn_time = monotonic_ts() - counter_request->enqueue_ts;

    ERL_NIF_TERM match = make_match(ctx->env, match_ref, ctx->offer, sojourn_time);
    ERL_NIF_TERM msg = make_reply(ctx->env, counter_tag, match);
    either_notify_or_assert_not_alive(ctx->env, &counter_request->pid, NULL, msg);
}

static ERL_NIF_TERM ask_reply_match_self(ask_ctx_t* ctx, ERL_NIF_TERM match_ref,
                                         ERL_NIF_TERM counter_offer)
{
    const int64_t sojourn_time = monotonic_ts() - ctx->enqueue_ts;

    if (ctx->is_async) {
        ERL_NIF_TERM msg_tag;

        if (ctx->reply_ref == Atoms._ticket) {
            msg_tag = enif_make_ref(ctx->env);
        }
        else {
            // Reuse custom reply
            msg_tag = ctx->reply_ref;
        }

        ERL_NIF_TERM msg_match = make_match(ctx->env, match_ref, counter_offer, sojourn_time);
        ERL_NIF_TERM msg = make_reply(ctx->env, msg_tag, msg_match);
        either_notify_or_assert_not_alive(ctx->env, &ctx->self, NULL, msg);

        return make_await(ctx->env, msg_tag);
    }
    else {
        return make_match(ctx->env, match_ref, counter_offer, sojourn_time);
    }

    //}
}

static void ask_reply_closed(ask_ctx_t* ctx) { ctx->nif_res = make_error(ctx->env, Atoms._closed); }

//

static bool ask_retry_can(ask_ctx_t* ctx)
{
    retry_t* retry = ctx->retry;
    int retry_nr = (retry == NULL ? 1 : retry->nr + 1);

    if (retry_nr >= (int)ctx->broker->opts.ask_max_tries) {
        return false;
    }
    return true;
}

static void ask_retry(ask_ctx_t* ctx, int argc, const ERL_NIF_TERM argv[])
{
    size_t retry_idx = ((size_t)argc) - 1;
    retry_t* retry = ctx->retry;

    request_t* request = ctx->request;
    ERL_NIF_TERM ticket_term = ctx->ticket_term;

    ctx->request = NULL;
    ctx->ticket_term = Atoms._none;

    if (retry != NULL) {
        assert(!enif_is_atom(ctx->env, argv[retry_idx]));
        retry->nr++;

        if (retry->request == NULL) {
            assert(retry->ticket_term == Atoms._none);
            retry->request = request;
            retry->ticket_term = ticket_term;
        }
        else {
            assert(retry->ticket_term != Atoms._none);
            assert(request == NULL);
            assert(ticket_term == Atoms._none);
        }

        ctx->nif_res = ask_retry_schedule(ctx->env, argc, argv);
    }
    else {
        assert(enif_is_atom(ctx->env, argv[retry_idx]));
        retry = cbroker_alloc_resource(&nr_of_live_retries, ResourceTypes.retry, sizeof(retry_t));
        memset(retry, 0, sizeof(retry_t));

        retry->nr = 1;
        retry->enqueue_ts = ctx->enqueue_ts;
        retry->ask_type = argv[retry_idx];
        retry->request = request;
        retry->ticket_term = ticket_term;

        ERL_NIF_TERM retry_term = enif_make_resource(ctx->env, retry);
        enif_release_resource(retry);

        ERL_NIF_TERM* retry_argv = cbroker_alloc(((size_t)argc) * sizeof(ERL_NIF_TERM));
        memcpy(retry_argv, argv, retry_idx * sizeof(ERL_NIF_TERM));
        retry_argv[retry_idx] = retry_term;

        ERL_NIF_TERM res = ask_retry_schedule(ctx->env, argc, retry_argv);
        cbroker_free(retry_argv);
        ctx->nif_res = res;
    }
}

static ERL_NIF_TERM ask_retry_schedule(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    return enif_schedule_nif(env, "nif_ask", 0, nif_ask, argc, argv);
}

static void ask_retry_clear(ask_ctx_t* ctx)
{
    retry_t* retry = ctx->retry;
    if (retry == NULL) {
        return;
    }

    request_t* request = retry->request;

    if (request != NULL) {
        assert(retry->ticket_term != Atoms._none);

        bool demonitor_res = request_demonitor_and_reclaim(
            ctx->env, request, false, ctx->local_state, &ctx->broker->opts, true);
        assert(demonitor_res);
        retry->request = NULL;
        retry->ticket_term = Atoms._none;
    }
    else {
        assert(retry->ticket_term == Atoms._none);
    }
}

static void ask_drop(ask_ctx_t* ctx, drop_reason_t reason)
{
    int64_t sojourn_time = monotonic_ts() - ctx->enqueue_ts;
    ERL_NIF_TERM drop = ctx->nif_res = make_drop(ctx->env, reason, sojourn_time);

    if (ctx->is_async) {
        ERL_NIF_TERM faux_ticket =
            (ctx->reply_ref == Atoms._ticket ? enif_make_ref(ctx->env) : ctx->reply_ref);

        ERL_NIF_TERM msg = make_reply(ctx->env, faux_ticket, drop);
        either_notify_or_assert_not_alive(ctx->env, &ctx->self, NULL, msg);

        ctx->nif_res = make_await(ctx->env, faux_ticket);
    }
    else {
        ctx->nif_res = drop;
    }
}

/*********************************************************************/

static bool request_demonitor(ErlNifEnv* caller_env, request_t* request)
{
    ticket_t* ticket = request->ticket;
    assert(ticket != NULL);

    if (enif_demonitor_process(caller_env, ticket, &ticket->mon) == 0) {
        return true;
    }
    else {
        return false;
    }
}

static void request_reclaim(request_t* request, bool ticket_used, local_state_t* opt_local_state,
                            const broker_opts_t* broker_opts, bool release_broker)
{
    assert(request != NULL);

    ticket_t* ticket = request->ticket;
    assert(ticket != NULL);
    assert(ticket->request == request);
    ticket->request = NULL;
    request->ticket = NULL;

    broker_t* broker = (broker_t*)request->broker;
    assert(broker != NULL);
    request->broker = NULL;

    shenv_reclaim(&request->shenv, opt_local_state, broker_opts);
    assert(request->shenv == NULL);

    if (opt_local_state != NULL) {
        mempool_return(&opt_local_state->request_pool, request);

        if (ticket_used) {
            enif_release_resource(ticket);
        }
        else {
            mempool_return(&opt_local_state->ticket_pool, ticket);
        }
    }
    else {
        cbroker_free(request);
        enif_release_resource(ticket);
    }

    //

    if (release_broker) {
        enif_release_resource(broker);
    }
}

static bool request_demonitor_and_reclaim(ErlNifEnv* caller_env, request_t* request,
                                          bool ticket_used, local_state_t* local_state,
                                          const broker_opts_t* broker_opts, bool release_broker)
{
    if (request_demonitor(caller_env, request)) {
        request_reclaim(request, ticket_used, local_state, broker_opts, release_broker);
        return true;
    }
    return false;
}

/*********************************************************************/

static void ticket_dtor(ErlNifEnv* caller_env, void* obj)
{
    cbroker_count_dtor(&nr_of_live_tickets);
    ticket_t* ticket = (ticket_t*)obj;
    request_t* request = ticket->request;

    if (request != NULL) {
        broker_t* broker = (broker_t*)request->broker;
        if (broker != NULL) {
            local_state_t* opt_local_state = broker_local_state(broker);
            shenv_reclaim(&request->shenv, opt_local_state, &broker->opts);
            assert(request->shenv == NULL);

            enif_release_resource(broker);
            request->broker = NULL;
        }
        else {
            assert(request->shenv == NULL);
        }

        cbroker_free(request);
        ticket->request = NULL;
    }

    memset(ticket, 0, sizeof(ticket_t));
}

//

static void ticket_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
    // ERL_NIF_TERM pid_term = enif_make_pid(caller_env, pid);
    ticket_t* ticket = (ticket_t*)obj;

    request_t* request = ticket->request;
    assert(request != NULL);
    assert(request->ticket == ticket);

    LOG("[request DOWN %T] batch %llu, offset %llu", pid_term, request->batch_id, request->offset);

    shenv_t* shenv = request->shenv;
    assert(shenv != NULL);

    broker_t* broker = (broker_t*)request->broker;
    assert(broker != NULL);

    const batch_id_t batch_id = request->batch_id;
    const offset_t offset = request->offset;

    local_state_t* opt_local_state = broker_local_state(broker);
    lease_t lease;
    memset(&lease, 0, sizeof(lease_t));

    bool was_reclaimed = false;

    if (broker_checkout_batch(broker, opt_local_state, batch_id, &lease)) {
        LOG("[request DOWN %T] batch found", pid_term, request->batch_id);
        batch_t* batch = lease.batch;
        assert(offset < batch->nr_of_cells);

        cell_t* cell = &batch->cells[offset];

        if (atomic_compare_exchange_strong(cell, &request, &sentinel_request_cancelled)) {
            LOG("[request DOWN %T] request cancelled", pid_term, request->batch_id);
            stats_queue_balance_sub(&broker->stats, request->weight);
            lease_consume_slot(&lease);
            request_reclaim(request, true, opt_local_state, &broker->opts, false);
            was_reclaimed = true;
        }
        else {
            LOG("[request DOWN %T] Too late to cancel request", pid_term);
            assert((request == &sentinel_request_cancelled) ||
                   (request == &sentinel_request_matched));
        }

        if (lease.batch != NULL && !lease.found_locally) {
            lease_ref_count_dec(&lease);
        }

        if (was_reclaimed) {
            enif_release_resource(broker);
        }
    }
}

/*********************************************************************/

static void retry_dtor(ErlNifEnv* caller_env, void* obj)
{
    cbroker_count_dtor(&nr_of_live_retries);
    retry_t* retry = (retry_t*)obj;
    request_t* request = retry->request;

    if (request != NULL) {
        ticket_t* ticket = request->ticket;
        assert(ticket != NULL);

        // whether demonitoring succeeds doesn't matter in this case
        enif_demonitor_process(caller_env, ticket, &ticket->mon);
        enif_release_resource(ticket);
        retry->request = NULL;
    }

    memset(retry, 0, sizeof(retry_t));
}

/*********************************************************************/

static bool broker_checkout_batch(broker_t* broker, local_state_t* opt_local_state,
                                  batch_id_t batch_id, lease_t* out_lease)
{
    batch_t* batch = NULL;

    if (opt_local_state != NULL) {
        if (opt_local_state->left_tail_id > batch_id && opt_local_state->right_tail_id > batch_id) {
            // both local tails have moved past batch_id, that batch is gone
            return false;
        }

        if ((batch = local_state_get_batch(opt_local_state, batch_id))) {
            out_lease->batch = batch;
            out_lease->found_locally = true;
            out_lease->broker = broker;
            out_lease->opt_local_state = opt_local_state;
            return true;
        }
    }

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

//

static void broker_checkout_all_batches(broker_t* broker, local_state_t* opt_local_state,
                                        lease_t** out_array, size_t* out_nr_of_batches)
{
    global_state_t* global_state = &broker->global_state;
    enif_mutex_lock(global_state->lock);

    const size_t nr_of_batches = cbroker_omap_size(global_state->batches);
    const size_t array_size = nr_of_batches * sizeof(lease_t);
    lease_t* array = cbroker_alloc(array_size);
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

    cbroker_free(array);
    *array_ptr = NULL;
}

//

static void broker_dtor(ErlNifEnv* caller_env, void* obj)
{
    cbroker_count_dtor(&nr_of_live_brokers);
    broker_t* broker = (broker_t*)obj;

    global_state_t* global_state = &broker->global_state;
    enif_mutex_destroy(global_state->lock);
    global_state->lock = NULL;

    mempool_destroy(&global_state->batch_pool);

    //

    for (size_t thread_id = 0; thread_id < broker->schedulers; thread_id++) {
        local_state_t* local_state = &broker->local_states[thread_id];
        void* destroy_ctx = global_state;
        cbroker_omap_destroy(local_state->batches, broker_dtor_cb_local_batch, destroy_ctx);
        local_state->batches = NULL;

        mempool_destroy(&local_state->request_pool);
        mempool_destroy(&local_state->ticket_pool);
        shenv_pool_destroy(&local_state->shenv_pool);
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
    batch_t* global_batch = NULL;
    bool present_in_global_state =
        cbroker_omap_lookup(global_state->batches, batch->id, (void**)&global_batch);
    assert(present_in_global_state);
    assert(global_batch == batch);
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

    cbroker_free(batch);
}

//

static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
    broker_t* broker = (broker_t*)obj;

    if (!broker->opts.depends_on_creator) {
        return;
    }

    ErlNifPid undefined_pid;
    enif_set_pid_undefined(&undefined_pid);

    shutdown_start(caller_env, broker, undefined_pid);
}

/*********************************************************************/

static ERL_NIF_TERM shutdown_start(ErlNifEnv* env, broker_t* broker, ErlNifPid self)
{
    global_state_t* global_state = &broker->global_state;
    bool is_closed = false;
    const bool is_caller_a_down_cb = enif_is_pid_undefined(&self);

    if (!atomic_compare_exchange_strong(&global_state->is_closed, &is_closed, true)) {
        // already closed, or in the process of closing
        return Atoms._ok;
    }

    if (!is_caller_a_down_cb) {
        enif_demonitor_process(env, broker, &broker->creator_mon);
    }

    // wait for any locked callers to clear out of the way
    enif_mutex_lock(global_state->lock);
    enif_mutex_unlock(global_state->lock);

    // from now on it's safe to access the global batch array

    shutdown_t* shutdown =
        cbroker_alloc_resource(&nr_of_live_shutdowns, ResourceTypes.shutdown, sizeof(shutdown_t));
    memset(shutdown, 0, sizeof(shutdown_t));

    shutdown->caller_pid = self;
    if (!is_caller_a_down_cb) {
        int mon_res = enif_monitor_process(env, shutdown, &self, &shutdown->caller_mon);
        assert(mon_res == 0);
    }

    shutdown->broker = broker;
    enif_keep_resource(broker); // to be released when we're done

    shutdown->batch_idx = 0;
    shutdown->cell_offset = 0;
    shutdown->phase = (is_caller_a_down_cb ? SHUTDOWN_PHASE_BACKUP : SHUTDOWN_PHASE_REGULAR);

    if (!is_caller_a_down_cb) {
        // Keep an extra reference to ensure that the monitor will be triggered
        // if the caller is killed in-between calls, since the latter then loses
        // the reference that was going to get passed.
        enif_keep_resource(shutdown);
    }

    return shutdown_continue(env, shutdown);
}

//

static ERL_NIF_TERM shutdown_continue(ErlNifEnv* env, shutdown_t* shutdown)
{
    // this is safe since no other thread will modify the global batch array at this point
    global_state_t* global_state = &shutdown->broker->global_state;
    const size_t nr_of_batches = cbroker_omap_size(global_state->batches);
    batch_t** batches = (batch_t**)cbroker_omap_values(global_state->batches);

    shutdown_pause_t pause = SHUTDOWN_CONTINUE;
    const bool was_initial_caller_a_down_cb = enif_is_pid_undefined(&shutdown->caller_pid);

    while (shutdown->batch_idx < nr_of_batches && pause == SHUTDOWN_CONTINUE) {
        batch_t* batch = batches[shutdown->batch_idx];
        pause = shutdown_batch(env, shutdown, batch);
    }

    //

    if (shutdown->batch_idx >= nr_of_batches) {
        broker_t* broker = shutdown->broker;
        memset(shutdown, 0, sizeof(shutdown_t));
        enif_release_resource(broker);

        enif_release_resource(shutdown);

        if (!was_initial_caller_a_down_cb) {
            enif_demonitor_process(env, shutdown, &shutdown->caller_mon);
            enif_release_resource(shutdown); // Extra reference (see shutdown_start)
        }

        return Atoms._ok;
    }
    else if (pause == SHUTDOWN_PAUSE_YIELD) {
        assert(shutdown->batch_idx < nr_of_batches);
        assert(shutdown->phase == SHUTDOWN_PHASE_REGULAR);
        ERL_NIF_TERM shutdown_term = enif_make_resource(env, shutdown);
        enif_release_resource(shutdown);
        return enif_schedule_nif(env, "nif_close", 0, nif_close, 1, &shutdown_term);
    }
    else {
        assert(pause == SHUTDOWN_PAUSE_NEXT_PHASE);
        // we'll resume execution after the monitor triggers
        assert(shutdown->batch_idx < nr_of_batches);
        assert(shutdown->phase == SHUTDOWN_PHASE_REGULAR);
        LOG_UNCOND("OJHHH!");
        return Atoms._ok;
    }
}

//

static void shutdown_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
    shutdown_t* shutdown = (shutdown_t*)obj;

    assert(shutdown->phase == SHUTDOWN_PHASE_REGULAR);
    shutdown->phase = SHUTDOWN_PHASE_BACKUP;

    ERL_NIF_TERM res = shutdown_continue(caller_env, shutdown);
    assert(res == Atoms._ok);
}

//

static void shutdown_dtor(ErlNifEnv* caller_env, void* obj)
{
    cbroker_count_dtor(&nr_of_live_shutdowns);
    shutdown_t* shutdown = (shutdown_t*)obj;

    // Assert that the DOWN callback was triggered and finished the job
    assert(shutdown->broker == NULL);
    assert(shutdown->batch_idx == 0);
    assert(shutdown->cell_offset == 0);
    assert(shutdown->phase == SHUTDOWN_PHASE_NONE);
    assert(shutdown->yield_copied_bytes == 0);
}

//

static shutdown_pause_t shutdown_batch(ErlNifEnv* env, shutdown_t* shutdown, batch_t* batch)
{
    const size_t nr_of_cells = batch->nr_of_cells;
    shutdown_pause_t pause = SHUTDOWN_CONTINUE;

    if (shutdown->cell_offset == 0) {
        size_t consumed_count = atomic_load(&batch->consumed_count);
        if (consumed_count >= nr_of_cells) {
            goto shutdown_batch_done;
        }

        atomic_store(&batch->consumed_count, batch->nr_of_cells);
        offset_t left_offset = atomic_exchange(&batch->left_tail, batch->nr_of_cells);
        offset_t right_offset = atomic_exchange(&batch->right_tail, batch->nr_of_cells);
        shutdown->cell_offset = MIN(left_offset, right_offset);
    }

    if (shutdown->cell_offset >= nr_of_cells) {
        goto shutdown_batch_done;
    }

    while (shutdown->cell_offset < nr_of_cells && pause == SHUTDOWN_CONTINUE) {
        cell_t* cell = &batch->cells[shutdown->cell_offset];
        pause = shutdown_cell(env, shutdown, cell);
        shutdown->cell_offset++;
    }

    if (shutdown->cell_offset >= nr_of_cells) {
    shutdown_batch_done:
        shutdown->batch_idx++;
        shutdown->cell_offset = 0;
    }

    return pause;
}

//

static shutdown_pause_t shutdown_cell(ErlNifEnv* env, shutdown_t* shutdown, cell_t* cell)
{
    request_t* request = atomic_load(cell);
    bool did_cancel = false;
    shutdown_pause_t pause = SHUTDOWN_CONTINUE;

    while (request != &sentinel_request_cancelled && request != &sentinel_request_matched) {
        if (atomic_compare_exchange_strong(cell, &request, &sentinel_request_cancelled)) {
            did_cancel = true;
            break;
        }
    }

    if (did_cancel && request != NULL) {
        stats_queue_balance_sub(&shutdown->broker->stats, request->weight);

        ticket_t* ticket = (ticket_t*)request->ticket;
        assert(ticket != NULL);

        if (enif_demonitor_process(env, ticket, &ticket->mon) == 0) {
            pause = shutdown_cell_notify(env, shutdown, request);
            ticket->request = NULL;

            shenv_reclaim(&request->shenv, NULL, &shutdown->broker->opts);
            assert(request->shenv == NULL);
            cbroker_free(request);
            enif_release_resource(shutdown->broker);
        }
        enif_release_resource(ticket);
    }

    return pause;
}

//

static shutdown_pause_t shutdown_cell_notify(ErlNifEnv* env, shutdown_t* shutdown,
                                             request_t* request)
{
    ERL_NIF_TERM msg = make_reply_drop(env, request, DROP_REASON_CLOSED);

    if (shutdown->phase == SHUTDOWN_PHASE_REGULAR) {
        if (!enif_send(env, &request->pid, NULL, msg) && (!enif_is_current_process_alive(env))) {
            // backup notification using NULL env
            either_notify_or_assert_not_alive(NULL, &request->pid, NULL, msg);
            return SHUTDOWN_PAUSE_NEXT_PHASE;
        }

        shutdown->yield_copied_bytes += term_size(env, msg);

        if (consume_timeslice(env, shutdown->yield_copied_bytes)) {
            shutdown->yield_copied_bytes = 0;
            return SHUTDOWN_PAUSE_YIELD;
        }
    }
    else {
        assert(shutdown->phase == SHUTDOWN_PHASE_BACKUP);
        either_notify_or_assert_not_alive(env, &request->pid, NULL, msg);
    }
    return SHUTDOWN_CONTINUE;
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

        if (atomic_load(&global_state->is_closed)) {
            // broker is either closed or being shutdown, we can't write to the global batch array
        }
        else if (cbroker_omap_lookup(global_state->batches, batch_id, (void**)&batch)) {
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
     * this behaves as a reference release. See `lease_ref_count_dec`. */
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
    ERL_NIF_TERM msg = make_reply_drop(env, request, reason);
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
    batch_t* batch = cbroker_alloc(size);
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
    atomic_store(&batch->left_tail, 0);
    atomic_store(&batch->right_tail, 0);
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
    ERL_NIF_TERM* cell_terms = cbroker_alloc(nr_of_cells * sizeof(ERL_NIF_TERM));

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
    const offset_t left_tail = atomic_load(&batch->left_tail);
    const offset_t right_tail = atomic_load(&batch->right_tail);
    const size_t consumed_count = atomic_load(&batch->consumed_count);

    ERL_NIF_TERM cell_terms_list =
        enif_make_list_from_array(env, cell_terms, (unsigned)nr_of_cells);
    cbroker_free(cell_terms);

    return enif_make_list6(
        env,
        //
        enif_make_tuple2(env, Atoms._id, enif_make_uint64(env, batch->id)),
        //
        enif_make_tuple2(env, Atoms._ref_count, enif_make_int64(env, ref_count)),
        //
        enif_make_tuple2(env, Atoms._left_tail, enif_make_uint64(env, left_tail)),
        //
        enif_make_tuple2(env, Atoms._right_tail, enif_make_uint64(env, right_tail)),
        //
        enif_make_tuple2(env, Atoms._consumed_count, enif_make_uint64(env, consumed_count)),
        //
        enif_make_tuple2(env, Atoms._cells, cell_terms_list));
}

/*********************************************************************/

static void batch_pool_init(mempool_t* pool, const pool_opts_t* opts, const size_t nr_of_cells)
{
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = batch_pool_cb_alloc;
    pool->clear_cb = batch_pool_cb_clear;
    pool->free_cb = batch_pool_cb_free;

    batch_pool_alloc_ctx_t alloc_ctx;
    memset(&alloc_ctx, 0, sizeof(batch_pool_alloc_ctx_t));
    alloc_ctx.nr_of_cells = nr_of_cells;

    mempool_init(pool, opts, &alloc_ctx);
}

static batch_t* batch_pool_get(mempool_t* pool, const size_t nr_of_cells)
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

    batch_t* batch = cbroker_alloc(size);
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

static void batch_pool_cb_free(void* obj) { cbroker_free(obj); }

//

static void request_pool_init(mempool_t* pool, const pool_opts_t* opts)
{
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = request_pool_cb_alloc;
    pool->clear_cb = request_pool_cb_clear;
    pool->free_cb = request_pool_cb_free;
    mempool_init(pool, opts, NULL);
}

static void* request_pool_cb_alloc(void* ctx)
{
    assert(ctx == NULL);
    request_t* request = cbroker_alloc(sizeof(request_t));
    memset(request, 0, sizeof(request_t));
    return request;
}

static void request_pool_cb_clear(void* obj)
{
    request_t* request = (request_t*)obj;
    assert(request->shenv == NULL);
    assert(request->broker == NULL);
    memset(request, 0, sizeof(request_t));
}

static void request_pool_cb_free(void* obj)
{
    request_t* request = (request_t*)obj;
    assert(request->shenv == NULL);
    assert(request->broker == NULL);
    cbroker_free(obj);
}

//

static void ticket_pool_init(mempool_t* pool, const pool_opts_t* opts)
{
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = ticket_pool_cb_alloc;
    pool->clear_cb = ticket_pool_cb_clear;
    pool->free_cb = ticket_pool_cb_free;
    mempool_init(pool, opts, NULL);
}

static void* ticket_pool_cb_alloc(void* ctx)
{
    assert(ctx == NULL);
    ticket_t* ticket =
        cbroker_alloc_resource(&nr_of_live_tickets, ResourceTypes.ticket, sizeof(ticket_t));
    memset(ticket, 0, sizeof(ticket_t));
    return ticket;
}

static void ticket_pool_cb_clear(void* obj)
{
    ticket_t* ticket = (ticket_t*)obj;
    memset(ticket, 0, sizeof(ticket_t));
}

static void ticket_pool_cb_free(void* obj) { enif_release_resource(obj); }

//

/*********************************************************************/

static void mempool_init(mempool_t* pool, const pool_opts_t* opts, void* alloc_ctx)
{
    assert(opts->initial_count <= opts->size);
    pool->count = opts->initial_count;
    pool->size = opts->size;
    pool->array = cbroker_alloc(pool->size * sizeof(void*));

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
        cbroker_free(pool->array);
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

static void shenv_pool_init(shenv_pool_t* pool) { memset(pool, 0, sizeof(shenv_pool_t)); }

//

static shenv_t* shenv_pool_get(shenv_pool_t* pool, size_t* out_idx)
{
    assert(pool->count <= pool->size);
    size_t idx = 0;
    ref_count_t request_count = 0;

    for (; idx < pool->count; idx++) {
        shenv_t* shenv = pool->array[idx];
        assert(shenv != NULL);

        request_count = atomic_load(&shenv->request_count);
        assert(request_count >= 0);

        if (request_count == 0 && !atomic_load(&shenv->env_cleared)) {
            // Another thread is still clearing the env (read-only variant)
            continue;
        }

        request_count =
            1 + atomic_fetch_add_explicit(&shenv->request_count, 1, memory_order_relaxed);
        assert(request_count >= 0);

        if (request_count == 1) {
            if (!atomic_load(&shenv->env_cleared)) {
                // Another thread is still clearing the env (revert increment variant)
                request_count =
                    (atomic_fetch_sub_explicit(&shenv->request_count, 1, memory_order_acq_rel) - 1);
                assert(request_count == 0);

                continue;
            }

            atomic_store(&shenv->env_cleared, false);
        }

        *out_idx = idx;
        return shenv;
    }

    return NULL;
}

//

static void shenv_pool_add(shenv_pool_t* pool, shenv_t* shenv)
{
    assert(pool->count <= pool->size);

    if (pool->count >= SHENV_POOL_MAX_COUNT && atomic_load(&shenv->ditched_by_writer)) {
        shenv_destroy(&shenv);
        return;
    }

    if (pool->count == pool->size) {
        pool->size = MAX(4, 2 * pool->size);

        if (pool->array == NULL) {
            pool->array = cbroker_alloc(pool->size * sizeof(shenv_t*));
        }
        else {
            pool->array = cbroker_realloc(pool->array, pool->size * sizeof(shenv_t*));
        }
    }

    pool->array[pool->count++] = shenv;
}

//

static void shenv_pool_remove(shenv_pool_t* pool, const size_t idx)
{
    assert(idx < pool->count);

    const size_t new_count = --pool->count;

    if (new_count < (pool->size >> 1)) {
        const size_t new_size = pool->size >> 1;
        shenv_t** new_array = cbroker_alloc(new_size * sizeof(shenv_t*));

        const size_t copy_before = idx * sizeof(shenv_t*);
        const size_t copy_after = (new_count - idx) * sizeof(shenv_t*);

        if (copy_before) {
            memcpy(new_array, pool->array, copy_before);
        }

        if (copy_after) {
            memcpy(&new_array[idx], &pool->array[idx + 1], copy_after);
        }

        cbroker_free(pool->array);

        pool->size = new_size;
        pool->array = new_array;
    }
    else {
        const size_t move_amount = (new_count - idx) * sizeof(shenv_t*);

        if (move_amount) {
            memmove(&pool->array[idx], &pool->array[idx + 1], move_amount);
        }
    }
}

//

static void shenv_pool_write(shenv_pool_t* shenv_pool, request_t* request, ask_ctx_t* ask_ctx)
{
    size_t shenv_idx = 0;
    shenv_t* shenv = NULL;

    if ((shenv = shenv_pool_get(shenv_pool, &shenv_idx))) {
        shenv_write(&shenv, request, ask_ctx);

        if (shenv == NULL) {
            // ditched
            shenv_pool_remove(shenv_pool, shenv_idx);
        }
    }
    else {
        shenv_write(&shenv, request, ask_ctx);

        if (shenv != NULL) {
            shenv_pool_add(shenv_pool, shenv);
        }
    }
}

//

static void shenv_pool_destroy(shenv_pool_t* pool)
{
    assert(pool->count <= pool->size);

    for (size_t idx = 0; idx < pool->count; idx++) {
        shenv_t** shenv_ptr = &pool->array[idx];
        shenv_destroy(shenv_ptr);
        assert(*shenv_ptr == NULL);
    }

    if (pool->array != NULL) {
        cbroker_free(pool->array);
    }

    memset(pool, 0, sizeof(shenv_pool_t));
}

/*********************************************************************/

static void shenv_write(shenv_t** shenv_ptr, request_t* request, ask_ctx_t* ask_ctx)
{
    shenv_t* shenv = *shenv_ptr;
    assert(ask_ctx->offer_size >= 0);
    size_t offer_size = (size_t)ask_ctx->offer_size;
    size_t copied_bytes = offer_size + term_size(ask_ctx->env, ask_ctx->reply_ref);

    if (shenv == NULL) {
        shenv = cbroker_alloc(sizeof(shenv_t));
        memset(shenv, 0, sizeof(shenv_t));

        atomic_store(&shenv->request_count, 1);

        shenv->env = cbroker_alloc_env();
        shenv->bytes_left = (budget_counter_t)ask_ctx->broker->opts.shared_env_budget;

        request->shenv = shenv;
        request->offer = enif_make_copy(shenv->env, ask_ctx->offer);
        request->reply_ref = enif_make_copy(shenv->env, ask_ctx->reply_ref);
        shenv->bytes_left -= (budget_counter_t)copied_bytes;

        shenv->copied_bytes += copied_bytes;
        shenv->copied_requests++;

        if (shenv->bytes_left > 0) {
            *shenv_ptr = shenv;
        }
        else {
            atomic_store(&shenv->ditched_by_writer, true);
        }
    }
    else {
        request->shenv = shenv;

        // We already set this when checking out of the pool.
        const ref_count_t request_count = atomic_load(&shenv->request_count);
        assert(request_count >= 1);

        shenv->bytes_left -= (budget_counter_t)copied_bytes;

        if (shenv->bytes_left <= 0) {
            atomic_store(&shenv->ditched_by_writer, true);
            *shenv_ptr = NULL;
        }

        request->offer = enif_make_copy(shenv->env, ask_ctx->offer);
        request->reply_ref = enif_make_copy(shenv->env, ask_ctx->reply_ref);

        shenv->copied_bytes += copied_bytes;
        shenv->copied_requests++;
    }
}

static void shenv_read(request_t* counter_request, ask_ctx_t* ask_ctx,
                       ERL_NIF_TERM* out_counter_tag, ERL_NIF_TERM* out_counter_offer)
{
    const size_t offer_size = counter_request->offer_size;
    shenv_t* shenv = counter_request->shenv;
    assert(shenv != NULL);
    assert(atomic_load(&shenv->request_count) >= 1);

    //

    if (counter_request->reply_ref == Atoms._ticket) {
        *out_counter_tag = enif_make_resource(ask_ctx->env, counter_request->ticket);
    }
    else {
        *out_counter_tag = enif_make_copy(ask_ctx->env, counter_request->reply_ref);
        ask_ctx->copied_bytes += term_size(ask_ctx->env, *out_counter_tag);
    }

    //

    *out_counter_offer = enif_make_copy(ask_ctx->env, counter_request->offer);
    ask_ctx->copied_bytes += offer_size;
}

static void shenv_reclaim(shenv_t** shenv_ptr, local_state_t* opt_local_state,
                          const broker_opts_t* broker_opts)
{
    shenv_t* shenv = *shenv_ptr;
    assert(shenv != NULL);

    ref_count_t request_count =
        (atomic_fetch_sub_explicit(&shenv->request_count, 1, memory_order_acq_rel) - 1);
    assert(request_count >= 0);

    //

    if (request_count > 0) {
        *shenv_ptr = NULL;
        return;
    }

    const bool ditched_by_writer = atomic_load(&shenv->ditched_by_writer);
    const bool clean_env = (!ditched_by_writer || (opt_local_state != NULL));

    if (clean_env) {
        enif_clear_env(shenv->env);
        shenv->bytes_left = (budget_counter_t)broker_opts->shared_env_budget;

        if (ditched_by_writer && opt_local_state != NULL) {
            shenv_pool_add(&opt_local_state->shenv_pool, shenv);
        }
        else if (!ditched_by_writer) {
            atomic_store(&shenv->env_cleared, true);
        }
        *shenv_ptr = NULL;
    }
    else if (ditched_by_writer) {
        shenv_destroy(shenv_ptr);
    }
    else {
        *shenv_ptr = NULL;
    }
}

static void shenv_destroy(shenv_t** shenv_ptr)
{
    shenv_t* shenv = *shenv_ptr;
    if (shenv != NULL) {
        assert(atomic_load(&shenv->request_count) == 0);

        // LOG("copied_bytes = %llu, copied_requests = %llu",
        // shenv->copied_bytes, shenv->copied_requests);

        cbroker_free_env(shenv->env);
        cbroker_free(shenv);
        *shenv_ptr = NULL;
    }
}

/*********************************************************************/

static queue_balance_t stats_queue_balance_add(stats_t* stats, queue_balance_t weight)
{
    queue_balance_t balance =
        weight + atomic_fetch_add_explicit(&stats->queue_balance, weight, memory_order_relaxed);

    return balance;
}

static void stats_queue_balance_sub(stats_t* stats, queue_balance_t weight)
{
    atomic_fetch_sub_explicit(&stats->queue_balance, weight, memory_order_relaxed);
}

static ERL_NIF_TERM stats_to_term(ErlNifEnv* env, stats_t* stats)
{
    queue_balance_t queue_balance =
        atomic_load_explicit(&stats->queue_balance, memory_order_relaxed);

    return enif_make_list1(
        env,
        //
        enif_make_tuple2(env, Atoms._queue_balance, enif_make_int64(env, queue_balance)));
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

static int get_broker_opt(ErlNifEnv* env, ERL_NIF_TERM key, ERL_NIF_TERM value,
                          broker_opts_t* out_opts)
{
    if (key == Atoms._ask_credits) {
        return (enif_get_int(env, value, &out_opts->ask_credits) && out_opts->ask_credits > 0);
    }
    else if (key == Atoms._ask_max_tries) {
        return get_size_t(env, value, &out_opts->ask_max_tries) && out_opts->ask_max_tries > 0;
    }
    else if (key == Atoms._batch_pool) {
        return get_pool_opts(env, value, &out_opts->batch_pool);
    }
    else if (key == Atoms._cells_per_batch) {
        return (get_size_t(env, value, &out_opts->cells_per_batch) &&
                out_opts->cells_per_batch > 0);
    }
    else if (key == Atoms._depends_on_creator) {
        return get_boolean(value, &out_opts->depends_on_creator);
    }
    else if (key == Atoms._max_queue_len) {
        // shorthand for setting both min and max balance at the same time
        size_t max_queue_len = 0;

        if (value == Atoms._unlimited) {
            out_opts->min_left_balance = QUEUE_BALANCE_UNLIMITED_LEFT;
            out_opts->max_right_balance = QUEUE_BALANCE_UNLIMITED_RIGHT;
            return 1;
        }
        else if (get_size_t(env, value, &max_queue_len) && max_queue_len > 0 &&
                 max_queue_len <= (size_t)QUEUE_BALANCE_MAX) {
            out_opts->min_left_balance = -(queue_balance_t)max_queue_len;
            out_opts->max_right_balance = (queue_balance_t)max_queue_len;
            return 1;
        }
        else {
            return 0;
        }
    }
    else if (key == Atoms._max_right_balance) {
        if (value == Atoms._unlimited) {
            out_opts->max_right_balance = QUEUE_BALANCE_UNLIMITED_RIGHT;
            return 1;
        }
        else {
            return (get_ptrdiff_t(env, value, &out_opts->max_right_balance) &&
                    out_opts->max_right_balance > 0);
        }
    }
    else if (key == Atoms._min_left_balance) {
        if (value == Atoms._unlimited) {
            out_opts->min_left_balance = QUEUE_BALANCE_UNLIMITED_LEFT;
            return 1;
        }
        else {
            return (get_ptrdiff_t(env, value, &out_opts->min_left_balance) &&
                    out_opts->min_left_balance < 0);
        }
    }
    else if (key == Atoms._request_pool) {
        return get_pool_opts(env, value, &out_opts->request_pool);
    }
    else if (key == Atoms._shared_env_budget) {
        return (get_size_t(env, value, &out_opts->shared_env_budget) &&
                out_opts->shared_env_budget > 0 &&
                out_opts->shared_env_budget <= (size_t)BUDGET_COUNTER_MAX);
    }
    else if (key == Atoms._ticket_pool) {
        return get_pool_opts(env, value, &out_opts->ticket_pool);
    }
    else {
        return 0;
    }
}

static ERL_NIF_TERM get_broker_opts(ErlNifEnv* env, ERL_NIF_TERM term, broker_opts_t* out_opts)
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
            return make_badopt(env, head);
        }

        //

        if (get_broker_opt(env, key, value, out_opts)) {
            continue;
        }
        else {
            return make_badopt(env, head);
        }
    }

    //

    if (enif_is_empty_list(env, term)) {
        return Atoms._ok;
    }

    return make_badopts(env, term);
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

static int get_pool_opts(ErlNifEnv* env, ERL_NIF_TERM term, pool_opts_t* out_opts)
{
    ERL_NIF_TERM head, tail, key, value;
    int arity = 0;
    const ERL_NIF_TERM* elements;

    size_t size = 0;
    bool size_set = false;

    size_t initial_count = 0;
    bool initial_count_set = false;

    //

    while (enif_get_list_cell(env, term, &head, &tail)) {
        term = tail;

        if (!(enif_get_tuple(env, head, &arity, &elements) && arity == 2 &&
              enif_is_atom(env, elements[0]))) {
            return 0;
        }
        key = elements[0];
        value = elements[1];

        if (key == Atoms._size) {
            if (!get_size_t(env, value, &size)) {
                return 0;
            }
            size_set = true;
        }
        else if (key == Atoms._initial_count) {
            if (!get_size_t(env, value, &initial_count)) {
                return 0;
            }
            initial_count_set = true;
        }
        else {
            return 0;
        }
    }

    //

    if (size_set) {
        out_opts->size = size;
        out_opts->initial_count =
            (initial_count_set ? initial_count : MIN(out_opts->initial_count, size));
    }
    else if (initial_count_set) {
        out_opts->initial_count = initial_count;
        out_opts->size = MAX(out_opts->size, initial_count);
    }

    //

    if (out_opts->initial_count > out_opts->size) {
        return 0;
    }

    return enif_is_empty_list(env, term);
}

//

static int get_retry(ErlNifEnv* env, ERL_NIF_TERM term, retry_t** out_retry)
{
    return enif_get_resource(env, term, ResourceTypes.retry, (void**)out_retry);
}

//

static int get_ptrdiff_t(ErlNifEnv* env, ERL_NIF_TERM term, ptrdiff_t* out)
{
    int64_t value;

    if (!enif_get_int64(env, term, &value)) {
        return 0;
    }
#if PTRDIFF_MAX < INT64_MAX
    if (value < PTRDIFF_MIN || value > PTRDIFF_MAX) {
        return 0;
    }
#endif
    *out = (ptrdiff_t)value;
    return 1;
}

//

static int get_shutdown(ErlNifEnv* env, ERL_NIF_TERM term, shutdown_t** out_shutdown)
{
    return enif_get_resource(env, term, ResourceTypes.shutdown, (void**)out_shutdown);
}

//

static int get_size_t(ErlNifEnv* env, ERL_NIF_TERM term, size_t* out)
{
    uint64_t value;

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

//

static int get_ticket(ErlNifEnv* env, ERL_NIF_TERM term, ticket_t** out_ticket)
{
    return enif_get_resource(env, term, ResourceTypes.ticket, (void**)out_ticket);
}

/*********************************************************************/

static ERL_NIF_TERM make_await(ErlNifEnv* env, ERL_NIF_TERM ticket)
{
    return enif_make_tuple2(env, Atoms._await, ticket);
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

static ERL_NIF_TERM make_boolean(int value) { return (value ? Atoms._true : Atoms._false); }

static ERL_NIF_TERM make_broker_opts(ErlNifEnv* env, const broker_opts_t* opts)
{
    ERL_NIF_TERM term_min_left_balance =
        (opts->min_left_balance > 0 ? Atoms._unlimited
                                    : enif_make_int64(env, opts->min_left_balance));

    ERL_NIF_TERM term_max_right_balance =
        (opts->max_right_balance < 0 ? Atoms._unlimited
                                     : enif_make_int64(env, opts->max_right_balance));

    return enif_make_list9(
        env,
        //
        enif_make_tuple2(env, Atoms._depends_on_creator, make_boolean(opts->depends_on_creator)),
        //
        enif_make_tuple2(env, Atoms._cells_per_batch, enif_make_uint64(env, opts->cells_per_batch)),
        //
        enif_make_tuple2(env, Atoms._min_left_balance, term_min_left_balance),
        //
        enif_make_tuple2(env, Atoms._max_right_balance, term_max_right_balance),
        //
        enif_make_tuple2(env, Atoms._ask_credits, enif_make_int(env, opts->ask_credits)),
        //
        enif_make_tuple2(env, Atoms._ask_max_tries, enif_make_uint64(env, opts->ask_max_tries)),
        //
        enif_make_tuple2(env, Atoms._batch_pool, make_pool_opts(env, &opts->batch_pool)),
        //
        enif_make_tuple2(env, Atoms._request_pool, make_pool_opts(env, &opts->request_pool)),
        //
        enif_make_tuple2(env, Atoms._ticket_pool, make_pool_opts(env, &opts->ticket_pool)));
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
        return Atoms._match_not_found;

    case DROP_REASON_TOO_MANY_RETRIES:
        return Atoms._too_many_tries;

    case DROP_REASON_LANE_FULL:
        return Atoms._full_lane;

    default:
        assert(reason == DROP_REASON_CLOSED);
        return Atoms._closed;
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

#ifdef CBROKER_COUNT_ALLOCS
static ERL_NIF_TERM make_perfcounter(ErlNifEnv* env, ERL_NIF_TERM key, _Atomic(int64_t)* counter)
{
    int64_t value = atomic_load_explicit(counter, memory_order_relaxed);
    return enif_make_tuple2(env, key, enif_make_int64(env, value));
}
#endif

static ERL_NIF_TERM make_pool_opts(ErlNifEnv* env, const pool_opts_t* opts)
{
    return enif_make_list2(
        env,
        //
        enif_make_tuple2(env, Atoms._size, enif_make_uint64(env, opts->size)),
        //
        enif_make_tuple2(env, Atoms._initial_count, enif_make_uint64(env, opts->initial_count)));
}

static ERL_NIF_TERM make_reply(ErlNifEnv* env, ERL_NIF_TERM tag, ERL_NIF_TERM reply)
{
    return enif_make_tuple2(env, tag, reply);
}

static ERL_NIF_TERM make_reply_drop(ErlNifEnv* env, request_t* request, const drop_reason_t reason)
{
    assert(request->ticket != NULL);
    int64_t sojourn_time = monotonic_ts() - request->enqueue_ts;

    ERL_NIF_TERM tag =
        (request->reply_ref == Atoms._ticket ? enif_make_resource(env, request->ticket)
                                             : enif_make_copy(env, request->reply_ref));

    ERL_NIF_TERM cancelled = make_drop(env, reason, sojourn_time);
    return make_reply(env, tag, cancelled);
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

static inline int consume_timeslice(ErlNifEnv* env, const size_t copied_bytes)
{
    if (copied_bytes == 0) {
        return 0;
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
        return enif_consume_timeslice(env, percent);
    }
    return 0;
}

static ErlNifTime monotonic_ts() { return enif_monotonic_time(ERL_NIF_NSEC); }
