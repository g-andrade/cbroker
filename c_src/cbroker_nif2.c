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
/*#define LOG(fmt, ...) do { \
    enif_fprintf(stderr, fmt "\n\r", ##__VA_ARGS__); \
     fflush(stderr); \
} while (0)*/

/*********************************************************************/

#define SPIN_LOCK_CREATING 1
#define SPIN_LOCK_DONE 2

/* Determined very informally, can probably be optimized (and different between
 * match and envs pools)
 */
#define INITIAL_MEMPOOL_SIZE 256
#define TARGET_MEMPOOL_SIZE 512

/*********************************************************************/

//

//

typedef uint_fast64_t waiter_id_t;
typedef uint_fast8_t spin_lock_t;

typedef struct waiter {
    _Atomic(spin_lock_t) spin_lock;
    waiter_id_t id;
    ErlNifPid pid;
    ErlNifEnv* env;
    ERL_NIF_TERM exchange_value;
    ERL_NIF_TERM tag_term;
    bool with_stats;
    ErlNifTime enqueue_ts;
    struct waiter* next;
} waiter_t;

//

typedef struct {
    ErlNifMonitor mon;
    ErlNifEnv* env;
    ERL_NIF_TERM broker_term;
    waiter_id_t waiter_id;
} tag_t;

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
    mempool_t waiter_pool;
    mempool_t env_pool;
} local_state_t;

//

enum BrokerFlags {
    BFLAGS_LEFT = 1,
    BFLAGS_RIGHT = 2
};

typedef enum BrokerFlags broker_flags_t;

//

typedef int_fast64_t balance_t;

typedef struct {
    size_t nr_of_schedulers;
    _Atomic(balance_t) balance;
    //
    ErlNifMutex* lock;
    broker_flags_t flags;
    waiter_id_t counter;
    //cbroker_omap_t* queue;
    ssize_t count;
    waiter_t* head;
    waiter_t* tail;
    //
    local_state_t local_states[];
} broker_t;

typedef ssize_t thread_id_t;

//

/*********************************************************************/

static int on_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info);
;
static void init_atoms(ErlNifEnv* caller_env);
static void broker_resource_load(ErlNifEnv* caller_env);
static void tag_resource_load(ErlNifEnv* caller_env);

static ERL_NIF_TERM nif_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
static ERL_NIF_TERM nif_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);
//static ERL_NIF_TERM nif_to_list(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]);

static ErlNifEnv* env_pool_get(local_state_t* local_state);
static void env_pool_return(local_state_t* local_state, ErlNifEnv* env);
static void* env_pool_alloc_env(void);
static void env_pool_clear_env(void* env);
static void env_pool_free_env(void* env);

static void* waiter_pool_alloc_waiter(void);
static void waiter_pool_clear_waiter(void* waiter);
static void waiter_pool_free_waiter(void* waiter);
static waiter_t* waiter_pool_get(local_state_t* local_state);
static void waiter_pool_return(local_state_t* local_state, waiter_t* waiter);

static void mempool_init(mempool_t* pool);
static void* mempool_get(mempool_t* pool);
static void mempool_return(mempool_t* pool, void* obj);
static void mempool_destroy(mempool_t* pool);

static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out);
static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, tag_t** out);
//static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out_broker, waiter_id_t* out_waiter_id);
//static int get_waiter_id(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM* out_side, waiter_id_t *out_waiter_id);

static ERL_NIF_TERM make_badarg(ErlNifEnv* env, ERL_NIF_TERM term);

static const thread_id_t get_or_assign_thread_id(void);

static void broker_dtor(ErlNifEnv* caller_env, void* obj);
static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon);

static void tag_dtor(ErlNifEnv* caller_env, void* obj);
static void tag_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon);

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

static ErlNifFunc nif_funcs[] = {{"new", 0, nif_new},       
                                 {"ask", 3, nif_ask},
                                 {"ask", 4, nif_ask},       
                                 {"ask", 5, nif_ask},
                                 {"cancel", 2, nif_cancel}};

ERL_NIF_INIT(cbroker_nif2, nif_funcs, on_load, NULL, NULL, NULL);

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
    tag_resource_load(caller_env);

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

/*********************************************************************/

// FIXME FIXME location

static void init_waiter_pool(mempool_t* pool) {
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = waiter_pool_alloc_waiter;
    pool->clear_cb = waiter_pool_clear_waiter;
    pool->free_cb = waiter_pool_free_waiter;
    mempool_init(pool);
}

static void init_env_pool(mempool_t* pool) {
    memset(pool, 0, sizeof(mempool_t));
    pool->alloc_cb = env_pool_alloc_env;
    pool->clear_cb = env_pool_clear_env;
    pool->free_cb = env_pool_free_env;
    mempool_init(pool);
}

static size_t sizeof_broker(size_t nr_of_schedulers)
{
    assert(nr_of_schedulers > 0);
    return sizeof(broker_t) + (nr_of_schedulers * sizeof(local_state_t));
}

static local_state_t* broker_local_state(broker_t* broker) {
    thread_id_t thread_id = get_or_assign_thread_id();
    assert(thread_id >= 0);
    assert(thread_id < broker->nr_of_schedulers);
    return &broker->local_states[thread_id];
}

static waiter_t* broker_cancel_waiter(broker_t* broker, const waiter_id_t waiter_id) {
    waiter_t* cancelled_waiter = NULL;
    balance_t balance_increment = 0;

    /////////

    enif_mutex_lock(broker->lock);

    waiter_t* waiter = broker->head;
    waiter_t* prev = NULL;


    while (waiter != NULL) {
        if (waiter->id == waiter_id) {
            LOG("[cancel] Found waiter %llu", waiter->id);

            if (prev == NULL) {
                broker->head = waiter->next;

                if (broker->tail == waiter) {
                    broker->tail = NULL;
                }
            }
            else {
                prev->next = waiter->next;

                if (broker->tail == waiter) {
                    broker->tail = prev;
                }
            }

            assert(--broker->count >= 0);
        
            //

            if (broker->flags & BFLAGS_LEFT) {
                balance_increment = +1;
            }
            else {
                balance_increment = -1;
            }

            if (broker->count == 0) {
                LOG("[cancel] Queue is now empty");
                broker->flags = BFLAGS_LEFT | BFLAGS_RIGHT;
            }

            cancelled_waiter = waiter;
            break;
        }
    }

    enif_mutex_unlock(broker->lock);

    /////

    if (balance_increment != 0) {
        atomic_fetch_add_explicit(&broker->balance, balance_increment, memory_order_relaxed);
    }

    return cancelled_waiter;
}

static ErlNifTime monotonic_ts() {
    return enif_monotonic_time(ERL_NIF_NSEC);
}

static tag_t* tag_new(ErlNifEnv* env,
                      local_state_t* local_state,
                      ErlNifPid pid,
                      ERL_NIF_TERM broker_term,
                      waiter_id_t waiter_id)
{
    tag_t* tag = enif_alloc_resource(ResourceTypes.tag, sizeof(tag_t));
    memset(tag, 0, sizeof(tag_t));

    int res = enif_monitor_process(env, tag, &pid, &tag->mon);
    assert(res == 0);

    tag->env = env_pool_get(local_state);
    tag->broker_term = enif_make_copy(tag->env, broker_term);
    tag->waiter_id = waiter_id;

    return tag;
}

static waiter_t* waiter_new(local_state_t* local_state, 
                            waiter_id_t id, 
                            ErlNifPid self, 
                            ERL_NIF_TERM exchange_value,
                            bool with_stats)
{
    waiter_t* waiter = waiter_pool_get(local_state);
    memset(waiter, 0, sizeof(waiter_t));

    atomic_store(&waiter->spin_lock, SPIN_LOCK_CREATING);

    waiter->id = id;
    waiter->pid = self;
    
    waiter->env = env_pool_get(local_state);
    waiter->exchange_value = enif_make_copy(waiter->env, exchange_value);
    waiter->tag_term = Atoms._none;
    waiter->with_stats = true;

    waiter->enqueue_ts = monotonic_ts();

    return waiter;
}

static void waiter_demonitor(ErlNifEnv* env, local_state_t* local_state, waiter_t* waiter) {
    tag_t* tag = NULL;

    int int_res = get_tag(waiter->env, waiter->tag_term, &tag);
    assert(int_res);

    if (enif_demonitor_process(env, tag, &tag->mon) == 0) {
        assert(tag->env != NULL);
        env_pool_return(local_state, tag->env);
        tag->env = NULL;
    }
}

static void waiter_release(local_state_t* local_state, waiter_t** waiter_ptr) {
    waiter_t* waiter = *waiter_ptr;

    if (waiter->env != NULL) {
        env_pool_return(local_state, waiter->env);
        waiter->env = NULL;
    }

    waiter_pool_return(local_state, waiter);

    *waiter_ptr = NULL;
}

// static ERL_NIF_TERM make_waiter_id(ErlNifEnv* env, bool is_left, waiter_id_t waiter_id) {
//     assert(waiter_id <= INT64_MAX);
//     int64_t signed_id = (is_left ? -waiter_id : waiter_id);
//     return enif_make_int64(env, signed_id);
// }

// static ERL_NIF_TERM make_tag(ErlNifEnv* env, ERL_NIF_TERM broker_term, bool is_left, waiter_id_t waiter_id) {
//     ERL_NIF_TERM id_term = make_waiter_id(env, is_left, waiter_id);
//     return enif_make_list_cell(env, broker_term, id_term);
// }

static ERL_NIF_TERM make_await(ErlNifEnv* env, ERL_NIF_TERM tag) {
    return enif_make_tuple2(env, Atoms._await, tag);
}

static ERL_NIF_TERM make_match(ErlNifEnv* env, 
                               ERL_NIF_TERM match_ref, 
                               ERL_NIF_TERM exchange_value) 
{
    return enif_make_tuple3(env, Atoms._match, match_ref, exchange_value);
}

static ERL_NIF_TERM make_match_with_stats(ErlNifEnv* env, 
                                          ERL_NIF_TERM match_ref, 
                                          ERL_NIF_TERM exchange_value,
                                          int64_t sojourn_time)
{
    ERL_NIF_TERM sojourn_time_term = enif_make_int64(env, sojourn_time);
    return enif_make_tuple4(env, Atoms._match, match_ref, exchange_value, sojourn_time_term);

}

static void notify_of_match(ErlNifEnv* env, 
                            const ErlNifPid pid,
                            const ERL_NIF_TERM tag_term,
                            const ERL_NIF_TERM match_ref,
                            const ERL_NIF_TERM exchange_value,
                            int64_t sojourn_time,
                            bool with_stats)
{
    ERL_NIF_TERM msg_content = (
            with_stats
            ? make_match_with_stats(env, match_ref, exchange_value, sojourn_time)
            : make_match(env, match_ref, exchange_value)
    );
    ERL_NIF_TERM msg = enif_make_tuple2(env, tag_term, msg_content);
    enif_send(env, &pid, NULL, msg);
}

static void notify_of_match_using_our_waiter(ErlNifEnv* env, 
                                             local_state_t* local_state,
                                             waiter_t* other_waiter,
                                             ERL_NIF_TERM match_ref,
                                             int64_t sojourn_time,
                                             waiter_t** our_waiter_ptr)
{
    waiter_t* our_waiter = *our_waiter_ptr;
    assert(our_waiter != NULL);

    // Reuse waiter env which we would discard anyway
    ErlNifEnv* msg_env = our_waiter->env;

    ERL_NIF_TERM tag_copy = enif_make_copy(msg_env, other_waiter->tag_term);
    ERL_NIF_TERM match_ref_copy = enif_make_copy(msg_env, match_ref);
    ERL_NIF_TERM exchange_value_copy = enif_make_copy(msg_env, our_waiter->exchange_value);

    ERL_NIF_TERM msg_content = (
            other_waiter->with_stats 
            ? make_match_with_stats(msg_env, match_ref_copy, exchange_value_copy, sojourn_time)
            : make_match(msg_env, match_ref_copy, exchange_value_copy)
    );
    ERL_NIF_TERM msg = enif_make_tuple2(msg_env, tag_copy, msg_content);

    enif_send(env, &other_waiter->pid, msg_env, msg);

    env_pool_return(local_state, msg_env);
    waiter_release(local_state, our_waiter_ptr);
    assert(*our_waiter_ptr == NULL);
}

static void notify_of_cancellation(ErlNifEnv* env, ErlNifPid pid, ERL_NIF_TERM tag_term) {
    ERL_NIF_TERM msg = enif_make_tuple2(env, tag_term, Atoms._cancelled);
    enif_send(env, &pid, NULL, msg);
}

static int exp_percentage(ErlNifTime enqueue_ts) {
    int64_t sojourn = monotonic_ts() - enqueue_ts;
    return MAX(1, MIN(100, sojourn / 10000));
}

/*********************************************************************/

static ERL_NIF_TERM nif_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifSysInfo sys_info;
    enif_system_info(&sys_info, sizeof(sys_info));
    size_t nr_of_schedulers = sys_info.scheduler_threads;
    assert(nr_of_schedulers > 0);

    const size_t broker_size = sizeof_broker(nr_of_schedulers);
    broker_t* broker = enif_alloc_resource(ResourceTypes.broker, broker_size);
    memset(broker, 0, broker_size);

    broker->nr_of_schedulers = nr_of_schedulers;

    broker->lock = enif_mutex_create("cbroker.broker.lock");
    broker->flags = BFLAGS_LEFT | BFLAGS_RIGHT;
    //broker->queue = cbroker_omap_new();

    for (thread_id_t thread_id = 0; thread_id < nr_of_schedulers; thread_id++) {
        local_state_t* local_state = &broker->local_states[thread_id];
        init_waiter_pool(&local_state->waiter_pool);
        init_env_pool(&local_state->env_pool);
    }

    ERL_NIF_TERM broker_term = enif_make_resource(env, broker);
    enif_release_resource(broker);
    return broker_term;
}

//

static ERL_NIF_TERM nif_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    broker_t* broker = NULL;
    bool is_left = false;
    broker_flags_t flags = 0;
    bool with_stats = false;
    ErlNifPid self;
    ERL_NIF_TERM self_term;
    ErlNifTime enqueue_ts = monotonic_ts();

    ERL_NIF_TERM broker_term = argv[0];
    ERL_NIF_TERM side = argv[1];
    ERL_NIF_TERM exchange_value = argv[2];
    ERL_NIF_TERM with_stats_term = (argc >= 4 ? argv[3] : Atoms._false);
    ERL_NIF_TERM ask_type = (argc >= 5 ? argv[4] : Atoms._regular);

    if (!get_broker(env, broker_term, &broker)) {
        return make_badarg(env, broker_term);
    }

    if (side == Atoms._left) {
        flags = BFLAGS_LEFT;
        is_left = true;
    }
    else if (side == Atoms._right) {
        flags = BFLAGS_RIGHT;
    } 
    else {
        return make_badarg(env, side);
    }


    if (with_stats_term == Atoms._true) {
        with_stats = true;
    }
    else if (with_stats_term != Atoms._false) {
        return make_badarg(env, with_stats_term);
    }

    if (!(ask_type == Atoms._regular || ask_type == Atoms._nb || ask_type == Atoms._fully_async)) {
        return make_badarg(env, ask_type);
    }

    if (enif_self(env, &self)) {
        self_term = enif_make_pid(env, &self);
    } else {
        return enif_make_badarg(env);
    }

    ///////////////////////

    //cbroker_omap_result_t map_res = CBROKER_OMAP_NOMEM;
    //bool bool_res = false;
    local_state_t* local_state = broker_local_state(broker);

    //

    balance_t balance_increment = (is_left ? -1 : +1);
    balance_t prev_balance = atomic_fetch_add_explicit(&broker->balance, balance_increment, memory_order_relaxed);
    LOG("[%T] [ask] Prev balance estimate: %d", self_term, prev_balance);

    waiter_t* our_waiter = NULL;
    //tag_t* our_tag = NULL;
    //ERL_NIF_TERM our_tag_term;

    waiter_t* other_waiter = NULL;
    bool was_enqueued = false;

    balance_t preemptive_alloc_threshold = 4; // MAX(1, broker->nr_of_schedulers >> 1);
    bool preemptively_alloc_waiter = (
        is_left 
        ? prev_balance <= -preemptive_alloc_threshold
        : prev_balance >= preemptive_alloc_threshold
    );

    if (preemptively_alloc_waiter) {
        LOG("[%T] [ask] Preemptively allocating waiter", self_term);
        our_waiter = waiter_new(local_state, 0, self, exchange_value, with_stats);
    }

    //////////////////////

    enif_mutex_lock(broker->lock);
    assert(broker->count >= 0);

    if (broker->flags & flags) {
        LOG("[%T] [ask] Enqueuing on queue with size %llu", self_term, cbroker_omap_size(broker->queue));

        waiter_id_t our_id = ++broker->counter;

        if (our_waiter == NULL) {
            LOG("[%T] [ask] Allocating waiter %llu within critical section", self_term, our_id);
            our_waiter = waiter_new(local_state, our_id, self, exchange_value, with_stats);
        }
        else {
            LOG("[%T] [ask] Assigning id %llu to prev allocated waiter", self_term, our_id);
            our_waiter->id = our_id;
        }

        if (broker->count++ == 0) {
            assert(broker->head == NULL);
            assert(broker->tail == NULL);
            broker->head = our_waiter;
            broker->tail = our_waiter;
            broker->flags = (is_left ? BFLAGS_LEFT : BFLAGS_RIGHT);
        }
        else {
            assert(broker->head != NULL);
            assert(broker->tail != NULL);
            broker->tail->next = our_waiter;
            broker->tail = our_waiter;
        }

        enif_mutex_unlock(broker->lock);
        was_enqueued = true;
    }
    else {
        assert(broker->count > 0);
        assert(broker->head != NULL);
        assert(broker->tail != NULL);

        ssize_t new_count = --broker->count;
        assert(new_count >= 0);

        other_waiter = broker->head;
        broker->head = other_waiter->next;
        other_waiter->next = NULL;

        if (new_count == 0) {
            broker->tail = NULL;
            assert(broker->head == NULL);
            LOG("[%T] [ask] Queue is now empty", self_term);
            broker->flags = BFLAGS_LEFT | BFLAGS_RIGHT;
        }
        else {
            assert(broker->head != NULL);
            LOG("[%T] [ask] Queue size is now %llu", self_term, qs);
        }

        enif_mutex_unlock(broker->lock);
    }

    //////////////////////

    if (was_enqueued) {
        assert(our_waiter != NULL);

        LOG("[%T] [ask] Creating tag", self_term);
        tag_t* tag = tag_new(env, local_state, self, broker_term, our_waiter->id);
        ERL_NIF_TERM tag_term = enif_make_resource(env, tag);
        our_waiter->tag_term = enif_make_copy(our_waiter->env, tag_term);
        enif_release_resource(tag);

        spin_lock_t prev_slock_value = atomic_exchange(&our_waiter->spin_lock, SPIN_LOCK_DONE);
        assert(prev_slock_value == SPIN_LOCK_CREATING);

        return make_await(env, tag_term);
    }
    
    /////////
    // got a match

    assert(other_waiter != NULL);

    LOG("[%T] [ask] Match: waiting on spin lock for %llu...", self_term, other_waiter->id);
    spin_lock_t slock_v = 0;
    do {
        slock_v = atomic_load(&other_waiter->spin_lock);
        LOG("slock: %u", slock_v);
        assert(slock_v != 0);
    } while (slock_v != SPIN_LOCK_DONE);

    LOG("[%T] [ask] Match good to go", self_term);

    ERL_NIF_TERM match_ref = enif_make_ref(env);
    int64_t our_sojourn_time = monotonic_ts() - enqueue_ts;
    int64_t other_sojourn_time = monotonic_ts() - other_waiter->enqueue_ts;

    // notify other
    waiter_demonitor(env, local_state, other_waiter);

    if (our_waiter != NULL) {
        LOG("[%T] [ask] Match: notifying other using our own waiter's env", self_term);
        notify_of_match_using_our_waiter(env,
                                         local_state,
                                         other_waiter,
                                         match_ref,
                                         other_sojourn_time,
                                         &our_waiter);

        assert(our_waiter == NULL);
    }
    else {
        LOG("[%T] [ask] Match: notifying other", self_term);
        ERL_NIF_TERM other_tag_copy = enif_make_copy(env, other_waiter->tag_term);
        notify_of_match(env,
                        other_waiter->pid,
                        other_tag_copy,
                        match_ref,
                        exchange_value,
                        other_sojourn_time,
                        other_waiter->with_stats);
    }

    // notify ourselves

    ERL_NIF_TERM match_res = Atoms._none;
    ERL_NIF_TERM other_exchange_value = enif_make_copy(env, other_waiter->exchange_value);

    if (ask_type == Atoms._fully_async) {
        LOG("[%T] [ask] Match: notifying ourselves asynchronously", self_term);
        ERL_NIF_TERM faux_tag = enif_make_ref(env);
        notify_of_match(env,
                        self,
                        faux_tag,
                        match_ref,
                        other_exchange_value,
                        our_sojourn_time,
                        with_stats);

        match_res = make_await(env, faux_tag);
    }
    else {
        LOG("[%T] [ask] Match: returning", self_term);
        match_res = (
            with_stats 
            ? make_match_with_stats(env, broker_term, other_exchange_value, our_sojourn_time)
            : make_match(env, broker_term, other_exchange_value)
        );
    }

    //

    waiter_release(local_state, &other_waiter);
    assert(other_waiter == NULL);

    return match_res;
}

//

static ERL_NIF_TERM nif_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[])
{
    tag_t* tag = NULL;
    broker_t* broker = NULL;
    ErlNifPid self;

    ERL_NIF_TERM tag_term = argv[0];

    if (!get_tag(env, tag_term, &tag)) {
        if (enif_is_ref(env, tag_term)) {
            // faux tag
            return Atoms._too_late;
        }
        return make_badarg(env, tag_term);
    }

    int get_broker_res = get_broker(env, tag->broker_term, &broker);
    assert(get_broker_res);

    if (!enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    local_state_t* local_state = broker_local_state(broker);
    waiter_t* cancelled_waiter = broker_cancel_waiter(broker, tag->waiter_id);

    ERL_NIF_TERM cancel_res = Atoms._none;

    if (cancelled_waiter == NULL) {
        cancel_res = Atoms._too_late;
    }
    else if (enif_demonitor_process(env, tag, &tag->mon) == 0) {
        assert(tag->env != NULL);
        env_pool_return(local_state, tag->env);
        tag->env = NULL;

        if (enif_compare_pids(&self, &cancelled_waiter->pid) != 0)
        {
            notify_of_cancellation(env, cancelled_waiter->pid, tag_term);
        }

        waiter_release(local_state, &cancelled_waiter);
        assert(cancelled_waiter == NULL);

        cancel_res = Atoms._cancelled;
    }

    return cancel_res;
}
    
    

/*********************************************************************/

/*********************************************************************/

static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out)
{
    return enif_get_resource(env, term, ResourceTypes.broker, (void**)out);
}

static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, tag_t** out)
{
    return enif_get_resource(env, term, ResourceTypes.tag, (void**)out);
}

// static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out_broker, waiter_id_t* out_waiter_id)
// {
//     ERL_NIF_TERM head, tail;
// 
//     broker_t* broker = NULL;
//     ERL_NIF_TERM side; // discarded
//     waiter_id_t waiter_id = 0;
// 
//     if (enif_get_list_cell(env, term, &head, &tail)
//         && get_broker(env, head, &broker)
//         && get_waiter_id(env, tail, &side, &waiter_id))
//     {
//         *out_broker = broker;
//         *out_waiter_id = waiter_id;
//         return 1;
//     }
//     return 0;
// }
// 
// static int get_waiter_id(ErlNifEnv* env, ERL_NIF_TERM term, ERL_NIF_TERM* out_side, waiter_id_t *out_waiter_id) {
//     int64_t signed_id = 0;
// 
//     if (enif_get_int64(env, term, &signed_id)) {
//         if (signed_id < 0) {
//             assert(signed_id > INT64_MIN);
//             *out_side = Atoms._left;
//             *out_waiter_id = -signed_id;
//         }
//         else {
//             *out_side = Atoms._right;
//             *out_waiter_id = signed_id;
//         }
//         return 1;
//     }
//     return 0;
// }

/*********************************************************************/

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

static void* waiter_pool_alloc_waiter() { 
    return (void*)enif_alloc(sizeof(waiter_t)); 
}

static void waiter_pool_clear_waiter(void* waiter) { 
    LOG("Clear waiter %llu", ((waiter_t*)waiter)->id);
    memset(waiter, 0, sizeof(waiter_t));
}

static void waiter_pool_free_waiter(void* waiter) { 
    LOG("Free waiter %llu", ((waiter_t*)waiter)->id);
    enif_free((ErlNifEnv*)waiter); 
}

static waiter_t* waiter_pool_get(local_state_t* local_state)
{
    waiter_t* waiter = (waiter_t*)mempool_get(&local_state->waiter_pool);
    assert(waiter != NULL);
    return waiter;
}

static void waiter_pool_return(local_state_t* local_state, waiter_t* waiter)
{
    assert(waiter != NULL);
    mempool_return(&local_state->waiter_pool, waiter);
}

/*********************************************************************/

static void mempool_init(mempool_t* pool)
{
    const size_t initial_size = INITIAL_MEMPOOL_SIZE;
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

static ERL_NIF_TERM make_badarg(ErlNifEnv* env, ERL_NIF_TERM term)
{
    ERL_NIF_TERM reason = enif_make_tuple2(env, Atoms._badarg, term);
    return enif_raise_exception(env, reason);
}

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
    broker_t* broker = (broker_t*) obj;
    assert(broker->count == 0);

    enif_mutex_destroy(broker->lock);

    for (thread_id_t thread_id = 0; thread_id < broker->nr_of_schedulers; thread_id++) {
        local_state_t* local_state = &broker->local_states[thread_id];
        mempool_destroy(&local_state->waiter_pool);
        mempool_destroy(&local_state->env_pool);
    }

    memset(broker, 0, sizeof_broker(broker->nr_of_schedulers));
}

//

static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
}

/*********************************************************************/

static void tag_dtor(ErlNifEnv* caller_env, void* obj)
{
    LOG("[tag destroy] %p", obj);
    tag_t* tag = (tag_t*) obj;
    assert(tag->env == NULL);

    memset(tag, 0, sizeof(tag_t));
}

static void tag_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon)
{
    ERL_NIF_TERM pid_term = enif_make_pid(caller_env, pid);
    tag_t* tag = (tag_t*) obj;
    broker_t* broker = NULL;

    LOG("[%T] [tag DOWN] Waiter %llu on broker %T", pid_term, tag->waiter_id,
        enif_make_copy(caller_env, tag->broker_term));

    ErlNifEnv* tag_env = tag->env;
    assert(tag_env != NULL);

    int get_broker_res = get_broker(tag_env, tag->broker_term, &broker);
    assert(get_broker_res);

    local_state_t* local_state = broker_local_state(broker);
    waiter_t* cancelled_waiter = broker_cancel_waiter(broker, tag->waiter_id);

    if (cancelled_waiter != NULL) {
        waiter_release(local_state, &cancelled_waiter);
        assert(cancelled_waiter == NULL);
    }

    tag->env = NULL;
    env_pool_return(local_state, tag_env);
}
