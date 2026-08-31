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
    X(_todo,                 "todo") \
    X(_too_late,             "too_late") \
    X(_true,                 "true") \
    X(_undefined,            "_undefined")


#define MAX(a, b) ((a) >= (b) ? (a) : (b))
#define MIN(a, b) ((a) <= (b) ? (a) : (b))

#define LOG(fmt, ...) enif_fprintf(stderr, fmt "\n\r", __VA_ARGS__); fflush(stderr)
//#define LOG(fmt, ...)

/*********************************************************************/


/*********************************************************************/

#define X(field, name) ERL_NIF_TERM field;
static struct {
    ATOM_LIST
} Atoms;
#undef X

//

typedef uint_fast64_t position_t;

//

static struct {
    ErlNifResourceType* broker;
    ErlNifResourceType* wmonitor;
} ResourceTypes;

//

typedef struct waiter {
    ErlNifPid pid;
    ErlNifEnv* env;
    ERL_NIF_TERM exchange_value;
    ERL_NIF_TERM wmonitor_term;
} waiter_t;

//

typedef struct {
    ErlNifMonitor mon;
    ErlNifEnv* env;
    ERL_NIF_TERM broker_term;
    position_t position;
} wmonitor_t;

//

typedef struct {
    ErlNifMutex* lock;
    position_t position_counter;
    ERL_NIF_TERM side;
    cbroker_omap_t* queue;
} broker_t;

/*********************************************************************/

static ERL_NIF_TERM make_badarg(ErlNifEnv* env, ERL_NIF_TERM term) {
    ERL_NIF_TERM reason = enif_make_tuple2(env, Atoms._badarg, term);
    return enif_raise_exception(env, reason);
}

static int get_broker(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out) {
    return enif_get_resource(env, term, ResourceTypes.broker, (void**) out);
}

static int get_wmonitor(ErlNifEnv* env, ERL_NIF_TERM term, wmonitor_t** out) {
    return enif_get_resource(env, term, ResourceTypes.wmonitor, (void**) out);
}

static ERL_NIF_TERM make_tag(ErlNifEnv* env, ERL_NIF_TERM broker_term, position_t position) {
    return enif_make_list_cell(env, broker_term, enif_make_uint64(env, position));
}

static int get_tag(ErlNifEnv* env, ERL_NIF_TERM term, broker_t** out_broker, position_t* out_position) {
    ERL_NIF_TERM head, tail;
    broker_t* broker = NULL;
    position_t position = 0;

    if (enif_get_list_cell(env, term, &head, &tail)
        && get_broker(env, head, &broker)
        && enif_get_uint64(env, tail, &position)
    ) {
        *out_broker = broker;
        *out_position = position;
        return 1;
    }
    else {
        return 0;
    }
}

/*********************************************************************/

static waiter_t* waiter_new(ErlNifPid pid, ERL_NIF_TERM exchange_value, wmonitor_t* wmonitor) {
    waiter_t* waiter = enif_alloc(sizeof(waiter_t));
    memset(waiter, 0, sizeof(waiter_t));

    waiter->pid = pid;

    ErlNifEnv* env = enif_alloc_env();
    waiter->env = env;
    waiter->exchange_value = enif_make_copy(env, exchange_value);
    waiter->wmonitor_term = Atoms._undefined;
    waiter->wmonitor_term = enif_make_resource(env, wmonitor);

    return waiter;
}

static wmonitor_t* wmonitor_new(ERL_NIF_TERM broker_term) {
    wmonitor_t* wmonitor = (wmonitor_t*) enif_alloc_resource(ResourceTypes.wmonitor, sizeof(wmonitor_t));
    memset(wmonitor, 0, sizeof(wmonitor_t));

    ErlNifEnv* env = enif_alloc_env();
    wmonitor->env = env;
    wmonitor->broker_term = enif_make_copy(env, broker_term);

    return wmonitor;
}

/*********************************************************************/

static ERL_NIF_TERM
niff_new(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    ErlNifPid self;
    if (! enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    broker_t* broker = enif_alloc_resource(ResourceTypes.broker, sizeof(broker_t));
    memset(broker, 0, sizeof(broker_t));

    broker->lock = enif_mutex_create("cbroker2");
    broker->side = Atoms._empty;
    broker->queue = cbroker_omap_new();

    return enif_make_resource(env, broker);
}

//

static ERL_NIF_TERM
niff_ask(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    broker_t* broker = NULL;
    ErlNifPid self;

    ERL_NIF_TERM broker_term = argv[0];
    ERL_NIF_TERM side = argv[1];
    ERL_NIF_TERM exchange_value = argv[2];

    if (! get_broker(env, broker_term, &broker)) {
        return make_badarg(env, broker_term);
    }

    if (! enif_self(env, &self)) {
        return enif_make_badarg(env);
    }
        
    if (side != Atoms._left && side != Atoms._right) {
        return make_badarg(env, side);
    }
   
    //LOG("[ask] Got args, estimated size %llu", cbroker_omap_size(broker->queue));

    ////////////////////////////

    wmonitor_t* new_monitor = NULL;
    waiter_t* new_waiter = NULL;

    ERL_NIF_TERM estimated_side = broker->side;

    bool expecting_enqueue = (
        estimated_side == Atoms._empty
        || (side != estimated_side)
    );

    if (expecting_enqueue) {
        //LOG("[ask] Preemptively allocating new waiter %s", "");
        new_monitor = wmonitor_new(broker_term);
        new_waiter = waiter_new(self, exchange_value, new_monitor);
    }

    ////////////////////////////

    //LOG("[ask] About to lock", "");
    enif_mutex_lock(broker->lock);

    bool is_empty = (broker->side == Atoms._empty);
    bool should_enqueue = (side == broker->side) || is_empty;

    if (should_enqueue) {
        //LOG("[ask] Enqueueing on side %T", side);

        if (new_waiter == NULL) {
            //LOG("[ask] Allocating new waiter on-demand%s", "");
            new_monitor = wmonitor_new(broker_term);
            new_waiter = waiter_new(self, exchange_value, new_monitor);
        }

        position_t position = broker->position_counter++;
        new_monitor->position = position;
        //LOG("[ask] Position is %llu", position);

        enif_monitor_process(env, new_monitor, &self, &new_monitor->mon);
        //LOG("[ask] Monitor created %s", "");

        cbroker_omap_result_t insert_res = cbroker_omap_insert(broker->queue, position, new_waiter);
        assert(insert_res == CBROKER_OMAP_OK);
        //LOG("[ask] Entry inserted %s", "");

        if (is_empty) {
            broker->side = side;
        }

        enif_mutex_unlock(broker->lock);

        ERL_NIF_TERM tag = make_tag(env, broker_term, position);
        return enif_make_tuple2(env, Atoms._await, tag);
    }
    else {
        //LOG("[ask] Dequeueing on side %T", side);
        assert(cbroker_omap_size(broker->queue) > 0);

        position_t head_pos = 0;
        waiter_t* head = NULL;
        bool first_res = cbroker_omap_first(broker->queue, &head_pos, (void**) &head);
        assert(first_res);
        //LOG("[ask] Head position is %llu", head_pos);

        bool has_next = true;
        bool delete_res = cbroker_omap_delete_and_next(broker->queue, head_pos, &has_next, NULL, NULL);
        assert(delete_res);
        //LOG("[ask] Head deleted from omap %s", "");

        if (! has_next) {
            //LOG("[ask] Queue is once again empty", "");
            broker->side = Atoms._empty;
        }

        enif_mutex_unlock(broker->lock);

        //LOG("[ask] Retrieving head monitor resource", "");
        wmonitor_t* head_monitor = NULL;
        int get_monitor_res = get_wmonitor(env, head->wmonitor_term, &head_monitor);
        assert(get_monitor_res);

        ERL_NIF_TERM ask_res;

        if (enif_demonitor_process(env, head_monitor, &head_monitor->mon) != 0) {
            //LOG("[ask] Too late to match", "");
            // too late to match
            ask_res = Atoms._retry;
        }
        else {
            //LOG("[ask] Head demonitored", "");
            enif_free_env(head_monitor->env);
            head_monitor->env = NULL;

            //LOG("[ask] Head exchange value about to be copied", "");
            ERL_NIF_TERM head_exchange_value = enif_make_copy(env, head->exchange_value);
            ask_res = enif_make_tuple2(env, Atoms._match, head_exchange_value);

            if (new_waiter == NULL) {
                //LOG("[ask] Sending match to waiter - no env", "");
                ERL_NIF_TERM match_msg_tag = make_tag(env, broker_term, head_pos);
                ERL_NIF_TERM match_msg = enif_make_tuple2(env, match_msg_tag, enif_make_tuple2(env, Atoms._match, exchange_value));
                enif_send(env, &head->pid, NULL, match_msg);
            }
            else {
                //LOG("[ask] Sending match to waiter - env reused", "");
                ErlNifEnv* new_waiter_env = new_waiter->env;
                ERL_NIF_TERM match_msg_tag = make_tag(new_waiter_env, broker_term, head_pos);
                ERL_NIF_TERM match_msg = enif_make_tuple2(new_waiter_env, match_msg_tag, enif_make_tuple2(new_waiter_env, Atoms._match, exchange_value));
                enif_send(env, &head->pid, new_waiter_env, match_msg);
            }

        }

        //LOG("[ask] Freeing head env", "");
        enif_free_env(head->env);

        //LOG("[ask] Freeing head", "");
        enif_free(head);

        //

        if (new_waiter != NULL) {
            //LOG("[ask] Freeing env of new waiter prematurely allocated", "");
            enif_free_env(new_monitor->env);
            new_monitor->env = NULL;

            // TODO demonitor!

            //LOG("[ask] Freeing env of new monitor prematurely allocated", "");
            enif_free_env(new_waiter->env);
            //LOG("[ask] Freeing new waiter prematurely allocated", "");
            enif_free(new_waiter);
        }

        //

        return ask_res;
    }
}

//

static ERL_NIF_TERM
niff_cancel(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    broker_t* broker = NULL;
    position_t position = 0;
    ErlNifPid self;

    if (! get_tag(env, argv[0], &broker, &position)) {
        return make_badarg(env, argv[0]);
    }

    LOG("cancel: get self %s", "");
    if (! enif_self(env, &self)) {
        return enif_make_badarg(env);
    }

    enif_mutex_lock(broker->lock);

    waiter_t* waiter = NULL;

    if (cbroker_omap_lookup(broker->queue, position, (void**) &waiter)) {
        cbroker_omap_delete_and_next(broker->queue, position, NULL, NULL, NULL);

        if (cbroker_omap_size(broker->queue) == 0) {
            broker->side = Atoms._empty;
        }

        enif_mutex_unlock(broker->lock);

        wmonitor_t* monitor = NULL;

        int get_monitor_res = get_wmonitor(env, waiter->wmonitor_term, &monitor);
        assert(get_monitor_res);

        if (enif_demonitor_process(env, monitor, &monitor->mon) == 0) {
            enif_free_env(monitor->env);
            monitor->env = NULL;
        }

        enif_free_env(waiter->env);
        enif_free(waiter);

        return Atoms._cancelled;
    }
    else {
        enif_mutex_unlock(broker->lock);
        return Atoms._too_late;
    }
}

//

static ERL_NIF_TERM
niff_to_list(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    broker_t* broker = NULL;

    if (! get_broker(env, argv[0], &broker)) {
        return make_badarg(env, argv[0]);
    }

    return Atoms._todo;
}

//

static ErlNifFunc nif_funcs[] = {
    {"new", 0, niff_new},
    {"ask", 3, niff_ask},
    {"cancel", 1, niff_cancel},
    {"to_list", 1, niff_to_list}
};

/*********************************************************************/

static void omap_free_waiter(uint64_t key, void* value, void* ctx) {
    assert(0);
}

/*********************************************************************/

static void broker_stop(broker_t* broker) {
    // TODO?
}

static void broker_dtor(ErlNifEnv* caller_env, void* obj) {
    broker_t* broker = (broker_t*) obj;
    broker_stop(broker);

    assert(broker->side == Atoms._empty);
    assert(cbroker_omap_size(broker->queue) == 0);

    cbroker_omap_destroy(broker->queue, omap_free_waiter, NULL);
    broker->queue = NULL;
}

static void broker_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon) {
    broker_t* broker = (broker_t*) obj;
    broker_stop(broker);

//void cbroker_omap_destroy(cbroker_omap_t* map,
//                          void (*free_value)(uint64_t key, void* value, void* ctx),
//                          void* ctx);

}

/*********************************************************************/

static void wmonitor_dtor(ErlNifEnv* caller_env, void* obj) {
    wmonitor_t* wmonitor = (wmonitor_t*) obj;

    if (wmonitor->env != NULL) {
        enif_free_env(wmonitor->env);
    }
}

static void wmonitor_down(ErlNifEnv* caller_env, void* obj, ErlNifPid* pid, ErlNifMonitor* mon) {
    LOG("wmonitor down!!", "");
    wmonitor_t* monitor = (wmonitor_t*) obj;
    broker_t* broker = NULL;

    int get_broker_res = get_broker(caller_env, monitor->broker_term, &broker);
    assert(get_broker_res);

    enif_mutex_lock(broker->lock);

    position_t position = monitor->position;
    waiter_t* waiter = NULL;

    if (cbroker_omap_lookup(broker->queue, position, (void**) &waiter)) {
        cbroker_omap_delete_and_next(broker->queue, position, NULL, NULL, NULL);

        if (cbroker_omap_size(broker->queue) == 0) {
            broker->side = Atoms._empty;
        }

        enif_mutex_unlock(broker->lock);

        assert(monitor->env != NULL);
        enif_free(monitor->env);
        monitor->env = NULL;

        enif_free(waiter->env);
        enif_free(waiter);
    }

    enif_mutex_unlock(broker->lock);
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

static void wmonitor_resource_load(ErlNifEnv* caller_env) {
    ErlNifResourceTypeInit callbacks = {wmonitor_dtor, NULL, wmonitor_down, 3, NULL};
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE;

    ResourceTypes.wmonitor = enif_init_resource_type(
        caller_env, 
        "cbroker.wmonitor",
        &callbacks,
        flags,
        &flags
    );
    assert(ResourceTypes.wmonitor != NULL);
}

static int nif_load(ErlNifEnv* caller_env, void** priv_data, ERL_NIF_TERM load_info) {
    init_atoms(caller_env);

    memset(&ResourceTypes, 0, sizeof(ResourceTypes));
    broker_resource_load(caller_env);
    wmonitor_resource_load(caller_env);

    return 0;
}

ERL_NIF_INIT(cbroker_nif2, nif_funcs, nif_load, NULL, NULL, NULL);
