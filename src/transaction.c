#include "transaction.h"
#include "debug.h"
#include "csapp.h"

int trans_ID = 0;

void trans_init() {
    // Initialize sentinel to point to itself
    trans_list.next = &trans_list;
    trans_list.prev = &trans_list;
    debug("Initialize transaction manager");
}

void trans_fini() {
    // Finalize sentinel to point to itself
    trans_list.next = &trans_list;
    trans_list.prev = &trans_list;
    debug("Finalize transaction manager");
}

TRANSACTION *trans_create() {
    TRANSACTION *tp = Malloc(sizeof(TRANSACTION));
    tp->id = trans_ID++;
    tp->refcnt = 0;
    tp->status = TRANS_PENDING;
    tp->depends = NULL;
    tp->waitcnt = 0;

    // Initalize semaphore
    Sem_init(&tp->sem, 0, 0);

    // Initialize mutex
    pthread_mutex_init(&tp->mutex, 0);

    // Traverse transaction list and insert new transaction
    TRANSACTION *last = trans_list.prev;
    tp->next = &trans_list;
    trans_list.prev = tp;
    tp->prev = last;
    last->next = tp;

    debug("Create new transaction %d", tp->id);

    //  Increment ref count by 1.
    trans_ref(tp, "for newly created transaction");

    return tp;
}

TRANSACTION *trans_ref(TRANSACTION *tp, char *why) {
    if(tp == NULL) return NULL;
    // Lock
    pthread_mutex_lock(&tp->mutex);

    // Increase ref count
    tp->refcnt++;

    // Unlock
    pthread_mutex_unlock(&tp->mutex);

    debug("Increase ref count on transaction %d (%d -> %d) %s", tp->id, tp->refcnt - 1, tp->refcnt, why);

    return tp;
}

void trans_unref(TRANSACTION *tp, char *why) {
    if(tp == NULL) return;
    // Lock
    pthread_mutex_lock(&tp->mutex);

    // Decrease ref count
    tp->refcnt--;

    // Unlock
    pthread_mutex_unlock(&tp->mutex);

    debug("Decrease ref count on transaction %d (%d -> %d) %s", tp->id, tp->refcnt + 1, tp->refcnt, why);

    int val;
    pthread_mutex_lock(&tp->mutex);

    // Obtain ref count.
    val = tp->refcnt;

    // Unlock
    pthread_mutex_unlock(&tp->mutex);

    /*  If ref count == 0, decrease the reference count of all of the transactions
     *  in the dependency set and free each dependency in the set. Then free the
     *  transaction.
     */
    if(val == 0) {
        debug("Free transaction %d", tp->id);
        DEPENDENCY *dcur = tp->depends;
        while(dcur != NULL) {
            DEPENDENCY *next = dcur->next;
            trans_unref(dcur->trans, "as transaction in dependency");
            Free(dcur);
            dcur = next;
        }

        TRANSACTION *cur = trans_list.next;
        while(cur != &trans_list) {
            if(cur == tp) {
                TRANSACTION *next = cur->next;
                TRANSACTION *prev = cur->prev;
                prev->next = next;
                next->prev = prev;
                break;
            }
            cur = cur->next;
        }

        Free(tp);
    }
}

void trans_add_dependency(TRANSACTION *tp, TRANSACTION *dtp) {
    //  If the transaction doesn't have a dependency, create a head.
    if(tp->depends == NULL) {
        DEPENDENCY *head = Malloc(sizeof(DEPENDENCY));
        head->trans = dtp;
        head->next = NULL;

        tp->depends = head;

        debug("Make transaction %d dependent on transaction %d", tp->id, dtp->id);
        trans_ref(dtp, "for transaction in dependency");
    }
    //  Else, append the dependency to the end of the dependency list.
    else {
        DEPENDENCY *newHead = Malloc(sizeof(DEPENDENCY));
        newHead->trans = dtp;
        newHead->next = tp->depends;
        tp->depends = newHead;

        debug("Make transaction %d dependent on transaction %d", tp->id, dtp->id);
        trans_ref(dtp, "for transaction in dependency");
    }
}

TRANS_STATUS trans_commit(TRANSACTION *tp) {
    debug("Transaction %d trying to commit", tp->id);

    DEPENDENCY *cur = tp->depends;

    /*  Increment the wait count of all transactions in the dependency set,
     *  and call P on each transactions semaphore to wait for those transactions
     *  to complete.
     */
    while(cur != NULL) {
        if(trans_get_status(cur->trans) != TRANS_PENDING) {
            cur = cur->next;
            continue;
        }
        pthread_mutex_lock(&cur->trans->mutex);
        cur->trans->waitcnt++;
        pthread_mutex_unlock(&cur->trans->mutex);
        P(&cur->trans->sem);
        cur = cur->next;
    }

    //  If any transaction in the dependency set aborted, abort the transaction and return.
    cur = tp->depends;
    while(cur != NULL) {
        if(trans_get_status(cur->trans) == TRANS_ABORTED) {
            return trans_abort(tp);
        }
        cur = cur->next;
    }

    //  Lock.
    pthread_mutex_lock(&tp->mutex);

    //  Change transaction status to committed.
    tp->status = TRANS_COMMITTED;

    //  Unlock.
    pthread_mutex_unlock(&tp->mutex);

    int i;

    //  Lock.
    pthread_mutex_lock(&tp->mutex);

    //  Obtain transaction's wait count.
    int cnt = tp->waitcnt;

    //  Unlock.
    pthread_mutex_unlock(&tp->mutex);

    //  V the transaction's wait count number of times.
    for(i = 0; i < cnt; i++) {
        V(&tp->sem);
    }

    debug("Transaction %d commits", tp->id);

    //  Decrease the transaction's ref count by 1.
    trans_unref(tp, NULL);

    return TRANS_COMMITTED;
}

TRANS_STATUS trans_abort(TRANSACTION *tp) {
    debug("Try to abort transaction %d", tp->id);

    //  If the transaction already commit, abort the program.
    if(trans_get_status(tp) == TRANS_COMMITTED) {
        abort();
    }
    /*  Else if the transaction is already aborted, V transaction's
     *  wait count number of times, decrease the transaction's
     *  ref count by 1, and return the aborted status.
     */
    else if(trans_get_status(tp) == TRANS_ABORTED) {
        debug("Transaction %d has already aborted", tp->id);

        pthread_mutex_lock(&tp->mutex);
        int cnt = tp->waitcnt;
        pthread_mutex_unlock(&tp->mutex);
        int i;
        for(i = 0; i < cnt; i++) {
            V(&tp->sem);
        }

        trans_unref(tp, "for aborting transaction");
        return TRANS_ABORTED;
    }
    /*  Else the transaction is pending. Set the status to
     *  aborted and V transaction's wait count number of times,
     *  decrease the transaction's ref count by 1, and return the
     *  aborted status.
     */
    else {
        pthread_mutex_lock(&tp->mutex);

        tp->status = TRANS_ABORTED;

        pthread_mutex_unlock(&tp->mutex);

        pthread_mutex_lock(&tp->mutex);
        int cnt = tp->waitcnt;
        pthread_mutex_unlock(&tp->mutex);
        int i;
        for(i = 0; i < cnt; i++) {
            V(&tp->sem);
        }


        debug("Transaction %d has aborted", tp->id);
        trans_unref(tp, "for aborting transaction");

        return TRANS_ABORTED;
    }
}

TRANS_STATUS trans_get_status(TRANSACTION *tp) {
    //  Lock.
    pthread_mutex_lock(&tp->mutex);

    //  Obtain transaction status.
    TRANS_STATUS status = tp->status;

    //  Unlock.
    pthread_mutex_unlock(&tp->mutex);

    //  Return status.
    return status;
}

void trans_show(TRANSACTION *tp) {
    fprintf(stderr, "[id=%d, status=%d, refcnt=%d]", tp->id, tp->status, tp->refcnt);
}

void trans_show_all() {
    //  Print all of the transactions.
    fprintf(stderr, "TRANSACTIONS:\n");
    TRANSACTION *cur = trans_list.next;
    while(cur != &trans_list) {
        trans_show(cur);
        cur = cur->next;
    }
    fprintf(stderr, "\n");
}