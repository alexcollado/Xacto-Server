#include "store.h"
#include "helpers.h"
#include "debug.h"
#include "csapp.h"

struct map store;

void store_init() {
    //  Initialize the store.
    store.table = Calloc(sizeof(MAP_ENTRY*) * NUM_BUCKETS, 1);
    store.num_buckets = NUM_BUCKETS;

    //  Initialize store mutex.
    pthread_mutex_init(&store.mutex, 0);

    debug("Initialize object store");
}

void store_fini() {
    debug("Finalize object store");

    int i;

    /*  Traverse the store and dispose of the keys, versions,
     *  and map entries. Finally, free the store itself.
     */
    for(i = 0; i < NUM_BUCKETS; i++) {
        if(store.table[i] != NULL) {
            MAP_ENTRY *curMapEntry = store.table[i];

            while(curMapEntry != NULL) {
                MAP_ENTRY *nextMapEntry = curMapEntry->next;
                key_dispose(curMapEntry->key);
                VERSION *curVersion = curMapEntry->versions;

                while(curVersion != NULL) {
                    VERSION *nextVersion = curVersion->next;
                    version_dispose(curVersion);
                    curVersion = nextVersion;
                }

                Free(curMapEntry);
                curMapEntry = nextMapEntry;
            }
        }
    }
    Free(store.table);
}

TRANS_STATUS store_put(TRANSACTION *tp, KEY *key, BLOB *value) {
    debug("Put mapping (key=%p [%s] -> value=%p [%s]) in store for transaction %d", key, key->blob->prefix, value, value->prefix, tp->id);

    //  Find or create the map entry.
    MAP_ENTRY *mapEntry = findMapEntry(key);

    //  Garbage collect the version list.
    garbageCollect(mapEntry);

    //  Attempt to add a new version.
    addVersion(mapEntry, tp, value, mapEntry->key);

    //  Return pending or aborted status.
    return trans_get_status(tp);
}

TRANS_STATUS store_get(TRANSACTION *tp, KEY *key, BLOB **valuep) {
    debug("Get mapping of key=%p [%s] in store for transaction %d", key, key->blob->prefix, tp->id);

    //  Find or create the map entry.
    MAP_ENTRY *mapEntry = findMapEntry(key);

    //  Garbage collect the version list.
    garbageCollect(mapEntry);

    VERSION *cur = mapEntry->versions;

    //  If there is no version, use a null value when adding the version.
    if(cur == NULL) *valuep = NULL;
    //  Else retrieve the latest version's value.
    else {
        while(cur->next != NULL) {
            cur = cur->next;
        }
        *valuep = cur->blob;
        blob_ref(*valuep, NULL);
    }

    //  Attempt to add a new version.
    addVersion(mapEntry, tp, *valuep, NULL);
    if(*valuep != NULL) blob_ref(*valuep, NULL);

    //  Return pending or aborted status.
    return trans_get_status(tp);
}

void store_show() {
    //  Show the contents of the store.
    fprintf(stderr, "CONTENTS OF STORE:\n");
    for (int i = 0; i < NUM_BUCKETS; i++) {
        fprintf(stderr, "%d:", i);
        MAP_ENTRY* cur = store.table[i];

        while (cur != NULL) {
            itemShow(cur, cur->key);
            cur = cur->next;
        }

        fprintf(stderr, "\n" );
    }

}

void itemShow(MAP_ENTRY* mapEntry, KEY *kp) {
    if(mapEntry->versions->blob == NULL) fprintf(stderr, "\t{key: %p [%s], versions: {creator=%d (%d), (NULL blob)}", kp, kp->blob->prefix, mapEntry->versions->creator->id, mapEntry->versions->creator->status);
    else fprintf(stderr, "\t{key: %p [%s], versions: {creator=%d (%d), blob=%p [%s]}", kp, kp->blob->prefix, mapEntry->versions->creator->id, mapEntry->versions->creator->status, mapEntry->versions->blob, mapEntry->versions->blob->prefix);
    VERSION* cur = mapEntry->versions->next;
    while(cur != NULL) {
        if(mapEntry->key->blob == NULL) fprintf(stderr, "{creator=%d (%d), (NULL blob)}", mapEntry->versions->creator->id, mapEntry->versions->creator->status);
        else fprintf(stderr, "{creator=%d (%d), blob=%p [%s]}", cur->creator->id, cur->creator->status, cur->blob, cur->blob->prefix);
        cur = cur->next;
    }
    fprintf(stderr, "}\n");
}

MAP_ENTRY *findMapEntry(KEY *key) {
    int i;
    //  Find an existing map entry. If map entry found, dispose the new key.
    for(i = 0; i < NUM_BUCKETS; i++) {
        if(store.table[i] != NULL) {
            MAP_ENTRY *curMapEntry = store.table[i];

            while(curMapEntry != NULL) {
                MAP_ENTRY *nextMapEntry = curMapEntry->next;
                if(!key_compare(key, curMapEntry->key)) {
                    debug("Matching entry exists, disposing of redundant key %p [%s]", key, key->blob->prefix);
                    key_dispose(key);

                    //  Return the found map entry.
                    return curMapEntry;
                }
                curMapEntry = nextMapEntry;
            }
        }
    }

    //  Map entry not found, so create a new one.
    MAP_ENTRY *newMapEntry = Calloc(sizeof(MAP_ENTRY), 1);
    newMapEntry->key = key;
    newMapEntry->versions = NULL;
    unsigned long bucket = key->hash;
    bucket %= 8;

    fprintf(stderr, "HASH\n%lu\n", bucket);

    //  Store map entry in the store.
    if(store.table[bucket] == NULL) store.table[bucket] = newMapEntry;
    else {
        MAP_ENTRY *curMapEntry = store.table[bucket];

        while(curMapEntry->next != NULL) {
            curMapEntry = curMapEntry->next;
        }
        curMapEntry->next = newMapEntry;
    }

    debug("Create new map entry for key %p [%s] at table index %lu", key, key->blob->prefix, bucket);

    //  Return the new map entry.
    return newMapEntry;
}

void garbageCollect(MAP_ENTRY *mapEntry) {
    //  If there are no versions, there is no garbage collection; return.
    if(mapEntry->versions == NULL) return;

    VERSION *curVersion = mapEntry->versions;
    VERSION *latestCommit, *earliestAbort;
    int flag = 1;

    //  Traverse version list and get the most recent commit and the earliest abort.
    while(curVersion != NULL) {
        TRANS_STATUS status = trans_get_status(curVersion->creator);
        if(status == TRANS_COMMITTED) latestCommit = curVersion;
        else if(status == TRANS_ABORTED && flag) {
            earliestAbort = curVersion;
            flag = 0;
        }
        curVersion = curVersion->next;
    }

    VERSION *prev = mapEntry->versions;
    VERSION *cur = mapEntry->versions->next;
    flag = 1;

    //  Traverse version list and dispose of any earlier commits.
    while(prev != NULL) {
        TRANS_STATUS status = trans_get_status(prev->creator);
        if(cur == NULL && status == TRANS_COMMITTED && flag) flag = 0;
        else if(status == TRANS_COMMITTED && !memcmp(prev, latestCommit, sizeof(VERSION))) version_dispose(prev);
        else if(status == TRANS_COMMITTED && memcmp(cur, latestCommit, sizeof(VERSION))) {
            flag = 0;
            break;
        }
        prev = cur;
        if(cur != NULL) cur = cur->next;
    }
    if(!flag) {
        while(cur != NULL) {
            TRANS_STATUS status = trans_get_status(cur->creator);
            if(status == TRANS_COMMITTED && !memcmp(cur, latestCommit, sizeof(VERSION))) {
                prev->next = cur->next;
                version_dispose(cur);
            }
            else if(status == TRANS_COMMITTED && memcmp(cur, latestCommit, sizeof(VERSION))) break;
            prev = prev->next;
            cur = prev->next;
        }
    }

    /*  Traverse version list and dispose any versions and
     *  abort their creator transactions starting from
     *  the earliest abort.
     */
    prev = mapEntry->versions;
    cur = mapEntry->versions->next;
    while(prev != NULL) {
        TRANS_STATUS status = trans_get_status(prev->creator);
        if(status == TRANS_ABORTED && memcmp(prev, earliestAbort, sizeof(VERSION))) {
            while(prev != NULL) {
                trans_abort(prev->creator);
                debug("test3");
                version_dispose(prev);
                prev = cur;
                if(cur != NULL) cur = cur->next;
            }
            break;
        }
        prev = cur;
        if(cur != NULL) cur = cur->next;
    }
}

void addVersion(MAP_ENTRY *mapEntry, TRANSACTION *tp, BLOB *bp, KEY *kp) {
    VERSION *curVersion = mapEntry->versions;

    /*  Traverse the version list and if a creator ID is less
     *  than the transaction passed in, abort the transaction and return.
     */
    while(curVersion != NULL) {
        if(curVersion->creator->id > tp->id) {
            debug("Current transaction ID (%d) is less than version creator (%d) -- aborting", tp->id, curVersion->creator->id);
            trans_ref(tp, "for reference to current transaction for aborting");
            trans_abort(tp);
            blob_unref(bp, "for aborting due to anachronistic dependency");
            return;
        }
        curVersion = curVersion->next;
    }

    //  Create a new version
    VERSION *version = version_create(tp, bp);

    /*  If there are no versions in the map entry,
     *  make this the head of the list and return.
     */
    curVersion = mapEntry->versions;
    if(curVersion == NULL) {
        debug("No previous version");
        mapEntry->versions = version;
        return;
    }


    VERSION *temp = mapEntry->versions;
    VERSION *prev;

    if (temp != NULL && temp->creator->id == tp->id) {
        mapEntry->versions = version;
        version_dispose(temp);
        return;
    }

    while(temp != NULL && temp->creator->id != tp->id) {
        TRANS_STATUS status = trans_get_status(temp->creator);
        if(status == TRANS_PENDING) trans_add_dependency(tp, temp->creator);
        prev = temp;
        temp = temp->next;
    }

    // Add version to end of version list
    if(temp == NULL) {
        prev->next = version;
        debug("Previous version is %p [%s]", prev, prev->blob->prefix);
    }
    else{
        prev->next = temp->next;
        version_dispose(prev);
    }
}