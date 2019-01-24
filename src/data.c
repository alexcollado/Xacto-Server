#include "data.h"
#include "debug.h"
#include "csapp.h"
#include "string.h"

BLOB *blob_create(char *content, size_t size) {
    if(content == NULL) return NULL;

    BLOB *bp = Malloc(sizeof(BLOB));
    bp->refcnt = 0;
    bp->size = size;
    bp->content = Malloc(size);
    memcpy(bp->content, content, size);
    bp->prefix = strndup(content, size);

    //  Initialize mutex.
    pthread_mutex_init(&bp->mutex, 0);

    debug("Create blob with content %p, size %lu -> %p", content, size, bp);

    //  Increase ref count by 1.
    blob_ref(bp, "for newly created blob");

    return bp;
}

BLOB *blob_ref(BLOB *bp, char *why) {
    if(bp == NULL) return NULL;
    //  Lock.
    pthread_mutex_lock(&bp->mutex);

    //  Increase ref count.
    bp->refcnt++;

    //  Unlock.
    pthread_mutex_unlock(&bp->mutex);

    debug("Increase reference count on blob %p [%p] (%d -> %d) %s", bp, bp->prefix, bp->refcnt - 1, bp->refcnt, why);

    return bp;
}

void blob_unref(BLOB *bp, char *why) {
    if(bp == NULL) return;
    //  Lock.
    pthread_mutex_lock(&bp->mutex);

    //  Decrease ref count.
    bp->refcnt--;

    //  Unlock.
    pthread_mutex_unlock(&bp->mutex);

    debug("Decrease reference count on blob %p [%p] (%d -> %d) %s", bp, bp->prefix, bp->refcnt + 1, bp->refcnt, why);

    //  If ref count == 0, free blob content and blob.
    if(bp->refcnt == 0) {
        debug("Free blob %p [%s]", bp, bp->prefix);
        Free(bp->content);
        Free(bp->prefix);
        Free(bp);
    }
}

int blob_compare(BLOB *bp1, BLOB *bp2) {
    //  If bp1 size == bp2 size, return result of memcmp, else return false.
    if(bp1->size == bp2->size) return memcmp(bp1->content, bp2->content, bp1->size);

    return 1;
}

int blob_hash(BLOB *bp) {
    if(bp == NULL) return -1;
    int bpHash = 6823, temp;

    //  Assign another pointer so that blob content isn't being incremented.
    char *copy = strndup(bp->content, bp->size);
    char *orig = copy;

    while((temp = *copy++)) bpHash = (bpHash + (bpHash << 5)) + temp;

    Free(orig);

    return bpHash;
}

KEY *key_create(BLOB *bp) {
    KEY *kp = Malloc(sizeof(KEY));

    //  Hash the blob content.
    kp->hash = blob_hash(bp);

    //  Key inherits reference to blob.
    kp->blob = bp;

    debug("Create key from blob %p -> %p [%p]", bp, kp, bp->prefix);
    return kp;
}

void key_dispose(KEY *kp) {
    debug("Dispose of key %p [%p]", kp, kp->blob->prefix);

    //  Decrement blob ref count.
    blob_unref(kp->blob, "for blob in key");

    Free(kp);
}

int key_compare(KEY *kp1, KEY *kp2) {
    //  If hashes are equal, content must be equal so return true, else return false.
    if(kp1->hash == kp2->hash) {
        return(blob_compare(kp1->blob, kp2->blob));
    }

    return 1;
}

VERSION *version_create(TRANSACTION *tp, BLOB *bp) {
    VERSION *vp = Malloc(sizeof(VERSION));

    if(tp == NULL) tp = trans_create();

    //  Version inherits reference to transaction.
    vp->creator = tp;

    //  Version inherits reference to blob.
    vp->blob = bp;

    vp->next = NULL;
    vp->prev = NULL;

    //  Increment transaction ref count.
    trans_ref(tp, "as creator of version");

    if(bp == NULL) debug("Create NULL version for transaction %d -> %p", tp->id, tp);
    else debug("Create version of blob %p [%p] for transaction %d -> %p", bp, bp->prefix, tp->id, tp);

    return vp;
}

void version_dispose(VERSION *vp) {
    debug("Dispose of version %p", vp);

    //  Decrement transaction ref count.
    trans_unref(vp->creator, "as creator of version");

    //  Decrement transaction ref count.
    blob_unref(vp->blob, "for blob in version");

    Free(vp);
}