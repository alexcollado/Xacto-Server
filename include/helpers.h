#ifndef __HELPERS_H__
#define __HELPERS_H__

#include "protocol.h"
#include "transaction.h"

void sighupHandler(int sig);

void *thread(void *vargp);

void xacto_get(int connfd, BLOB *bp);

MAP_ENTRY *findMapEntry(KEY *key);

void garbageCollect(MAP_ENTRY *mapEntry);

void addVersion(MAP_ENTRY *mapEntry, TRANSACTION *tp, BLOB *bp, KEY *kp);

void itemShow(MAP_ENTRY* mapEntry, KEY *kp);

#endif