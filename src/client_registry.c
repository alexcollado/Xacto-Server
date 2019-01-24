#include "client_registry.h"
#include "debug.h"
#include "csapp.h"

typedef struct client_registry {
    int client_count;
    int wait_count;
    fd_set client_fds;
    int client_fd_max;
    pthread_mutex_t mutex;
    sem_t wait_sem;
} CLIENT_REGISTRY;

CLIENT_REGISTRY *creg_init() {
    CLIENT_REGISTRY *cr = Malloc(sizeof(CLIENT_REGISTRY));
    cr->client_count = 0;
    cr->wait_count = 0;
    cr->client_fd_max = 1024;

    //  Zero out the fd_set
    fd_set fdSet;
    FD_ZERO(&fdSet);
    cr->client_fds = fdSet;

    //  Initialize mutex
    pthread_mutex_init(&cr->mutex, 0);

    //  Initalize semaphore
    Sem_init(&cr->wait_sem, 0, 0);

    debug("Initialize client registry");
    return cr;
}

void creg_fini(CLIENT_REGISTRY *cr) {
    debug("Finalize client registry");
    Free(cr);
}

void creg_register(CLIENT_REGISTRY *cr, int fd) {
    //  Lock
    pthread_mutex_lock(&cr->mutex);

    //  Increment client count and add fd to fd_set
    cr->client_count++;
    FD_SET(fd, &cr->client_fds);

    //  Unlock
    pthread_mutex_unlock(&cr->mutex);

    debug("Register client %d (Total connected: %d)", fd, cr->client_count);
}

void creg_unregister(CLIENT_REGISTRY *cr, int fd) {
    //  Lock
    pthread_mutex_lock(&cr->mutex);

    //  Decrement client count and clear fd from fd_set
    cr->client_count--;
    FD_CLR(fd, &cr->client_fds);

    //  Unlock
    pthread_mutex_unlock(&(cr->mutex));

    debug("Unregister client %d (Total connected: %d)", fd, cr->client_count);

    //  Unblock when number of clients reaches zero
    if(cr->client_count == 0) V(&cr->wait_sem);
}

void creg_wait_for_empty(CLIENT_REGISTRY *cr) {
    //  Block until number of clients reaches zero
    P(&cr->wait_sem);
}

void creg_shutdown_all(CLIENT_REGISTRY *cr) {
    int i;
    for(i = 0; i < FD_SETSIZE; i++) {
        //  If fd is in fd_set, shutdown the registered client file descriptor
        if(FD_ISSET(i, &cr->client_fds)) {
            shutdown(i, SHUT_RD);
            debug("Shutting down client %d", i);
        }
    }
}