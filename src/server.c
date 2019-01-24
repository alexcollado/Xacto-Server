#include <inttypes.h>
#include "server.h"
#include "transaction.h"
#include "protocol.h"
#include "data.h"
#include "store.h"
#include "helpers.h"
#include "debug.h"
#include "csapp.h"

CLIENT_REGISTRY *client_registry;

void *xacto_client_service(void *arg) {
    //  Retrieve file descriptor.
    int connfd = *((int *) arg);

    debug("[%d] Starting client service", connfd);

    //  Detach thread so it doesn't have to be explicitly reaped.
    Pthread_detach(pthread_self());

    //  Free storage occupied.
    Free(arg);

    //  Register client file descriptor with client registry.
    creg_register(client_registry, connfd);

    //  Create transaction to carry out requests.
    TRANSACTION *tp = trans_create();

    TRANS_STATUS status = TRANS_PENDING;

    //  Enter service loop.
    while(1) {
        //  Allocate memory for reply packet.
        XACTO_PACKET *pkt = Calloc(sizeof(XACTO_PACKET), 1);
        void **datap = Malloc(sizeof(void**));

        //  Receive reply packet.
        proto_recv_packet(connfd, pkt, datap);

        //  PUT command received.
        if(pkt->type == XACTO_PUT_PKT) {

            debug("[%d] PUT packet received", connfd);

            // Allocate space for data packets.
            XACTO_PACKET *data_pkt1 = Calloc(sizeof(XACTO_PACKET), 1);
            XACTO_PACKET *data_pkt2 = Calloc(sizeof(XACTO_PACKET), 1);
            void **datap1 = Calloc(sizeof(void**), 1);
            void **datap2 = Calloc(sizeof(void**), 1);

            //  Receive first data packet.
            proto_recv_packet(connfd, data_pkt1, datap1);
            debug("[%d] Received key, size %" PRIu32, connfd, data_pkt1->size);

            //  Receive second data packet.
            proto_recv_packet(connfd, data_pkt2, datap2);
            debug("[%d] Received value, size %" PRIu32, connfd, data_pkt2->size);

            //  Create key and value from data packets.
            BLOB *bp1 = blob_create(*datap1, data_pkt1->size);
            KEY *kp = key_create(bp1);
            BLOB *bp2 = blob_create(*datap2, data_pkt2->size);

            //  Put key and value in the store.
            status = store_put(tp, kp, bp2);

            //  Prepare to send the reply packet.
            XACTO_PACKET *reply_pkt = Calloc(sizeof(XACTO_PACKET), 1);
            reply_pkt->type = XACTO_REPLY_PKT;
            if(trans_get_status(tp) == TRANS_ABORTED) reply_pkt->status = 2;
            else reply_pkt->status = 0;
            reply_pkt->null = 0;
            reply_pkt->size = 0;
            struct timespec t;
            clock_gettime(CLOCK_MONOTONIC, &t);
            reply_pkt->timestamp_sec = t.tv_sec;
            reply_pkt->timestamp_nsec = t.tv_nsec;

            //  Send the reply packet.
            proto_send_packet(connfd, reply_pkt, NULL);

            //  Free packet and data pointers.
            Free(data_pkt1);
            Free(data_pkt2);
            Free(*datap1);
            Free(*datap2);
            Free(datap1);
            Free(datap2);
            Free(reply_pkt);

            //  Show store contents and transactions.
            store_show();
            trans_show_all();

            /*  If store put returned an aborted status,
             *  free the packet and data pointers,
             *  abort the transaction, and break out of the
             *  service loop.
             */
            if(status == TRANS_ABORTED) {
                Free(pkt);
                Free(datap);
                trans_abort(tp);
                break;
            }
        }
        //  GET command received.
        else if(pkt->type == XACTO_GET_PKT) {
            debug("[%d] GET packet received", connfd);

            //  Allocate space for data packet.
            XACTO_PACKET *data_pkt1 = Calloc(sizeof(XACTO_PACKET), 1);
            void **datap1 = Calloc(sizeof(void**), 1);

            //  Receive the data packet.
            proto_recv_packet(connfd, data_pkt1, datap1);
            debug("[%d] Received key, size %" PRIu32, connfd, data_pkt1->size);

            //  Create key and value from data packet.
            BLOB *bp = blob_create(*datap1, data_pkt1->size);
            KEY *kp = key_create(bp);

            BLOB **buf = Malloc(sizeof(BLOB**));
            BLOB *bp_reply = NULL;

            //  Get the value associated with the key from the store.
            status = store_get(tp, kp, buf);

            //  If a value was found, send a reply and data packet with the found value.
            if(buf != NULL && *buf != NULL) {
                bp_reply = *buf;

                //  Prepare to send the reply packet.
                XACTO_PACKET *reply_pkt = Calloc(sizeof(XACTO_PACKET), 1);
                reply_pkt->type = XACTO_REPLY_PKT;
                if(trans_get_status(tp) == TRANS_ABORTED) reply_pkt->status = 2;
                else reply_pkt->status = 0;
                reply_pkt->size = 0;
                reply_pkt->null = 0;
                struct timespec t1;
                clock_gettime(CLOCK_MONOTONIC, &t1);
                reply_pkt->timestamp_sec = t1.tv_sec;
                reply_pkt->timestamp_nsec = t1.tv_nsec;

                //  Send the reply packet.
                proto_send_packet(connfd, reply_pkt, NULL);

                //  Unreference the blob obtained from store_get.
                xacto_get(connfd, bp_reply);

                /*  If store put returned an aborted status,
                 *  free the packet and data pointers,
                 *  abort the transaction, and break out of the
                 *  service loop.
                 */
                if(status == TRANS_ABORTED) {
                    Free(reply_pkt);
                    Free(buf);
                    Free(data_pkt1);
                    Free(*datap1);
                    Free(datap1);
                    Free(pkt);
                    Free(datap);
                    trans_abort(tp);
                    break;
                }

                //  Prepare to send the data packet.
                XACTO_PACKET *data_pkt2 = Calloc(sizeof(XACTO_PACKET), 1);
                data_pkt2->type = XACTO_DATA_PKT;
                data_pkt2->status = 0;
                data_pkt2->size = bp_reply->size;
                data_pkt2->null = 0;
                struct timespec t2;
                clock_gettime(CLOCK_MONOTONIC, &t2);
                data_pkt2->timestamp_sec = t2.tv_sec;
                data_pkt2->timestamp_nsec = t2.tv_nsec;

                //  Send the data packet.
                proto_send_packet(connfd, data_pkt2, bp_reply->content);

                //  Free packet and data pointers.
                Free(data_pkt1);
                Free(data_pkt2);
                Free(*datap1);
                Free(datap1);
                Free(reply_pkt);
                Free(buf);
            }
            // Else send a reply and data packet with a null value
            else {
                //  Prepare to send the reply packet.
                XACTO_PACKET *reply_pkt = Calloc(sizeof(XACTO_PACKET), 1);
                reply_pkt->type = XACTO_REPLY_PKT;
                if(trans_get_status(tp) == TRANS_ABORTED) reply_pkt->status = 2;
                else reply_pkt->status = 0;
                reply_pkt->size = 0;
                reply_pkt->null = 0;
                struct timespec t1;
                clock_gettime(CLOCK_MONOTONIC, &t1);
                reply_pkt->timestamp_sec = t1.tv_sec;
                reply_pkt->timestamp_nsec = t1.tv_nsec;

                //  Send the reply packet.
                proto_send_packet(connfd, reply_pkt, NULL);

                //  Unreference the blob obtained from store_get.
                xacto_get(connfd, bp_reply);

                /*  If store put returned an aborted status,
                 *  free the packet and data pointers,
                 *  abort the transaction, and break out of the
                 *  service loop.
                 */
                if(status == TRANS_ABORTED) {
                    Free(reply_pkt);
                    Free(buf);
                    Free(data_pkt1);
                    Free(*datap1);
                    Free(datap1);
                    Free(pkt);
                    Free(datap);
                    trans_abort(tp);
                    break;
                }

                //  Prepare to send the data packet.
                XACTO_PACKET *data_pkt2 = Calloc(sizeof(XACTO_PACKET), 1);
                data_pkt2->type = XACTO_DATA_PKT;
                data_pkt2->status = 0;
                data_pkt2->size = 0;
                data_pkt2->null = 1;
                struct timespec t2;
                clock_gettime(CLOCK_MONOTONIC, &t2);
                data_pkt2->timestamp_sec = t2.tv_sec;
                data_pkt2->timestamp_nsec = t2.tv_nsec;

                //  Send the data packet.
                proto_send_packet(connfd, data_pkt2, NULL);

                //  Free packet and data pointers.
                Free(data_pkt1);
                Free(data_pkt2);
                Free(*datap1);
                Free(datap1);
                Free(reply_pkt);
                Free(buf);
            }

            //  Show the contents of the store and the transactions.
            store_show();
            trans_show_all();
        }
        //  COMMIT command received.
        else if(pkt->type == XACTO_COMMIT_PKT) {
            debug("[%d] COMMIT packet received", connfd);

            //  Commit the transaction
            status = trans_commit(tp);

            int statusn;
            if(status == TRANS_ABORTED) {
                statusn = 2;
            }
            else if(status == TRANS_COMMITTED) {
                statusn = 1;
            }

            //  Prepare to send the reply packet.
            XACTO_PACKET *reply_pkt = Calloc(sizeof(XACTO_PACKET), 1);
            reply_pkt->type = XACTO_REPLY_PKT;
            reply_pkt->status = statusn;
            reply_pkt->size = 0;
            reply_pkt->null = 0;
            struct timespec t1;
            clock_gettime(CLOCK_MONOTONIC, &t1);
            reply_pkt->timestamp_sec = t1.tv_sec;
            reply_pkt->timestamp_nsec = t1.tv_nsec;

            //  Send the reply packet.
            proto_send_packet(connfd, reply_pkt, NULL);

            //  Free packet and data pointers.
            Free(reply_pkt);
            Free(pkt);
            Free(datap);

            //  Show the contents of the store and the transactions, then break out of the service loop.
            store_show();
            trans_show_all();
            break;
        }
        else {
            //  Free packet and data pointers and break out of the service loop if an unknown command was receieved.
            Free(pkt);
            Free(datap);
            break;
        }
        //  Free packet and data pointers and continue the service loop.
        Free(pkt);
        Free(datap);
    }

    debug("[%d] Ending client service", connfd);

    //  If the transaction is still pending, abort it.
    if(status == TRANS_PENDING) trans_abort(tp);

    //  Unregister the client file descriptor.
    creg_unregister(client_registry, connfd);

    //  Close the client connection.
    Close(connfd);
    return NULL;
}

void xacto_get(int connfd, BLOB *bp) {
    if(bp == NULL) debug("[%d] Value is NULL", connfd);
    else debug("[%d] Value is %s", connfd, bp->prefix);
    blob_unref(bp, "obtained from store_get");
}