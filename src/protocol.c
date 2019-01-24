#include "protocol.h"
#include "debug.h"
#include "csapp.h"

int proto_send_packet(int fd, XACTO_PACKET *pkt, void *data) {
    //  Convert multi-byte fields of packet to network byte order.
    uint32_t size = pkt->size;
    pkt->size = htonl(pkt->size);
    pkt->timestamp_sec = htonl(pkt->timestamp_sec);
    pkt->timestamp_nsec = htonl(pkt->timestamp_nsec);

    //  Write the header to the wire.
    if(rio_writen(fd, pkt, sizeof(XACTO_PACKET)) == -1) return -1;

    /*  If header specifies a non-zero payload length,
     *  write payload data to the wire.
     */
    if(pkt->size != 0)
        if(rio_writen(fd, data, size) == -1) return -1;

    return 0;
}

int proto_recv_packet(int fd, XACTO_PACKET *pkt, void **datap) {
    //  Read packet header from wire.
    ssize_t rdres = rio_readn(fd, pkt, sizeof(XACTO_PACKET));
    if(rdres == -1 || rdres == 0) {
        debug("EOF on fd %d", fd);
        return -1;
    }

    //  Convert multi-byte fields of packet to host byte order.
    pkt->size = ntohl(pkt->size);
    pkt->timestamp_sec = ntohl(pkt->timestamp_sec);
    pkt->timestamp_nsec = ntohl(pkt->timestamp_nsec);

    /*  If header specifies a non-zero payload length,
     *  read payload data from the wire.
     */
    if(pkt->size != 0) {
        *datap = Malloc(pkt->size);
        rdres = rio_readn(fd, *datap, pkt->size);
        if(rdres == -1 || rdres == 0) {
            debug("EOF on fd %d", fd);
            return -1;
        }
    }

    return 0;
}