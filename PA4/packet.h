#ifndef PACKET_H
#define PACKET_H

#include <stdint.h> 

typedef enum {
    DATA,
    CTRL,
    ACKN
} pkt_type_t;


// a wrapper struct to hold different types of packet
typedef struct packet_t {
    pkt_type_t type;
    char payload[];

} packet_t;

// responsible for holding data from the file + info for chunk reassembly
typedef struct DataPacket{
    int file_id;
    int chunk_no;
    uint8_t checksum;
    size_t data_size; // just to know how much bytes is stored in data (up to given chunk input)
    unsigned char data[];

} DataPacket;

// responsible for holding the metadata individual files + the total to be sent
#pragma pack(push, 1)
typedef struct ControlPacket{
    int tot_files; // allows late joiners to know if they missed entire files
    int file_id;
    int tot_chunk; // total number of chunks for file_name
    int chunk_size;
    long file_size;
    int total_file_checksum;
    // int file_checksum; --> could use to validate final file
    char file_name[];

} ControlPacket;
#pragma pack(pop)

// responsible for confirmation between sender and reciever
typedef struct Ack{
    int originator_id; // an id number to differentiate who sent this ack 
    int ack_type; // 1 = NACK/RESEND, 2 = FINISHED, 3 = REQUEST_CTRL, 5 = JOIN_ACK, 9 = SENDER_FINISHED_SIGNAL
    int ack_no; // packet or file

} Ack;

uint8_t checksum(const unsigned char *data, size_t len);

#endif
