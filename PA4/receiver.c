#include <stdio.h> 
#include <stdlib.h>    
#include <string.h>
#include <sys/stat.h>
#include "multicast.h"
#include "packet.h"

char dir_name[] = "receiver_";

// indicate to write entire file buffer to disk
int write_flag;

// keeps track of the current ACK seq number sent for the current file
// starts at -1 for each file (ACK 0 = acknowledged the first chunk)
// seq_no = last inorder chunk index
int seq_no = -1; 

// buffer to put received chunks in
unsigned char *file_buf;

// keep track of the current file info
ControlPacket *cur_file_info;


// SUMMARY OF TODO:
// - implement retransmission --> requesting missing chunks + files, and handling + sending ACKs
// - implement closing the program
// - debug all of the functions in receiver.c (need to fix packet receiving bug first)
//       --> all of sender's current functions are debugged


// handles asking for files sent before receiver could join
void request_file();

// place chunk in correct position + update sequence number if needed
void reassemble_file(DataPacket *pkt, int location){

    printf("DEBUGING: writing into idx %d\n", location);


    // copy the data into the correct location
    memcpy(&file_buf[location], pkt->data, pkt->data_size);


    // check if we need to update the sequence number
    if (pkt->chunk_no == (seq_no + 1)){
        // we can advance the sequence number by 1 (assuming sender only sends a new packet once everyone ACK'd)
        // could modify sender to send up to n at a time
        seq_no++;
    }

    // check if we received all chunks
    if (seq_no == (cur_file_info->tot_chunk-1)){
        // finished --> set the write flag
        write_flag = 1;
    }
}


int write_file(){
    printf("DEBUGING: writing the file\n");

    int verified = 1; // NEED TO IMPLEMENT FINAL FILE VERIFICATION

    if (verified){
        // create the new file
        char path[1024];
        snprintf(path, sizeof(path), "%s/%s", dir_name, cur_file_info->file_name);

        FILE* fptr = fopen(path, "wb");
        if (!fptr){
            perror(">> Something went wrong in writing the file");

            // reject the file
            return 0;
        }

        // write the file buffer
        fwrite(file_buf, 1, strlen((const char *)file_buf), fptr);

        // close the file
        fclose(fptr);

        // free buffer
        free(file_buf);
    }

    // reset write flag
    write_flag = 0;

    // let other functions know if we rejected or accepted the file
    // --> useful to indicate if we need to request the whole file again
    return verified;

    
}

// verify integrity, accept or reject file..
void verify_chunk(DataPacket *pkt){

    // check if we already receieve this packet before
    int idx = pkt->chunk_no * cur_file_info->chunk_size;

    if (file_buf[idx] == '\0'){
        // ignore any duplicate chunks
        printf("DEBUGING: Have %c at idx %d\n", file_buf[idx], idx);
        free(pkt);
        return;
    }

    // perform checksum calculation on the sent data
    uint8_t calc_chksum = checksum(pkt->data, pkt->data_size);

    printf("~~~~~~~~~~~~~~~DEBUGING~~~~~~~~~~~~~~~~~~\n");
    printf("Got file %d, chunk %d with %s\n", pkt->file_id ,pkt->chunk_no,pkt->data);
    printf("Calculated: %d\n", calc_chksum);
    printf("sent: %d\n", pkt->checksum);
    printf("~~~~~~~~~~~~~~~~~END~~~~~~~~~~~~~~~~~~\n");

    // determine if the chunk is corrupted + haven't receive this chunk yet
    if (calc_chksum == pkt->checksum){
        // write data into file_buf
        reassemble_file(pkt, idx);

        // if we ACKed all the chunks for this file..
        if (write_flag){
            int file_verified = write_file();

            // TODO:
            if (file_verified){
                // free control packet to move onto the next file
                free(cur_file_info);


                // exit for now since implementation is incomplete
                printf("exiting...\n");
                exit(0);

            } else {
                // need to transmit the entire file + its control packet again
                
            }
        }

        // SEND ACK HERE

    } else {
        // RETRANSMIT IMPLMENTATION HERE
        // since sender has a timeout, we could reduce network overhead by not sending back ACK

    }


    // no longer need packet --> free it
    free(pkt);
    
}

void listener(mcast_t* mcast){
    // set 10KB as the packet size limit
    int max_payload = 10000;

    while(1){
        packet_t* pkt = calloc(1, sizeof(packet_t) + max_payload);

        if (multicast_check_receive(mcast) > 0) {
            multicast_receive(mcast, pkt, sizeof(pkt));

            printf("DEBUGING: got something\n");

            // parse packet type
            switch(pkt->type) {
                case DATA:
                    //check if we received the control packet
                    if (!file_buf){
                        // missing control packet --> request (TODO)

                        printf("No control packet --> exit program for now\n");
                        multicast_destroy(mcast);
                        break;

                    } else {
                        // proceed with checking the chunk
                        verify_chunk((DataPacket *) pkt->payload);
                    }
                    break;
                case CTRL:
                    // save the current file info
                    cur_file_info = (ControlPacket *) pkt->payload;


                    printf("~~~~~~~~~~~~~~~DEBUGING~~~~~~~~~~~~~~~~~~\n");
                    printf("Control info:\n");
                    printf("total files: %d\n", cur_file_info->tot_files);
                    printf("cur file: %d\n", cur_file_info->file_id);
                    printf("total chunks: %d\n", cur_file_info->tot_chunk);
                    printf("chunk size: %d\n", cur_file_info->chunk_size);
                    printf("file name: %s\n", cur_file_info->file_name);
                    printf("~~~~~~~~~~~~~~~~~END~~~~~~~~~~~~~~~~~~\n");
                    
                    // initalize the file buffer (should be null during intialization)
                    file_buf = (unsigned char *)calloc(cur_file_info->tot_chunk, cur_file_info->chunk_size); 

                    break;
                case ACKN: // TODO
                    // the sender has information for the receiver (e.g. finished transmiting and want to shutdown)

                    
            }

        }
    }

}




int main(int argc, char const* argv[]){

    if (argc <= 1){
        printf(">> Error: receiver.c requires a positive integer for identification\n");
        return 2;
    }

    // get the id number
    char* end;
    int recv_id = strtol(argv[1], &end, 10);
    if (*end != '\0' || recv_id <= 0){
        printf(">> Error: The id argument requires a positive integer\n");
        return 2;
    }

    // check if the id is unique by creating the directory
    strcat(dir_name,argv[1]); 
    struct stat statbuf; 

    if (stat(dir_name, &statbuf) == 0) { // there was a duplicate dir
        printf(">> Error: Given id number is not unique\n");
        return 2;
    } else {
        // id is unique -> create the directory
        if (mkdir(dir_name, 0755) != 0) {
            perror(">> Error: Could not create a directory for this receiever");

        }
    }

    // join the multicast group
    mcast_t *cast = multicast_init("239.2.2.2", 4000, 5000); // send and receive on different ports

    multicast_setup_recv(cast);

    // start listening for packets
    listener(cast);

    return 0;
}

