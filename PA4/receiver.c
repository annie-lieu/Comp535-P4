#include <stdio.h> 
#include <stdlib.h>    
#include <string.h>
#include <sys/stat.h>
#include "multicast.h"
#include "packet.h"

char[] dir_name = "receiver_";

int write_flag; // indicate to write entire file buffer to disk

int seq_no = -1; // keeps track of the current ACK seq number sent for the current file
// starts at -1 for each file 
// seq_no = last inorder chunk index

unsigned char *file_buf;

ControlPacket *cur_file_info; // keep track of the current file info

int main(int argc, char const* argv[]){

    if (argc <= 1){
        printf(">> Error: receiver.c requires a positive integer for identification")
    }

    // get the id number
    char* end;
    int recv_id = strtol(argv[1], &end, 10);
    if (*end != '\0' || recv_id <= 0){
        printf(">> Error: The '-c' argument requires a positive integer");
    }

    // check if the id is unique by creating the directory
    strcat(dir_name,argv[1]); 


    struct stat statbuf; // this is from that mkdir tutorial...

    if (stat(dir_name, &statbuf) == 0) { // there was a duplicate dir
        printf(">> Error: Given id number is not unique");
    } else {
        // id is unique -> create the directory
        if (mkdir(dir_name, 0755) != 0) {
            perror(">> Error: Could not create Directory");
        }
    }

    // join the multicast group
    mcast_t *cast = multicast_init("239.2.2.2", 4000, 5000); // send and receive on different ports

    multicast_setup_recv(cast);

    // start listening for packets
    listener(cast);

    

    // prepare for recieving multicasting --> joining the multicasting group + setting up the window

    // have a buffer to receive stuff

    // one function to order + handle check verification --> have some sort of array to place stuff inorder
    // implement just receiving the data and placing it into the buffer --> leave room for ACK


    // another function to read/write to disk 
    // --> open a file to write once we receive its first chunk, 
    // then keep wrtting until there are no more inorder chunks
    // have a pointer so the thread will only read from the inorder section

    // another function to do the final file verification

    return 0;
}

void listener(mcast_t* mcast){
    // set 10KB as the packet size limit
    int max_payload = 10000;

    while(1){
        packet_t* pkt = malloc(sizeof(packet_t) + max_payload);

        if (multicast_check_receive(mcast) > 0) {
            multicast_receive(mcast, pkt, sizeof(pkt));

            // parse packet type
            switch(pkt->type) {
                case DATA:
                    //check if we received the control packet
                    if (!file_buf){
                        // missing control packet --> request (TODO)

                        printf("No control packet --> break");
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



                    printf("Control info:");
                    printf("total files: %d", cur_file_info->tot_files);
                    printf("cur file: %d", cur_file_info->file_id);
                    printf("total chunks: %d", cur_file_info->tot_chunk);
                    printf("chunk size: %d", cur_file_info->chunk_size);
                    printf("file name: %s", cur_file_info->file_name);
                    


                    // initalize the file buffer (should be null)
                    file_buf = (unsigned char *)malloc(cur_file_info->chunk_size * cur_file_info->tot_chunk); 

                    // interate through entire buffer and set to '\0'
                    for (int i = 0; i <= sizeof(file_buf)){
                        file_buf[i] = '\0';
                    }

                    break;
                case ACK:
                    // TODO

                    
            }


            
        }
    }

} // listens for packets and ACKs --> if ack from sender (sender wants to terminate), must handle
// if received a data packet --> send to reassemble_file
// look if we have a buffer for the packet's file id
// if yes, place in buffer in the correct location
// else, (missing control packet for the inital file--> need to ACK 0)


// if control packet --> ...
// if ack...



// verify integrity, accept or reject file..
void verify_chunk(DataPacket *pkt){
    // perform checksum calculation on the sent data
    uint32_t calc_chksum = checksum(pkt->data, pkt->data_size);

    printf("Got file %d, chunk %d with %s", pkt->file_id ,pkt->chunk_no,pkt->data)
    printf("Calculated: %d", calc_chksum);
    printf("sent: %d", pkt->checksum);

    // determine if the chunk is corrupted
    if (calc_chksum == pkt->checksum){
        // write data into file_buf
        reassemble_file(pkt);

        // if we ACKed all the chunks for this file..
        if (write_flag){
            int file_verified = write_file();

            // TODO:
            if (file_verified){
                // free control packet to move onto the next file
                free(cur_file_info);

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


// place chunk in correct position + update sequence number if needed
void reassemble_file(DataPacket *pkt){

    // copy the data into the correct location
    int idx = pkt->chunk_no * cur_file_info->chunk_size;

    printf("writing into idx %d", idx);


    memcpy(&file_buf[idx], pkt->data);


    // check if we need to update the sequence number
    if (pkt->chunk_no == (seq_no + 1)){
        // we can advance the sequence number by 1 (assuming sender only sends packets 1 at a time)
        seq_no++;
    }

    // check if we received all chunks
    if (seq_no == (cur_file_info->tot_chunk-1)){
        // finished --> set the write flag
        write_flag = 1;
    }
}


int write_file(){
    printf("writing the file")

    int verified = 1; // NEED TO IMPLEMENT FINAL FILE VERIFICATION

    if (verified){
        // create the new file
        char *path = "";
        memcpy(path, dir_name)
        memcpy(path,"/");
        memcpy(path, cur_file_info->file_name);

        FILE* fptr = fopen(path, "wb");
        if (!fptr){
            printf("Something went wrong in writing the file");

            // reject the file
            return 0;
        }

        // write the file buffer
        fwrite(file_buf, 1, strlen(file_buf), fptr);

        // close the file
        fclose(fptr);

        // free buffer
        free(file_buf);
    }

    // reset write flag
    write_flag = 0;

    // let other functions know if we rejected or accepted the file
    return verified

    
}



void request_missing_file();
//void terminate_session(); // send finished ack 


