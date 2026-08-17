#include <stdio.h> 
#include <stdlib.h>    
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include "multicast.h"
#include "packet.h"
#include <time.h>

char dir_name[] = "receiver_";  // receiver's directory to store multicast files
int write_flag;                 // indicate to write entire file buffer to disk
unsigned char *file_buf;        // buffer to put received chunks in
ControlPacket *cur_file_info;   // keep track of the current file info
int recv_id;                    // receiver id
int chunks_count = 0;           // ABSOLUTE count of chunks received
char *received_chunks;          // bitmap of chunks received
int files_completed;            // track how many files in batch completed



void send_nack(mcast_t* mcast, int chunk_no);

void send_ack_to_sender(mcast_t* mcast, Ack a);

// place chunk in correct position + update sequence number if needed
void reassemble_file(DataPacket *pkt, int location){

    if (received_chunks[pkt->chunk_no] == 0) {
        
        memcpy(&file_buf[location], pkt->data, pkt->data_size);
        
        // Mark as received and increment the ABSOLUTE count
        received_chunks[pkt->chunk_no] = 1;
        chunks_count++;
        
    }

    // Check for completion
    if (chunks_count == cur_file_info->tot_chunk) {
        // find checksum for whole file
        uint8_t final_integrity = checksum(file_buf, cur_file_info->file_size);

        if (final_integrity == cur_file_info->total_file_checksum) {
            printf(">> Integrity Verified for %s\n", cur_file_info->file_name);
            write_flag = 1;
        } else {
            printf(">> File Checksum Mismatch! Got: 0x%02X, Expected: 0x%02X\n", 
                    final_integrity, cur_file_info->total_file_checksum);
            memset(file_buf, 0, cur_file_info->tot_chunk * cur_file_info->chunk_size);
            
            // Reset state so the Carousel can try again
            printf(">> Resetting state to receive the file again\n");
            memset(received_chunks, 0, cur_file_info->tot_chunk * sizeof(int));
            chunks_count = 0;
        }
    }
}


int write_file(){
   if (!file_buf) return 0;

    char path[1024];
    snprintf(path, sizeof(path), "%s/%s", dir_name, cur_file_info->file_name);

    FILE* fptr = fopen(path, "wb");
    if (!fptr){
        perror(">> Fopen error");
        return 0;
    }

    // Write and Flush
    size_t written = fwrite(file_buf, 1, cur_file_info->file_size, fptr);
    fflush(fptr);
    fclose(fptr);

    printf(">> Successfully wrote %zu bytes into disk\n", written);

    // Clean buffer
    free(file_buf);
    file_buf = NULL; 
    
    write_flag = 0;
    return 1;

    
}

// verify integrity, accept or reject file..
void verify_chunk(DataPacket *pkt, mcast_t* mcast){

    // get the chunk's file buffer location
    int idx = pkt->chunk_no * cur_file_info->chunk_size;

    // check if we already written to this location before (received duplicate chunk)
    if (received_chunks[pkt->chunk_no] == 1) return;

    // perform checksum calculation on the sent data
    uint8_t calc_chksum = checksum(pkt->data, pkt->data_size);

    // determine if the chunk is corrupted + haven't receive this chunk yet
    if (calc_chksum == pkt->checksum){
        // write data into file_buf
        reassemble_file(pkt, idx);

        // if we ACKed all the chunks for this file..
        if (write_flag) {
            if (file_buf != NULL) {
                // Write the current file to disk
                if (write_file()) { 
                    files_completed++;
                    
                    // Prepare and send the finished ACK
                    Ack final_ack;
                    final_ack.originator_id = recv_id;
                    final_ack.ack_type = 2;             
                    final_ack.ack_no = cur_file_info->file_id; 

                    send_ack_to_sender(mcast, final_ack);
                    printf(">> Sent FINISHED ACK for File %d.\n", cur_file_info->file_id);

                    // Check if this was the last file in the sender's batch
                    if (files_completed >= cur_file_info->tot_files) {
                        printf(">> I have all %d files. Exiting.\n", files_completed);

                        free(cur_file_info);
                        free(file_buf);
                        free(mcast);

                        exit(0);
                    } else {
                        printf(">> Saved %d/%d files. Waiting for others...\n", files_completed, cur_file_info->tot_files);
                        
                        // Reset state to wait for the next file
                        free(cur_file_info);
                        cur_file_info = NULL;
                        free(received_chunks);
                        received_chunks = NULL;
                        chunks_count = 0;
                        write_flag = 0;
                        
                        printf(">> Ready for next file...\n");
                    }
                }
            }
        }

    } else {
        // send nack when gap detected or checksum fails

        printf(">> Checksum failed for chunk %d! Sending NACK.\n", pkt->chunk_no);

        send_nack(mcast, pkt->chunk_no);
        return; 

    }
    
}

// send nack to sender
void send_nack(mcast_t* mcast, int chunk_no){

    Ack nack;
    nack.originator_id = recv_id; 
    nack.ack_type = 1;           
    nack.ack_no = chunk_no;

    packet_t *p_out = malloc(sizeof(packet_t) + sizeof(Ack));
    p_out->type = ACKN;
    memcpy(p_out->payload, &nack, sizeof(Ack));

    multicast_send(mcast, p_out, sizeof(packet_t) + sizeof(Ack));
    free(p_out);
    return;
}

// send ack to sender
void send_ack_to_sender(mcast_t* mcast, Ack a){

    packet_t *p_out = malloc(sizeof(packet_t) + sizeof(Ack));
    p_out->type = ACKN;
    memcpy(p_out->payload, &a, sizeof(Ack));
    multicast_send(mcast, p_out, sizeof(packet_t) + sizeof(Ack));
    free(p_out);
}

// find the first chunk that is missing
int find_first_missing_chunk(){
    int total = cur_file_info->tot_chunk;
    for (int i = 0; i < total ; i++){
        if (received_chunks[i] == 0){ 
            return i;
        }
    }
    return -1;
}

void listener(mcast_t* mcast){
    // set 10KB as the packet size limit
    int max_payload = 10000;
    packet_t* pkt = calloc(1, sizeof(packet_t) + max_payload);

    // listen for packets
    while(1){

        if (multicast_check_receive(mcast) > 0) {
            multicast_receive(mcast, pkt, sizeof(packet_t) + max_payload);

            // parse packet type
            switch(pkt->type) {
                case DATA:
                    if (!file_buf) {
                        // ask for ctrl packet
                        static time_t last_request = 0;
                        if (time(NULL) - last_request > 1) { // Only ask once per second
                            printf(">> Received data but missing Control Packet. Requesting...\n");
                            Ack nack = { .originator_id = recv_id, .ack_type = 3, .ack_no = 0 };
                            send_ack_to_sender(mcast, nack);
                            last_request = time(NULL);
                        }
                    } else {
                        verify_chunk((DataPacket *) pkt->payload, mcast);
                    }
                    break;
                case CTRL:

                    ControlPacket *incoming = (ControlPacket*)pkt->payload;

                    if (cur_file_info == NULL) {
                        cur_file_info = calloc(1, sizeof(ControlPacket) + 256);
                    }
                    memcpy(cur_file_info, incoming, sizeof(ControlPacket) + 256);

                    // SCENARIO: WE ALREADY FINISHED THIS FILE
                    char path[1024];
                    snprintf(path, sizeof(path), "%s/%s", dir_name, incoming->file_name);
                    
                    if (access(path, F_OK) == 0) {
                        printf("Carousel: Already have %s. Sending ACK and ignoring.\n", incoming->file_name);
                        Ack already_done = { .originator_id = recv_id, .ack_type = 2, .ack_no = incoming->file_id };
                        send_ack_to_sender(mcast, already_done);
                        break; // Exit the switch case, don't allocate or reset
                        if (file_buf != NULL) {
                            free(file_buf);
                            file_buf = NULL;
                        }
                        break;
                    }

                    // SCENARIO: No buffer OR it's a brand new File ID
                    if (file_buf == NULL || (cur_file_info != NULL && incoming->file_id != cur_file_info->file_id)) { 
                        
                        // If we had a buffer from a previous file that didn't finish, clean it up first
                        if (file_buf != NULL) {
                            free(file_buf);
                            free(received_chunks);
                            free(cur_file_info);
                        }

                        // Initialize new file metadata
                        cur_file_info = calloc(1, sizeof(ControlPacket) + 256);
                        memcpy(cur_file_info, pkt->payload, sizeof(ControlPacket) + 256);

                        printf("Starting File %d/%d: %s\n", 
                                cur_file_info->file_id + 1, 
                                cur_file_info->tot_files, 
                                cur_file_info->file_name);

                        file_buf = (unsigned char *)calloc(cur_file_info->tot_chunk, cur_file_info->chunk_size); 
                        received_chunks = calloc(cur_file_info->tot_chunk, sizeof(char));
                        chunks_count = 0; 
                        write_flag = 0;

                    } 
                    
                    // SCENARIO: Always send the JOIN_ACK to keep sender happy whether new file or heartbeat
                    Ack join_ack = { .originator_id = recv_id, .ack_type = 5, .ack_no = incoming->file_id };
                    send_ack_to_sender(mcast, join_ack);
                    break;
                case ACKN: 
                    // the sender has information for the receiver (e.g. finished transmiting and want to shutdown)
                    Ack *in_signal = (Ack*)pkt->payload;

                    // PROTECTION: If we don't have metadata, we can't process heartbeats
                    if (cur_file_info == NULL) {
                        printf("Received Heartbeat but missing metadata. Requesting Control Packet...\n");
                        Ack req = { .originator_id = recv_id, .ack_type = 3, .ack_no = 0 };
                        send_ack_to_sender(mcast, req);
                        break; 
                    }

                    if (in_signal->ack_type == 9) { // Sender says they are done

                        char path[1024];
                        snprintf(path, sizeof(path), "%s/%s", dir_name, cur_file_info->file_name);

                        if (chunks_count == (cur_file_info->tot_chunk) || access(path, F_OK) == 0) {
                            // every chunk recieved or we already have the file
                            Ack done;
                            done.originator_id = recv_id;
                            done.ack_type = 2;
                            done.ack_no = cur_file_info->file_id; 
                            
                            // send done packet back to sender
                            send_ack_to_sender(mcast, done); 
                            
                            // Trigger the file write if not already done
                            if (file_buf != NULL) {
                                write_flag = 1;
                            }
                        } else {
                            // request missing chunk

                            send_nack(mcast, find_first_missing_chunk());
                        }
                    }
                    break;
                    
            }
            // finished processing the packet --> reset it to receive the next packet
            memset(pkt, '\0', sizeof(packet_t) + max_payload);   
        }
    }
}




int main(int argc, char const* argv[]){
    files_completed = 0;

    if (argc <= 1){
        printf(">> Error: receiver.c requires a positive integer for identification\n");
        return 2;
    }

    // get the id number
    char* end;
    recv_id = strtol(argv[1], &end, 10);
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

