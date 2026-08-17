#include <stdio.h> 
#include <stdlib.h>    
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include "multicast.h"
#include <time.h>
#include <sys/stat.h>
#include "packet.h"

#define MAX_RECEIVERS 32

int total_unique_count = 0;              // To track if we've found anyone at all
uint32_t master_receiver_mask = 0;       // Bits set for every ID we've EVER seen
long total_packets_sent = 0;

bool is_id_in_list(int *list, int count, int id);
void send_chunk(int chunk_no, DataPacket **pkts, mcast_t *mcast, packet_t *envelope);

long get_file_size(const char *filename);
int calculate_chunks(long fsize, int csize);

// function for spliting a file into chunks and packaging them into DataPackets
DataPacket** chunk_file(int csize, int tot_chunk, int fid, const char* fname){
    // create the buffer to hold the packets
    DataPacket **data_pkts = calloc(tot_chunk, sizeof(DataPacket*));

    // open the file
    FILE* fptr = fopen(fname, "rb");

    if (!fptr) {
        perror(">> File opening error occured - Will not multicast file");
        return NULL;
    }

    rewind(fptr);
    for (int i = 0; i < tot_chunk; i++){
        // create a buffer to store the chunk in
        unsigned char *buf = calloc(1, csize);

        if (!buf) {
            perror(">> Calloc chunk error occured - Will not multicast file");
            
            fclose(fptr);

            for (int j = 0; j < i; j++){ // check this will cause errors
                free(data_pkts[j]);
            }
            free(data_pkts);

            return NULL;
        }

        // reading the file
        size_t data_size = fread(buf, 1, csize, fptr);

        // create the data packet
        data_pkts[i] = calloc(1,sizeof(DataPacket) + data_size);
        data_pkts[i]->file_id = fid;
        data_pkts[i]->chunk_no = i;
        data_pkts[i]->checksum = checksum(buf, data_size); 
        data_pkts[i]->data_size = data_size;
        memcpy(data_pkts[i]->data, buf, data_size);

    }

    fclose(fptr);

    return data_pkts;
}

int get_bit_index(int id, int *list) {
    // 1. Check if we already know this ID
    for (int i = 0; i < total_unique_count; i++) {
        if (list[i] == id) {
            return i; // Use this index as the bit position
        }
    }

    // 2. If it's a new ID, add it to the list
    if (total_unique_count < 32) {
        list[total_unique_count] = id;
        printf("New Receiver Registered: ID %d at Bit %d\n", id, total_unique_count);
        return total_unique_count++; 
    }

    return -1; // Too many receivers 
}



// a function to send those chunks and check for ACK + handles retransmission
void send_file(int tot_files, int fid, int tot_chunk, int csize, char* fname,
                 DataPacket **pkts, mcast_t *mcast, uint32_t *file_completion_masks, int *discovered_ids){ 

    // get file checksum value
    FILE *fp = fopen(fname, "rb");
    fseek(fp, 0, SEEK_END);
    size_t file_size = ftell(fp);
    rewind(fp);
    unsigned char *temp_buffer = malloc(file_size);
    fread(temp_buffer, 1, file_size, fp);
    uint8_t master_checksum = checksum(temp_buffer, file_size);
    free(temp_buffer);
    fclose(fp);

    // create the control packet to dictate the shape of this file
    long ctrl_len = sizeof(ControlPacket) + strlen(fname) + 1;
    long fsize = get_file_size(fname);

    ControlPacket *c = calloc(1, ctrl_len);
    c->tot_files = tot_files;
    c->file_id = fid;
    c->tot_chunk = tot_chunk;
    c->chunk_size = csize;
    c->file_size = fsize;
    c->total_file_checksum = master_checksum;

    // get absolute path name
    const char *pure_name = strrchr(fname, '/');
    if (pure_name) pure_name++;
    else pure_name = fname;

    strcpy(c->file_name, pure_name);

    // wrap the control packet in a shell
    packet_t *meta_package = malloc(sizeof(packet_t) + ctrl_len);
    meta_package->type = CTRL;
    memcpy(meta_package->payload, c, ctrl_len);

    int max_ack_size = sizeof(packet_t) + sizeof(Ack) + 100;
    packet_t *recv_pkt = malloc(max_ack_size); 
    
    // Send CTRL packet
    multicast_send(mcast, meta_package, sizeof(packet_t) + ctrl_len);
    total_packets_sent++;

    // Wait and listen for JOIN_ACKS
    time_t start_time = time(NULL);
    // Wait 2 seconds for everyone to check in
    while (time(NULL) - start_time < .5) { 
        if (multicast_check_receive(mcast) > 0) {
            multicast_receive(mcast, recv_pkt, 1000);
            Ack *in = (Ack*)recv_pkt->payload;
            
            if (in->ack_type == 5) {
                int bit_pos = get_bit_index(in->originator_id, discovered_ids);
                if (bit_pos != -1) {
                    master_receiver_mask |= (1 << bit_pos); // Flip their switch to "Required"
                }
            }
        }
    }

    // sending the data packets (sucessfully sent control packet)
    int cur_idx = 0; // min seq chunk to send

    int max_data_pkt_size = sizeof(packet_t) + sizeof(DataPacket) + csize;
    packet_t *envelope = malloc(max_data_pkt_size);

    // Create the inner payload (The Signal)
    Ack eof_signal;
    eof_signal.ack_type = 9;         
    eof_signal.originator_id = 0;
    eof_signal.ack_no = tot_chunk;  

    // Wrap it in the packet_t shell
    packet_t *p_eof = malloc(sizeof(packet_t) + sizeof(Ack));
    p_eof->type = ACKN;            
    memcpy(p_eof->payload, &eof_signal, sizeof(Ack));

    // Size to send
    int eof_size = sizeof(packet_t) + sizeof(Ack);

    while (true) {

        // Check if everyone we know about has finished THIS specific file
        if (master_receiver_mask != 0 && (file_completion_masks[fid] & master_receiver_mask) == master_receiver_mask) {
            break; 
        }

        // If we still have new chunks to send, send one
        if (cur_idx < tot_chunk) {
            send_chunk(cur_idx, pkts, mcast, envelope);
            cur_idx++;
        } else {
            multicast_send(mcast, p_eof, eof_size); // Heartbeat
            total_packets_sent++;
            usleep(1000); 
        }

        while (multicast_check_receive(mcast) > 0) {
            multicast_receive(mcast, recv_pkt, max_ack_size);
            Ack *in = (Ack*)recv_pkt->payload;

            int bit_pos = get_bit_index(in->originator_id, discovered_ids);
            if (bit_pos != -1) {
                master_receiver_mask |= (1 << bit_pos);
            }

            if (in->ack_type == 1) { // NACK
                send_chunk(in->ack_no, pkts, mcast, envelope);
            } 
            else if (in->ack_type == 2) { // FINISHED
                if (bit_pos != -1) {
                    file_completion_masks[in->ack_no] |= (1 << bit_pos);
                }
            }
            else if (in->ack_type == 3) { // REQUEST_CTRL (Late Joiner)
                // Send the Control Packet so they can initialize
                multicast_send(mcast, meta_package, sizeof(packet_t) + ctrl_len);
                total_packets_sent++;
            }
        }

        // for safety: if no one is listening
        // break out of loop
        if (master_receiver_mask == 0 && time(NULL) - start_time > 3) break;

    }
    free(envelope);
    free(meta_package);
    free(recv_pkt);
}

// send chunk (dont keep malloc and freeing packet_t)
void send_chunk(int chunk_no, DataPacket **pkts, mcast_t *mcast, packet_t *envelope) {
    long payload_len = sizeof(DataPacket) + pkts[chunk_no]->data_size;
    envelope->type = DATA;
    memcpy(envelope->payload, pkts[chunk_no], payload_len);
    multicast_send(mcast, envelope, sizeof(packet_t) + payload_len);
    total_packets_sent++;
}

// return true if id is in list
bool is_id_in_list(int *list, int count, int id){

    for(int i = 0; i < count; i ++){
        if (list[i] == id){
            return true;
        }
    }
    return false;

}

long get_file_size(const char *filename) {
    struct stat st;
    if (stat(filename, &st) == 0) {
        return st.st_size;
    }
    return -1; // Error case
}

int calculate_chunks(long fsize, int csize) {
    if (fsize <= 0) return 0;
    return (int)((fsize + csize - 1) / csize);
}


int main(int argc, char const* argv[]){
    
    if (argc < 2) {
        printf(">> Error: sender.c requires at least 1 file as input \n");
        return 2;
    }

    int file_idx = 1;
    long chunk_size = 1024; // in bytes

    // user specified the chunk size
    if (strcmp(argv[1], "-c") == 0){

        // try to convert to a number
        char* end;
        chunk_size = strtol(argv[2], &end, 10);
        if (*end != '\0' || chunk_size <= 0){
            printf(">> Error: The '-c' argument requires a positive integer \n");
            return 2;
        }

        // files start at argv[3]
        file_idx = 3;
    }

    // determine which given files can be multicast
    int tot_files = 0;
    char** files = calloc(argc - file_idx, sizeof(char*));
    DataPacket*** file_chunks = calloc(argc - file_idx, sizeof(DataPacket**)); // keep in memory to allow late joiners to recieve
    int file_len[argc - file_idx];

    for (int i = file_idx; i < argc; i++){

        FILE* fptr;

        fptr = fopen(argv[i], "r");

        // checking if we can read the current file
        if (!fptr) {
            perror(">> Error: Cannot open file");
            printf("    --> Will not multicast file %s\n", argv[i]);
            continue;

        } else {
            // duplicate file check
            bool is_duplicate = false;
            for (int j = 0; j < tot_files; j++) {
                if (strcmp(files[j], argv[i]) == 0) {
                    printf(">> Warning: Skipping duplicate file path: %s\n", argv[i]);
                    is_duplicate = true;
                    break;
                }
            }
            if (is_duplicate) {
                fclose(fptr); 
                continue;     
            }
            // can read --> prepare to multicast
            printf(">> Chunking and processing file %s \n", argv[i]);

            // determine total number of chunks needed
            long fsize = get_file_size(argv[i]);
            file_len[tot_files] = calculate_chunks(fsize, chunk_size); // Calculate ONCE

            // create data packets for the file and store in memory 
            file_chunks[tot_files] = chunk_file(chunk_size, file_len[tot_files], tot_files, argv[i]);

            // parse the path to only get the file name
            const char *filename = strrchr(argv[i], '/');

            if (filename) {
                // argv[i] was a path so move filename to the start of the file name
                filename++;
            } else {
                // argv[i] wasn't a path
                filename = argv[i];
            }

            files[tot_files] = strdup(argv[i]);

            // check if chunk_file() succeeded
            if (!file_chunks[tot_files]){
                // something went wrong --> will not multicast
                files[tot_files] = NULL;
                file_len[tot_files] = 0;
                continue;

            }

            tot_files++;

        }

    }
    int *discovered_ids = calloc(1024, sizeof(int));

    uint32_t file_completion_masks[tot_files];
    memset(file_completion_masks, 0, sizeof(file_completion_masks));

    // initializing sockets for multicasting
    mcast_t *cast = multicast_init("239.2.2.2", 5000, 4000); // send and receive on different ports

    multicast_setup_recv(cast);

    bool all_batch_done = false;

    struct timespec start_total, end_total;
    clock_gettime(CLOCK_MONOTONIC, &start_total); // Start Clock    

    while (!all_batch_done) {
        all_batch_done = true; // Assume we are done until proven otherwise

        for (int i = 0; i < tot_files; i++) {
            // Check if anyone is missing this specific file
            if ((file_completion_masks[i] & master_receiver_mask) != master_receiver_mask || master_receiver_mask == 0) {
                
                all_batch_done = false; // Someone is missing it, so the batch isn't done

                // start processing
                printf(">> Currently sending %s \n", files[i]);
                
                send_file(tot_files, i, file_len[i], chunk_size, files[i], file_chunks[i], cast, file_completion_masks, discovered_ids);
                
            }
        }

        // check if no receivers joined the multicast
        if (total_unique_count == 0){
            all_batch_done = true;
        }

        if (!all_batch_done) {
            printf(">> Loop complete, but some receivers not receieved all files yet. Starting next pass...\n");
            sleep(2); // Prevent slamming the CPU/Network
        }
    }

    // track statistics

    clock_gettime(CLOCK_MONOTONIC, &end_total); // Stop Clock

    double total_latency = (end_total.tv_sec - start_total.tv_sec) + 
                        (end_total.tv_nsec - start_total.tv_nsec) / 1000000000.0;
    double pps = total_packets_sent / total_latency;

    printf(">> TRANSMISSION COMPLETE <<\n");
    printf(">> All known receivers have confirmed all files. Closing sender.\n");
    printf("|| Total End-to-End Latency: %.3f seconds\n", total_latency);
    printf("|| Average Throughput: %.2f packets/sec\n", pps);
    
    // free all resources
    for (int i = 0; i < tot_files; i++) {
        for (int j = 0; j < file_len[i]; j++){
            free(file_chunks[i][j]);
        }

        free(files[i]);
        free(file_chunks[i]);

    }

    free(discovered_ids);
    free(files);
    free(file_chunks);
    
    printf(">> Exiting...\n");

    return 0;
}


