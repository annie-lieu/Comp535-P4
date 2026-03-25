#include <stdio.h> 
#include <stdlib.h>    
#include <string.h>
#include <unistd.h>
#include "multicast.h"
#include "packet.h"

// counts the current number of receivers in the multicast
//  --> we increase this count when we receive some ACK (or NACK) from a reciever with a different ID
int receiver_num = 0; 


// SUMMARY OF TODO:
// - implement retransmission --> waiting for ACK from all current receivers and sending chunks based on lowest ACK
// - implement closing the program --> message all receivers that sender is shutting down (que for receiver to request missing files)
//                                 --> handle all missing file request before closing
//                                 --> when a receiver receives all files, it can ACK sender back and disconnect (update receiver_num)
// - track statistics about transmission



// determines how large a file's buffer should be
int file_buf_size(int csize, FILE* fptr){
    
    // go to the end of the file
    fseek(fptr, 0L, SEEK_END);

    // get size of the file
    long fsize = ftell(fptr);

    // close the file
    fclose(fptr);

    // calculate the total chunks needed --> ceiling(fsize/csize)
    int tot = (int) fsize/csize + (fsize % csize != 0);

    return tot;

}

// function for spliting a file into chunks and packaging them into DataPackets
DataPacket** chunk_file(int csize, int tot_chunk, int fid, const char* fname){
    // create the buffer to hold the packets
    DataPacket **data_pkts = calloc(csize, sizeof(DataPacket*));

    // open the file
    FILE* fptr = fopen(fname, "rb");

    // just an extra check
    if (!fptr) {
        perror(">> File opening error occured - Will not multicast file");
        return NULL;
    }


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

        //printf("Read: \n|||%s|||\n",buf);
        printf("~~~~~~~~~~~~~~~DEBUGING~~~~~~~~~~~~~~~~~~\n");
        printf("size: %ld \n",data_size);
        printf("chunkno: %d \n",i);
        printf("fid: %d \n",fid);
        

        // create the data packet
        data_pkts[i] = calloc(1,sizeof(DataPacket) + data_size);
        data_pkts[i]->file_id = fid;
        data_pkts[i]->chunk_no = i;
        data_pkts[i]->checksum = checksum(buf, data_size); 
        data_pkts[i]->data_size = data_size;
        memcpy(data_pkts[i]->data, buf, data_size);

        printf("checksum: %d \n",data_pkts[i]->checksum);
        printf("~~~~~~~~~~~~~~~~~END~~~~~~~~~~~~~~~~~~\n");
        // maybe checksum should include other file headers to ensure no corruptions for fields
    }

    fclose(fptr);

    return data_pkts;
}



// a function to send those chunks and check for ACK + handles retransmission
void send_file(int tot_files, int fid, int tot_chunk, int csize, char* fname,
                 DataPacket **pkts, mcast_t *mcast){ 

    printf("DEBUGING: sending control \n");

    // create the control packet to dictate the shape of this file
    long ctrl_len = sizeof(ControlPacket) + strlen(fname) + 1;
    ControlPacket *c = calloc(1, ctrl_len);
    c->tot_files = tot_files;
    c->file_id = fid;
    c->tot_chunk = tot_chunk;
    c->chunk_size = csize;
    strcpy(c->file_name, fname);

    // wrap the control packet in a shell
    packet_t *meta_package = malloc(sizeof(packet_t) + ctrl_len);
    meta_package->type = CTRL;
    memcpy(meta_package->payload, c, ctrl_len);

    // send the intial control packet
    multicast_send(mcast, meta_package, sizeof(packet_t) + ctrl_len);


    // free the shell
    free(meta_package);


    //TODO: check if all current receivers (receiver_num) got the control packet 
    


    // sending the data packets (sucessfully sent control packet)
    int cur_idx = 0; // min seq chunk to send

    while (cur_idx < tot_chunk){
        printf("~~~~~~~~~~~~~~~DEBUGING~~~~~~~~~~~~~~~~~~\n");
        printf("sending chunk %d \n", cur_idx);
        printf("struct size %ld \n", sizeof(DataPacket));
        printf("data size %ld \n", pkts[cur_idx]->data_size);
        printf("~~~~~~~~~~~~~~~~~END~~~~~~~~~~~~~~~~~~\n");

        // put the data into a shell
        long payload_len = sizeof(DataPacket) + pkts[cur_idx]->data_size;
        packet_t *package = malloc(sizeof(packet_t) + payload_len);
        package->type = DATA;
        memcpy(package->payload, pkts[cur_idx], payload_len);

        // sending
        multicast_send(mcast, package, sizeof(packet_t) + payload_len);

        // no longer need this outer shell
        free(package);


        // wait for ACK and retransmit as needed (update cur_idx accordingly)
        cur_idx++; // temporary line since missing above implementation

        // --> if we gotten ACKs from everyone => cur_idx = min(all ACKs received)
        // --> if we haven't received ACKs from all receivers => cur_idx = min(cur_idx, min of all ACKs received)


        // currently sends packets one at a time.. (maybe change implementation to send more at a time?)

    }

    return;
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
            printf("    -- Will not multicast file %s\n", argv[i]);
            continue;

        } else {
            // can read --> prepare to multicast
            printf(">> Chunking and processing file %s \n", argv[i]);

            // determine total number of chunks needed
            file_len[tot_files] = file_buf_size(chunk_size,fptr);

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

            files[tot_files] = strdup(filename);

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

    // initializing sockets for multicasting
    mcast_t *cast = multicast_init("239.2.2.2", 5000, 4000); // send and receive on different ports

    multicast_setup_recv(cast);
    

    // multicasting the processed files
    for (int i = 0; i < tot_files; i++){
        
        // start processing
        printf(">> Currently sending %s \n", files[i]);

        printf("~~~~~~~~~~~~~~~DEBUGING~~~~~~~~~~~~~~~~~~\n");
        printf("file %d \n", i);
        printf("tot chunks %d \n", file_len[i]);
        printf("~~~~~~~~~~~~~~~~~END~~~~~~~~~~~~~~~~~~\n");

        // sending
        send_file(tot_files, i, file_len[i], chunk_size, files[i], file_chunks[i], cast);

    }

    // finished sending files
    // TODO: get ready to terminate after confirming all current recievers got the files



    // free all resources
    for (int i = 0; i < tot_files; i++) {
        for (int j = 0; j < file_len[i]; j++){
            free(file_chunks[i][j]);
        }

        free(files[i]);
        free(file_chunks[i]);

    }

    free(files);
    free(file_chunks);
    
    printf(">> Exiting\n");

    return 0;
}


