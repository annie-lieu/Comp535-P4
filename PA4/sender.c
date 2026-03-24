#include <stdio.h> 
#include <stdlib.h>    
#include <string.h>
#include "multicast.h"
#include "packet.h"


int receiver_num = 0; // counts the current number of receivers in the multicast


int main(int argc, char const* argv[]){
    // parse the input to send something
    if (argc < 2) {
        printf(">> Error: sender.c requires at least 1 file as input");
        return 1;
    }

    int file_idx = 1;
    long chunk_size = 1024; // bytes

    if (strcpm(argv[1], "-c")){
        // specifies the chunk size

        // try to convert to a number
        char* end;
        chunk_size = strtol(argv[2], &end, 10);
        if (*end != '\0' || i <= 0){
            printf(">> Error: The '-c' argument requires a positive integer");
        }

        // files start at index 3
        file_idx = 3;
    }

    // determine which given files can be multicast
    int tot_files = 0;
    char* files[argc - file_idx];
    DataPacket** file_chunks[argc - file_idx]; // keep in memory to allow late joiners to recieve

    for (file_idx; file_idx < argc; file_idx++){

        FILE* fptr;

        fptr = fopen(argv[file_idx], "r");

        // checking if we can read the current file
        if (!fptr) {
            printf(">> Cannot open file ___ in this directory - Will not multicast file");
            continue;

        } else {
            // can read --> prepare to multicast
            printf(">> Chunking and processing file %s", argv[file_idx]);

            strcpy(files[tot_files], argv[file_idx]);

            // determine total number of chunks needed
            int tot = file_buf_size(chunk_size,fptr);

            // create data packets for the file and store in memory
            file_chunks[tot_files] = chunk_file(chunk_size, tot, file_idx, files[tot_files])

            if (!file_chunks[tot_files]){
                // something went wrong in chunking --> will not multicast
                files[tot_files] = NULL;
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
        printf(">> Currently sending %s", files[i]);

        // sending
        send_file(tot_files, i, sizeof(file_chunks[i]), chunk_size, files[i], file_chunks[i], cast);

    }

    // get all the files to send
    // validate they are files we can read data from
    // for each file, we chunk separately and send
    // notify the user we are currently sending _ file

    // should track statistics 

    // finished sending files
    // TODO: get ready to terminate after confirming all current recievers got the files

    return 0;
}

int file_buf_size(int csize, FILE* fptr){
    
    // go to the end of the file
    fseek(fptr 0L, SEEK_END);

    // get size of the file
    long fsize = ftell(fptr);

    // close the file
    fclose(fp);

    // calculate the total chunks needed --> ceiling(fsize/csize)
    int tot = (int) fsize/csize + (fsize % csize != 0);

    return tot;

}

// some sort of chunking function + buffer for sending
DataPacket** chunk_file(int csize, int tot_chunk, int fid, char* fname){
    // create the buffer to hold the string
    DataPacket **data_pkts = malloc(sizeof(DataPacket*) * csize)

    // open the file
    FILE* fptr;

    fptr = fopen(fname, "rb");

    // just an extra check
    if (!fptr) {
        printf(">> File opening error occured - Will not multicast file");
        return NULL;
    }


    for (int i = 0; i < tot_chunk; i++){
        // need mallocing strings to save to a buffer
        unsigned char *buf = malloc(csize);

        if (!buf) {
            printf(">> Malloc chunk error occured - Will not multicast file")
            
            fclose(fptr);

            for (int j = 0; j <= i; j++){
                free(data_pkts[j]);
            }
            free(data_pkts);

            return NULL;
        }

        // reading the file
        fread(buf, 1, csize, fptr);

        printf("Read: %s",buf);
        printf("size: %f",sizeof(*buf));

        // create the data packet
        int data_size = sizeof(*buf); // check it actually gets the size of the string

        data_pkts[i] = malloc(sizeof(DataPacket) + sizeof(*buf) + 1);
        data_pkts[i]->file_id = fid;
        data_pkts[i]->chunk_no = i;
        data_pkts[i]->checksum = checksum(buf, data_size);
        data_pkts[i]->data_size = data_size;
        memcpy(data_pkts[i]->data, buf);

        printf("checksum: %d",data_pkts[i]->checksum);
    }

    fclose(fptr);

    return data_pkts;
}



// should add multi threading to chunk and send at the same time --> use a blocking queue
// technically, we dont need a formal blocking queue if we use array pointers
// and keep everyone in their segments


// a function to send those chunks and check for ACK --> need TO
// handles retransmission
void send_file(int tot_files, int fid, int tot_chunk, int csize, char* fname,
                 DataPacket **pkts, mcast_t *mcast){ 

    // create the control packet to dictate the shape of this file
    ControlPacket *c = malloc(sizeof(ControlPacket) + strlen(fname) + 1);
    c->tot_files = tot_files;
    c->file_id = fid;
    c->tot_chunk = tot_chunk;
    c->chunk_size = csize;
    memcpy(c->file_name, fname);


    printf("sending control");

    // send the intial control packet
    multicast_send(mcast, c, sizeof(c) + strlen(fname) + 1);

    //TODO: check if all current receivers (receiver_num) got the packet 
    


    // sending the data packets (sucessfully sent control packet)
    int cur_idx = 0; // min seq chunk to send

    while (cur_idx != tot_chunk){
        // send something (check error message of multicast_send()?)
        printf("sending chunk %d", cur_idx);


        multicast_send(mcast, pkts[cur_idx], sizeof(DataPacket) + pkts[cur_idx]->data_size);


        // wait for ACK and retransmit as needed (update cur_idx accordingly)
        cur_idx++; // temporary since missing above implementation


        // currently sends packets one at a time.. (maybe send more at a time?)

    }
    


    // send metadata in a control packet --> time to get all acks from receivers = RTT -> TO = RTT*2
    
    // send chunks with headers

    // get ACK --> wait until TO before retransmission
    // --> cur_min = min(cur_min, min ACK received) (use cur_min if ack_received != receiver_num)

    // retransmit chunks in window if nessacary --> slide the window to the min ack number and send all of that
    // loop until we cur_min = all the chunks


    return;
}

// when the sender finishes sending everything, it sends a message
// all current receivers must send back FILE ACK (# of files) 
// --> sender responds back = that receiver disconnect + sender sets receiver_num--;
// --> if reciever TO (say 30s), it sends back FILE ACK (# of files) until sender says it can disconnect 
// if a receiver is missing file(s), it messages back (FILE NACK up to k), then we retransmit those files
// when sender receiver_num = 0, it can disconnect 





// just implement the sending part and leave room for implementing a TO and other reliability




// a function to track stats?