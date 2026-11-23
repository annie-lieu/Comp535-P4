# Comp535-P4
This project simulates a sender multicasting files to any number of receiver, even if the connection is unreliable or a receiver joins late. That is, the program will ensure all receivers obtain every multicasted file uncorrupted.  

## Compile   
```bash
make clean  
make sender receiver 
```

## Running   

### Receiver
Receivers can join the multicast before or during the transmission and will terminate when they receive all the multicasted files intact. If the receiver joins after transmission ends, they will be stuck forever waiting and needs to be manually terminated.

- To have a receiver join the multicast: `./receiver id` where id is a positive unique integer. 

Each receiver will have their own directory, identified by their inputted id, to store the received mutlicasted files.

### Sender
The program only expects 1 sender instance. The sender can multicast any type of file and multicast multiple at once. When the sender starts transmission, it will continue to multicast until all receivers obtain the uncorrupted files. If no receivers join during the first transmission cycle, the sender will stop multicasting. 

- To start multicasting the files: `./sender path_to_file1 path_to_file2 ...`

- To specify packet chunk size for transmission: `./sender -c N file1 ...` where N is the chuck size in bytes

After the sender finishes transmission, it will report the `Total End-to-End Latency` and `Average Packet Throughput`.
