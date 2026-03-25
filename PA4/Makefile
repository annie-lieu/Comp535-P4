CC = gcc
CFLAGS = -Wall -g

sender: sender.o packet.o multicast.o
	$(CC) $(CFLAGS) sender.o packet.o multicast.o -o sender

receiver: receiver.o packet.o multicast.o
	$(CC) $(CFLAGS) receiver.o packet.o multicast.o -o receiver

sender.o: sender.c packet.h multicast.h
	$(CC) $(CFLAGS) -c sender.c -o sender.o

receiver.o: receiver.c packet.h multicast.h
	$(CC) $(CFLAGS) -c receiver.c -o receiver.o

packet.o: packet.c packet.h
	$(CC) $(CFLAGS) -c packet.c -o packet.o

multicast.o: multicast.c multicast.h
	$(CC) $(CFLAGS) -c multicast.c -o multicast.o

clean:
	rm -f sender receiver *.o 
	rm -rf receiver_*