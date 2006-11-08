OBJS=main.o \
	socket.o

CFLAGS=-g -pg
LIBS=-lpthread

mcbench: $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LIBS)
