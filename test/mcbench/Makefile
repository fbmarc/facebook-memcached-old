OBJS=main.o \
	socket.o

CFLAGS=-g
LIBS=-lpthread

mcbench: $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LIBS)
