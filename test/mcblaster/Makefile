OBJS=main.o socket.o cpu.o

CFLAGS=-Werror -g -pthread

mcblaster: $(OBJS)
	$(CC) $(CFLAGS) -o $@ $(OBJS) $(LIBS)

clean:
	-rm -f $(OBJS) mcblaster
