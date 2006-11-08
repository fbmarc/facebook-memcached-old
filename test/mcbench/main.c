/*
 * Round-trip time calculator for memcached.
 */
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <errno.h>
/*#include <malloc.h>*/
#include <pthread.h>

/* Percentage chance that we'll do a "get" (as opposed to a "delete") */
#define DEFAULT_PROBABILITY_GET 95

/* Number of iterations to run */
#define DEFAULT_ITERATIONS 100000

/* Number of connections to open */
#define DEFAULT_CONNECTIONS 1000

/* Number of simultaneous requests to send */
#define DEFAULT_REQUESTS 10

/* Size of our key space (determines cache hit rate) */
#define DEFAULT_KEYS 1000

/* Default time to wait for a response */
#define DEFAULT_POLL_WAIT_SEC 5

/* Default random seed */
#define DEFAULT_SEED 0

int probability_get = DEFAULT_PROBABILITY_GET;
int num_iterations = DEFAULT_ITERATIONS;
int num_connections = DEFAULT_CONNECTIONS;
int num_requests = DEFAULT_REQUESTS;
int num_keys = DEFAULT_KEYS;
int poll_wait_sec = DEFAULT_POLL_WAIT_SEC;

pthread_mutex_t stats_mutex;
pthread_mutex_t fd_mutex;

volatile int iteration_count = 0;
volatile int get_count = 0;
volatile int set_count = 0;
volatile int delete_count = 0;
volatile long long total_usec = 0;

char	*fd_in_use;
int	*fds;

unsigned long seed = DEFAULT_SEED;
/*
 * Blocks until a descriptor is readable.
 */
void ensure_readable(int fd)
{
	if (! waitread(fd, poll_wait_sec)) {
		fprintf(stderr, "\nfd %d went %d sec with no reply!\n",
			fd, poll_wait_sec);
		exit(1);
	}
}


/*
 * Consumes a value from a "GET". The "VALUE" keyword is assumed to have
 * already been read.
 */
void consume_value(int fd)
{
	char buf[BUFSIZ];
	char *c;
	int  bytesread;
	int  valuesize;

	buf[BUFSIZ - 1] = '\0';
	bytesread = read(fd, buf, BUFSIZ - 1);
	if (bytesread < 0) {
		perror("read");
		exit(1);
	}

	/* Keys are fixed length and flags are always 0 */
	if (bytesread < 25) {
		fprintf(stderr, "Not enough fields in VALUE line\n");
		exit(1);
	}

	c = strchr(buf + 22, '\n');
	if (c == NULL) {
		fprintf(stderr, "VALUE line not terminated\n");
		exit(1);
	}

	valuesize = atoi(buf + 22);

	/* Consume the trailing \r\nEND\r\n */
	valuesize += 7;

	/* And credit what we've already read */
	valuesize -= bytesread - (c - buf) + 1;

	while (valuesize > 0) {
		do {
			bytesread = read(fd, buf, BUFSIZ);
			if (bytesread == 0) {
				fprintf(stderr, "fd %d closed\n", fd);
				exit(1);
			}

			if (bytesread < 0)
				if (errno == EAGAIN)
					break;
				else {
					perror("read");
					exit(1);
				}

			valuesize -= bytesread;
		} while (bytesread > 0);

		if (valuesize > 0) {
			ensure_readable(fd);
		}
	}
}


/*
 * Sends a "set" and reads the response.
 */
void send_set(int fd, int keynum)
{
	char msg[BUFSIZ];
	int len = (random() % 1000000) / ((random() % 100) + 1);
	int byteswritten;

	sprintf(msg, "set rttestkey%09d 0 0 %d\r\n", keynum, len);
//printf(msg);
	do {
		byteswritten = write(fd, msg, strlen(msg));
		if (byteswritten < 0 && errno != EAGAIN) {
			perror("write");
			exit(1);
		}
	} while (byteswritten < 0);

	/* Send random junk from the stack; the value is unimportant */
	while (len > 0) {
		if (! waitwrite(fd, poll_wait_sec)) {
			fprintf(stderr, "\nfd %d went %d sec unwritable!\n",
				fd, poll_wait_sec);
			exit(1);
		}

		byteswritten = write(fd, msg, len < BUFSIZ ? len : BUFSIZ);
		if (byteswritten < 0) {
			if (errno == EAGAIN) {
				byteswritten = 0;
				continue;
			}
			perror("write");
			exit(1);
		}
		
		len -= byteswritten;
	}

	if (! waitwrite(fd, poll_wait_sec)) {
		fprintf(stderr, "\nfd %d went %d sec unwritable!\n",
			fd, poll_wait_sec);
		exit(1);
	}

	if (write(fd, "\r\n", 2) != 2) {
		perror("write");
		exit(1);
	}

	ensure_readable(fd);

	if (read(fd, msg, 8) < 8) {
		perror("read");
		exit(1);
	}

	pthread_mutex_lock(&stats_mutex);
	set_count++;
	pthread_mutex_unlock(&stats_mutex);
}


/*
 * Sends a "get" and reads the response.
 */
void send_get(int fd)
{
	int keynum = random() % num_keys;
	char cmd[30];
	char reply[5];
	struct timeval start, end;
	int usec;

	sprintf(cmd, "get rttestkey%09d\r\n", keynum);

//printf(cmd);
	gettimeofday(&start, NULL);
	if (write(fd, cmd, strlen(cmd)) < strlen(cmd)) {
		perror("write");
		exit(1);
	}

	while (1) {
		ensure_readable(fd);

		if (read(fd, reply, 5) == 5) {
			break;
		}
	}

	gettimeofday(&end, NULL);
	usec = (end.tv_usec - start.tv_usec) +
		(end.tv_sec - start.tv_sec) * 1000000;

	/* This will be "VALUE" or "END\r\n" */
	if (reply[0] == 'E') {
		send_set(fd, keynum);
	} else {
		consume_value(fd);
	}

	pthread_mutex_lock(&stats_mutex);
	get_count++;
	total_usec += usec;
	pthread_mutex_unlock(&stats_mutex);
}

/*
 * Sends a "delete" and reads the response.
 */
void send_delete(int fd)
{
	char cmd[50];
	char reply[20];

	sprintf(cmd, "delete rttestkey%09d\r\n", random() % num_keys);
//printf(cmd);
	if (write(fd, cmd, strlen(cmd)) < strlen(cmd)) {
		perror("write");
		exit(1);
	}

	ensure_readable(fd);

	/* Either "NOT_FOUND\r\n" or "DELETED\r\n" */
	if (read(fd, reply, 20) < 9) {
		perror("read");
		exit(1);
	}
	
	pthread_mutex_lock(&stats_mutex);
	delete_count++;
	pthread_mutex_unlock(&stats_mutex);
}

/*
 * Grabs an unused file descriptor.
 */
int unused_fd()
{
	int fd;

	pthread_mutex_lock(&fd_mutex);
	do {
		fd = fds[random() % num_connections];
	} while (fd_in_use[fd]);

	fd_in_use[fd]++;
	pthread_mutex_unlock(&fd_mutex);

	return fd;
}

/*
 * Releases a file descriptor.
 */
int release_fd(int fd)
{
	pthread_mutex_lock(&fd_mutex);
	fd_in_use[fd] = 0;
	pthread_mutex_unlock(&fd_mutex);
}

/*
 * Thread main loop.
 */
void *thread_main(void *arg)
{
	while (1) {
		int want_get = (random() % 100) < probability_get;
		int fd = unused_fd();
		int usec;

		if (want_get)
			send_get(fd);
		else
			send_delete(fd);

		release_fd(fd);


		pthread_mutex_lock(&stats_mutex);
		iteration_count++;
		pthread_mutex_unlock(&stats_mutex);
	}
}


/* Reports statistics. */
int stats()
{
	pthread_mutex_lock(&stats_mutex);
	if (iteration_count > 0)
		printf("\r%8d gets %8d sets %8d dels %12lld usec %4lld usec/get",
			get_count, set_count, delete_count, total_usec,
			total_usec / get_count);
	pthread_mutex_unlock(&stats_mutex);
	fflush(stdout);
}

void usage(char *prog)
{
	printf("Usage: %s [-c n] [-g n] [-i n] [-k n] [-r n] [-s s] [-t n] host port\n", prog);
	printf("\t-c n Open n connections (default: %d)\n",
		DEFAULT_CONNECTIONS);
	printf("\t-g n Probability of 'get' request (default: %d)\n",
		DEFAULT_PROBABILITY_GET);
	printf("\t-i n Limit run to n iterations (default: %d)\n",
		DEFAULT_ITERATIONS);
	printf("\t-k n Number of keys in key space (default: %d)\n",
		DEFAULT_KEYS);
	printf("\t-r n Requests to send simultaneously (default: %d)\n",
		DEFAULT_REQUESTS);
	printf("\t-s s Seed to use for random number generation (default: %d)\n",
		DEFAULT_SEED);
	printf("\t-t n Time out requests after n seconds (default: %d)\n",
		DEFAULT_POLL_WAIT_SEC);
	exit(1);
}

/* Returns an integer argument from getopt */
int intarg(char *prog, char *arg)
{
	int x = atoi(arg);
	if (x == 0)
		usage(prog);
	return x;
}

int main(int argc, char **argv)
{
	pthread_t thr;
	char	*host;
	int	port;
	int	i;
	int	c;
	char random_state[16];

	while ((c = getopt(argc, argv, "c:g:i:k:r:t:")) != EOF) {
		switch (c) {
		case 'c':
			num_connections = intarg(argv[0], optarg);
			break;
		case 'g':
			probability_get = intarg(argv[0], optarg);
			break;
		case 'i':
			num_iterations = intarg(argv[0], optarg);
			break;
		case 'k':
			num_keys = intarg(argv[0], optarg);
			break;
		case 'r':
			num_requests = intarg(argv[0], optarg);
			break;
		case 's':
			seed = intarg(argv[0], optarg);
			break;
		case 't':
			poll_wait_sec = intarg(argv[0], optarg);
			break;
		default:
			usage(argv[0]);
			break;
		}
	}

	initstate(seed, random_state, sizeof(random_state));

	if (argc < optind + 2)
		usage(argv[0]);

	host = argv[optind++];
	port = intarg(argv[0], argv[optind]);

	fds = malloc(num_connections * sizeof(int));
	if (fds == NULL) {
		perror("malloc");
		exit(1);
	}

	for (i = 0; i < num_connections; i++) {
		fds[i] = clientsock(host, port);
		if (fds[i] < 0) {
			perror(host);
			exit(1);
		}
		if ((i % 25) == 0) {
			printf("\rOpened %d connections", i);
			fflush(stdout);
		}

		set_nodelay(fds[i], 1);
		set_nonblock(fds[i]);
	}
	printf("\rOpened %d connections\n", num_connections);

	fd_in_use = calloc(1, fds[num_connections - 1]);
	if (fd_in_use == NULL) {
		perror("calloc");
		exit(1);
	}

	pthread_mutex_init(&stats_mutex, NULL);
	pthread_mutex_init(&fd_mutex, NULL);

	for (i = 0; i < num_requests; i++)
		pthread_create(&thr, NULL, thread_main, NULL);

	while (iteration_count < num_iterations) {
		poll(NULL, 0, 1000);
		stats();
	}

	stats();
	putchar('\n');
}
