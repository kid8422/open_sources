SRCS = main.c
LIBSRCS = open.c close.c put.c do_recovery.c do_snapshot.c
TARG = kvs_static
TARG_DYNAMIC = kvs_dynamic

CC = gcc
OPTS = -Wall -g -pg
LIBS = -L. -lkvs

OBJS = $(SRCS:.c=.o)
LOBJS = $(LIBSRCS:.c=.o)
LARS = libkvs.a
LSO = libkvs.so


all: 

static: $(TARG)

$(TARG) : $(OBJS) $(LARS)
	$(CC) $(OPTS) -o $(TARG) $(OBJS) $(LIBS) -static

$(LARS) : $(LOBJS)
	ar rs $(LARS) $(LOBJS)

dynamic: $(TARG_DYNAMIC)

$(TARG_DYNAMIC) : $(OBJS) $(LSO)
	$(CC) $(OPTS) -o $(TARG_DYNAMIC) $(OBJS) $(LIBS)

# 동적 라이브러리 생성
$(LSO) : $(LOBJS)
	$(CC) $(OPTS) -shared -o $(LSO) $(LOBJS)

%.o: %.c
	$(CC) $(OPTS) -c $< -o $@

clean:
	rm -f $(OBJS) $(LOBJS) $(LARS) $(LSO) $(TARG) $(TARG_DYNAMIC) kvs.img *.out *.txt


