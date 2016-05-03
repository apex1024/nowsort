HADOOP_HOME = /u/cs258/hadoop

CC = mpicc
LDFLAGS = -I $(HADOOP_HOME)/include -L $(HADOOP_HOME)/lib/native -lhdfs -L /usr/java/lib

all: radix.c
	$(CC) $(LDFLAGS) -o radix radix.c
clean:
	rm -f radix simple