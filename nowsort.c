/*
 * Authors: Wolf Honore, Victor Liu, Ka Wo Hong
 * Assignment: CSC 258 Project (Spring 2016)
 *
 * Description:
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hdfs.h"

#define KEY_SIZE 10
#define REC_SIZE 100
#define RECORD(data, i) (&data[(i * REC_SIZE)])

/* HDFS functions */
void read_data(hdfsFS, char *, unsigned char *, int);
void write_data(hdfsFS, char *, unsigned char *, int);

/* Sort functions */
void sort(unsigned char *, int);
void quicksort(unsigned char *, int, int);
int partition(unsigned char *, int, int);
int cmp_records(unsigned char *, unsigned char *);

/* MPI variables */
int nprocs;
int rank;

/*
 * Read data from HDFS. 
 */
void read_data(hdfsFS fs, char *path, unsigned char *data, int data_size) {
    hdfsFile data_f = hdfsOpenFile(fs, path, O_RDONLY, 0, 0, 0);

    hdfsRead(fs, data_f, data, data_size);

    hdfsCloseFile(fs, data_f);
}

/*
 * Write data to HDFS.
 */
void write_data(hdfsFS fs, char *path, unsigned char *data, int data_size) {
    hdfsFile data_f = hdfsOpenFile(fs, path, O_WRONLY, 0, 0, 0);

    hdfsWrite(fs, data_f, data, data_size);

    hdfsCloseFile(fs, data_f);
}

/*
 * Compare two records, byte by byte.
 */
int cmp_records(unsigned char *rec1, unsigned char *rec2) {
    int i;
    for (i = 0; i < KEY_SIZE; i++) {
        unsigned char k1 = *(rec1 + i);
        unsigned char k2 = *(rec2 + i);

        if (k1 < k2) {
            return -1;
        }
        else if (k1 > k2) {
            return 1;
        }
    }   

    return 0;
}

/*
 * Sort data in place.
 */
int partition(unsigned char *data, int begin, int end) {
    unsigned char tmp[REC_SIZE];
    unsigned char *pivot = RECORD(data, begin);

    int left = begin;
    int right = end;

    while (left < right) {
        while (cmp_records(RECORD(data, left), pivot) <= 0)
            left++;
        while (cmp_records(RECORD(data, right), pivot) > 0)
            right--;
        if (left < right) { 
            memcpy(tmp, RECORD(data, left), REC_SIZE);
            memcpy(RECORD(data, left), RECORD(data, right), REC_SIZE);
            memcpy(RECORD(data, right), tmp, REC_SIZE);
        }    
    }

    if (begin < right) {
        memcpy(tmp, RECORD(data, begin), REC_SIZE);
        memcpy(RECORD(data, begin), RECORD(data, right), REC_SIZE);
        memcpy(RECORD(data, right), tmp, REC_SIZE);
    }

    return right;
}

/*
 * Recursive quicksort.
 */
void quicksort(unsigned char *data, int begin, int end) {
    int mid;

    if (begin >= end)
        return;

    mid = partition(data, begin, end);
    quicksort(data, begin, mid - 1);
    quicksort(data, mid + 1, end);
}

/*
 * Sort data.
 */
void sort(unsigned char *data, int nrecs) {
    quicksort(data, 0, nrecs - 1);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "nowsort <file> <nrecords>\n");
        exit(1);
    }
    char *in_path = argv[1];
    char *out_path = "out.dat";
    int nrecs = atoi(argv[2]);
    int data_size = nrecs * REC_SIZE;
    unsigned char data[data_size];

    /* Setup */
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    hdfsFS fs = hdfsConnect("default", 0);

    /* Read data */
    read_data(fs, in_path, data, data_size);
    
    /* Sort data */
    sort(data, nrecs);

    /* Write data */
    write_data(fs, out_path, data, data_size);

    /* Cleanup */
    MPI_Finalize();

    hdfsDisconnect(fs);

    return 0;
}
