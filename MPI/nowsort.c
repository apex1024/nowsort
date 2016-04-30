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
#include <stddef.h>
#include "hdfs.h"

#define KEY_SIZE 10
#define REC_SIZE 100
#define RECORD(data, i) (&data[(i * REC_SIZE)])
#define BUCKET_SIZE 16
#define NBITS 4
#define MAX_VALUE 512

/* HDFS functions */
void read_data(hdfsFS, char *, unsigned char *, int);
void write_data(hdfsFS, char *, unsigned char *, int);

/* Defining a bucket */
typedef struct{
    int index;
	unsigned char * array;
}Bucket;

/* Sort functions */
void sort(unsigned char *, int);
void quicksort(unsigned char *, int, int);
int partition(unsigned char *, int, int);
int cmp_records(unsigned char *, unsigned char *);

/* Bucket functions */
void bucket_insert(Bucket *, unsigned char*);
void init_bucket(Bucket *, int, unsigned char *);

/* MPI variables */
int nprocs;
int rank;

/* 
 * Initialize all buckets
 */
void init_bucket(Bucket *b, int nrecs, unsigned char *data){
	int i, no_bucket;
	unsigned char temp[REC_SIZE];

    for(i = 0; i < BUCKET_SIZE; i++){
        b[i].array = (unsigned char*) malloc(sizeof(unsigned char) * nrecs * REC_SIZE);
        b[i].index = 0;
    }
	for (i = 0; i < nrecs; i++){
		memcpy(temp, RECORD(data, i), REC_SIZE);
		no_bucket = (int) *temp >> (8 - NBITS);
		memcpy(RECORD(b[no_bucket].array, b[no_bucket].index), temp, REC_SIZE);
		b[no_bucket].index = b[no_bucket].index +1;
	}
}

/*
 * Read data from HDFS. 
 */
void read_data(hdfsFS fs, char *path, unsigned char *data, int data_size) {
    hdfsFile data_f = hdfsOpenFile(fs, path, O_RDONLY, 0, 0, 0);
    if (data_f == NULL) {
        fprintf(stderr, "%d: failed to open %s for reading\n", rank, path);
        MPI_Finalize();
        exit(1);
    }

    hdfsRead(fs, data_f, data, data_size);

    hdfsCloseFile(fs, data_f);
}

/*
 * Write data to HDFS.
 */
void write_data(hdfsFS fs, char *path, unsigned char *data, int data_size) {
    hdfsFile data_f = hdfsOpenFile(fs, path, O_WRONLY, 0, 0, 0);
    if (data_f == NULL) {
        fprintf(stderr, "%d: failed to open %s for writing\n", rank, path);
        MPI_Finalize();
        exit(1);
    }

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
    if (argc < 3) {
        fprintf(stderr, "nowsort <file> <nrecords>\n");
        exit(1);
    }
	int i, j, count = 0;
    char *in_path = argv[1];
    char *out_path = "out.dat";
    int nrecs = atoi(argv[2]);
    int data_size = nrecs * REC_SIZE;
    unsigned char data[data_size];
	Bucket buckets[BUCKET_SIZE];
	int *begin, *end;
	MPI_Status status;
	
    /* Setup */
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	begin = (int*)malloc(nprocs * sizeof(int));
	end = (int*)malloc(nprocs * sizeof(int));
	for (i = 0; i < nprocs; i++){	
		begin[i] = i * BUCKET_SIZE/nprocs;
		end[i] = (i + 1) * BUCKET_SIZE/nprocs - 1;
	}
	
    hdfsFS fs = hdfsConnect("default", 0);
    if (fs == NULL) {
        fprintf(stderr, "%d: failed to connect to HDFS\n", rank);
        MPI_Finalize();
        exit(1);
    }
	
	/* Read data */
	read_data(fs, in_path, data, data_size);
	
    /* Initialize buckets */
	init_bucket(buckets, nrecs, data);
	

	/* distribut data */
	if (rank != 0){
		for (i = begin[rank]; i <= end[rank]; i++){
			MPI_Recv(&buckets[i].array[0], data_size, MPI_UNSIGNED_CHAR , 0, 0,MPI_COMM_WORLD, &status);
			MPI_Recv(&buckets[i].index, 1, MPI_INT , 0, 0,MPI_COMM_WORLD, &status);
		}
	}else{
		for (i = 1; i < nprocs; i++){
			for (j = begin[i]; j <= end[i]; j++){
				MPI_Send(&buckets[j].array[0], data_size, MPI_UNSIGNED_CHAR , i, 0,MPI_COMM_WORLD);
				MPI_Send(&buckets[j].index, 1, MPI_INT , i, 0,MPI_COMM_WORLD);
			}
		}
	}

    /* Sort data */
	for (i = begin[rank]; i<= end[rank]; i++)
		sort(buckets[i].array, buckets[i].index);
	
	/* Gather the data*/
	if (rank != 0){
		for (i = begin[rank]; i <= end[rank]; i++){
			MPI_Send(&buckets[i].array[0], data_size, MPI_UNSIGNED_CHAR , 0, 0,MPI_COMM_WORLD);
			MPI_Send(&buckets[i].index, 1, MPI_INT , 0, 0,MPI_COMM_WORLD);
		}
	}else{
		for (i = 1; i < nprocs; i++){
			for (j = begin[i]; j <= end[i]; j++){
				MPI_Recv(&buckets[j].array[0], data_size, MPI_UNSIGNED_CHAR , i, 0,MPI_COMM_WORLD, &status);
				MPI_Recv(&buckets[j].index, 1, MPI_INT , i, 0,MPI_COMM_WORLD, &status);
			}
		}
	}
	
	if (rank == 0){
		 for (i = 0; i< BUCKET_SIZE; i++){
			memcpy(RECORD(data, count), RECORD(buckets[i].array, 0), REC_SIZE * buckets[i].index);
			count += buckets[i].index;
		}
		/* Write data */
		write_data(fs, out_path, data, data_size);
	}
    /* Cleanup */
    MPI_Finalize();

    hdfsDisconnect(fs);

    return 0;
}
