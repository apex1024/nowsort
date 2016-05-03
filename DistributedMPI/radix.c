/*
 * CSC 258: External Sort Distributed MPI Implementation
 * Victor, Wolf, Ka Wo
 *
 *
 * What you need to know:
 * This program runs on cycle1, cycle2, cycle3, and node2x14a
 * There will be 7 threads:
 * TID 0 to 2 (inclusive) will be run on the cycle machines. These are the HDFS
 * readers.
 * TID 3 to 7 (inclusive) will be the sorters and listeners. TID 3 is node2x14a,
 * TID 4 - 7 is cycle1 - cycle3, respectively.
 *
 * Consult the run script for running.
 * Also, this program will not work unless you have your environment setup.
 * Read the README for more information.
 *
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/time.h>
#include <string.h>
#include "hdfs.h"

#define KEY_SIZE 10
#define REC_SIZE 100

#define BYTE_VALUES 256
#define FIRST_BYTE_VALUE 0

#define READ_BLOCK_SIZE 1000

#define NUM_READERS 3

// Parameters
long num_records;
char *in_path;
char *out_path;

// MPI variables
int tid;
int num_threads;

// Memory variables
unsigned int offset, range, **ptrs;
unsigned char ****storage;

// HDFS variables
hdfsFS hdfs;

// Timing variables
struct timeval begin, start, end;
long t;
double apptime;

void timeIt(struct timeval *which)
{
    gettimeofday(&end, NULL);
    t = (end.tv_sec - which->tv_sec) * 1000000;
    t += end.tv_usec - which->tv_usec;
    apptime = ((double) t) / 1000000.0;
}

int isListener(int tid) {
    return tid >= 3;
}

// To get number of records, input and output files on HDFS
void parseArgs(int argc, char **argv)
{
    int c;
    num_records = 0;
    in_path = "";
    out_path = "";
    while ((c = getopt (argc, argv, "n:i:o:")) != -1)
        switch (c)
        {
            case 'n':
                num_records = atol(optarg);
                break;
            case 'i':
                in_path = optarg;
                break;
            case 'o':
                out_path = optarg;
                break;
            default:
                printf("Error: malformed arguments!\n");
                exit(-1);
				break;
        }
}

// Allocates memory on the sorters / writers
// firstBucketRange is the value of the first possible char on each machine
// maxPerBucket is the total number of records allowed in each bucket
// (We assume distribution is uniform)
void allocate(int firstBucketRange, int maxPerBucket)
{
    ptrs = malloc(firstBucketRange * sizeof(unsigned int *));
    storage = malloc(firstBucketRange * sizeof(unsigned char *));
    for (int i = 0; i < firstBucketRange; ++i) {
        ptrs[i] = malloc(256 * sizeof(unsigned int *));
        storage[i] = malloc(256 * sizeof(unsigned char *));
        for (int j = 0; j < 256; ++j) {
            ptrs[i][j] = 0;
            storage[i][j] = malloc(maxPerBucket * sizeof(unsigned char *));
            for (int k = 0; k < maxPerBucket; ++k) {
                storage[i][j][k] = malloc(100 * sizeof(unsigned char));
            }
        }
    }
}

// Given a record, determines where to send the record based on the first
// byte of the key
int getTargetTID(unsigned char *record, int offset)
{
    if (record[offset] < 51) return 4;
    if (record[offset] < 102) return 5;
    if (record[offset] < 127) return 6;
    return 3;
}

// Add a record into the appropriate bucket based on the first and second bytes
void addToBucket(unsigned char *record)
{
    unsigned char firstChar = record[0];
    unsigned char secondChar = record[1];
    memcpy(storage[firstChar - offset][secondChar][ptrs[firstChar - offset][secondChar]], record, REC_SIZE);
    ++ptrs[firstChar - offset][secondChar];
}


// Run by a reader
void readAndSendRecords()
{
    hdfs = hdfsConnect("default", 0);
    hdfsFile in_file = hdfsOpenFile(hdfs, in_path, O_RDONLY, 0, 0, 0);
    
    // Setup
    unsigned char record[REC_SIZE * READ_BLOCK_SIZE];
    unsigned long bytesRead, bytesRequested, chunk_size;
    chunk_size = ((num_records * REC_SIZE) + NUM_READERS - 1) / NUM_READERS;
    bytesRequested = READ_BLOCK_SIZE;
    
    int bytes_received;
    MPI_Status status;
    
    // Assuming readers are low-order TIDs
    hdfsSeek(hdfs, in_file, tid * chunk_size);
    for (long i = 0; i < chunk_size; i += READ_BLOCK_SIZE)
    {
        // Adjust for overflow of reading bytes
        if (i + READ_BLOCK_SIZE >= chunk_size) {
            bytesRequested = chunk_size - (i - chunk_size);
        }
        bytesRead = hdfsRead(hdfs, in_file, record, REC_SIZE * bytesRequested);
        if (bytesRead <= 0) break;
        
        for (int r = 0; r < bytesRead; r += REC_SIZE)
        {
            // Send to appropriate target
            int target = getTargetTID(record, r);
            MPI_Send(&(record[r]), REC_SIZE, MPI_CHAR, target, 0, MPI_COMM_WORLD);
        }
        
    }
    
    // Send termination signal
    int terminate = 1;
    for (int id = 3; id < 7; ++id) {
        MPI_Send(&terminate, 1, MPI_CHAR, id, 0, MPI_COMM_WORLD);
    }
    
    hdfsCloseFile(hdfs, in_file);
}


// Run by listeners
void listenForRecords()
{
    int flag;
    MPI_Status status;
    int bytes_received;
    unsigned char buffer[REC_SIZE];
    int openChannels = 3;
    
    while (openChannels > 0)
    {
        // probe since we don't know which cycle will send us the next message
        MPI_Probe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        int src = status.MPI_SOURCE;
        MPI_Get_count(&status, MPI_CHAR, &bytes_received);
    
        if (bytes_received == 1) {
            // Termination signal
            MPI_Recv(buffer, 1, MPI_CHAR, src, 0, MPI_COMM_WORLD, &status);
            --openChannels;
        }
        else {
            MPI_Recv(buffer, REC_SIZE, MPI_CHAR, src, 0, MPI_COMM_WORLD, &status);
            addToBucket(buffer);
        }
    }
}


// For comparing two records
int cmpRecords(unsigned char *rec1, unsigned char *rec2) {
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


// Simple bubble-sort
void sort(unsigned char **records, int len)
{
    char tmp[REC_SIZE];
    int i, j;
    for (i = 0; i < len - 1; ++i)
    {   for (j = 0; j < len - 1 - i; ++j)
        {
            if (cmpRecords(records[j], records[j + 1]) > 0) {
                memcpy(tmp, records[j], REC_SIZE);
                memcpy(records[j], records[j+1], REC_SIZE);
                memcpy(records[j+1], tmp, REC_SIZE);
            }
        }
    }
}

void sortRecords()
{
    for (int i = 0; i < range; ++i) {
        for (int j = 0; j < 256; ++j) {
            // For each bucket
            sort(storage[i][j], ptrs[i][j]);
        }
    }
}


// Run by the writers
void writeBackRecords()
{
    
    int ext_id = tid;
    if (tid > 3) ext_id -= 4;
    hdfs = hdfsConnect("default", 0);
    
    // Strings in C! Yay!
    char out_path_append[256];
    char int_ext[20];
    sprintf(int_ext, "%d", ext_id);
    strcpy(out_path_append, out_path);
    strcat(out_path_append, ".");
    strcat(out_path_append, int_ext);
    
    // Each will write to its own file
    hdfsFile out_file = hdfsOpenFile(hdfs, out_path_append, O_WRONLY, 0, 0, 0);
    for (int i = 0; i < range; ++i)
    {   for (int j = 0; j < 256; ++j)
        {   for (int k = 0; k < ptrs[i][j]; ++k)
            {
                hdfsWrite(hdfs, out_file, storage[i][j][k], REC_SIZE);
            }
        }
    }
    hdfsCloseFile(hdfs, out_file);
}

int main(int argc, char **argv)
{  
    gettimeofday(&start, NULL);
    gettimeofday(&begin, NULL);
    
    parseArgs(argc, argv);
    
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &num_threads);
    MPI_Comm_rank(MPI_COMM_WORLD, &tid);
    
    int resultlen;
    char pname[MPI_MAX_PROCESSOR_NAME];
    MPI_Get_processor_name(pname, &resultlen);
    printf("Hello from %s, tid %d\n", pname, tid);

    
    // These values are static and based off of the available memory on each
    // of the machines
    if (isListener) {
        switch (tid) {
            case 4:     // cycle1
                offset = 0;
                range = 51;
                allocate(51, 4000);
                break;
            case 5:     // cycle2
                offset = 51;
                range = 51;
                allocate(51, 4000);
                break;
            case 6:     // cycle3
                offset = 102;
                range = 25;
                allocate(25, 4000);
                break;
            case 3:     // node2x14a
                offset = 127;
                range = 129;
                allocate(129, 4000);
                break;
        }
    }
    
    
    // Wait for all machines to finish allocating memory
    MPI_Barrier(MPI_COMM_WORLD);
    if (tid == 0) {
        timeIt(&start);
        printf("Memory allocation complete (%.6f seconds)\n", apptime);
    }
    
    if (isListener(tid)) {
        gettimeofday(&start, NULL);
        listenForRecords();
        timeIt(&start);
        printf("Thread %d listening complete (%.6f seconds)\n", tid, apptime);
        
        gettimeofday(&start, NULL);
        sortRecords();
        timeIt(&start);
        printf("Thread %d sorting complete (%.6f seconds)\n", tid, apptime);
        
        gettimeofday(&start, NULL);
        writeBackRecords();
        timeIt(&start);
        printf("Thread %d writing complete (%.6f seconds)\n", tid, apptime);
    }
    else {
        readAndSendRecords();
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    if (tid == 0) {
        timeIt(&begin);
        printf("Total app time: %.6f seconds\n", tid, apptime);
    }
    
    MPI_Finalize();

    return 0;
}