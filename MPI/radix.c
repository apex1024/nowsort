/*******************************************************************************
  radix.c

  Implements NOWsort's distrubtion, in-core sorting, and gathering steps
*******************************************************************************/

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/time.h>
#include <string.h>

#define KEY_SIZE 10
#define REC_SIZE 100
#define MAX_RECORDS_PER_THREAD_PER_BUCKET 100

// 95 for the ASCII input
#define BYTE_VALUES 95

// 33 is the ASCII code of "!", the first valid ASCII character
#define FIRST_BYTE_VALUE 33

int tid;
int num_threads;
int num_records;
char *in_path;
char *out_path;

void parseArgs(int argc, char **argv);
void printUsage();


/***
 * This is the function that maps each record to a target machine.
 *
 * It uses the number of possible values each byte can be (95 for the ASCII
 * case) and assumes the bytes are consecutive values.
 *
 * The number of buckets assigned to each thread is the total possible byte
 * values divided by the number of threads.
 *
 * The assumption here is that our input data is able to fit in the combined
 * memory of all our machines.
 
 * Returns the TID of the target machine.
 */
int getBucket(char *record)
{
    int possible_values = BYTE_VALUES;
    int buckets_per_thread = (possible_values + num_threads - 2) / (num_threads - 1);
    int bucket = (record[0] - FIRST_BYTE_VALUE) / buckets_per_thread;
    return bucket + 1;      // +1 because we don't want the master thread working
}

/***
 * The master thread reads the data, maps the data to threads, sends it out,
 * gathers, and finally writes the data back.
 */
void master()
{
    MPI_Status status;
    int bytes_received;
    
    FILE *fpin;
    FILE *fpout;
    fpin = fopen(in_path, "r");
    fpout = fopen(out_path, "w");
    
    // Read and send out data
    char record[REC_SIZE];
    while (fgets(record, REC_SIZE, fpin)) {
        
        // TODO: probably a better idea to send over the key only
        MPI_Send(record, REC_SIZE, MPI_CHAR, getBucket(record), 0, MPI_COMM_WORLD);
    }
    
    // Tell everyone we're done sending
    char terminate = 0;
    for (int id = 1; id < num_threads; ++id) {
        MPI_Send(&terminate, 1, MPI_CHAR, id, 0, MPI_COMM_WORLD);
    }
    
    // Note: this iteration order matters. TID 1 will contain all the lower
    // sorted keys first
    for (int id = 1; id < num_threads; ++id) {
        while (1) {
            // Told we're done receiving
            MPI_Probe(id, 0, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_CHAR, &bytes_received);
            if (bytes_received == 1) {
                break;
            }
            
            // Receive data from each thread
            MPI_Recv(record, REC_SIZE, MPI_CHAR, id, 0, MPI_COMM_WORLD, &status);
            //printf("%s\n", record);
            
            // maybe we can write to file directly
            fputs(record, fpout);
        }
    }
    
    fclose(fpin);
    fclose(fpout);
}


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

void bubblesort(char records[][REC_SIZE], int length)
{
    char tmp[REC_SIZE];
    int i, j;
    for (i = 0; i < length - 1; ++i)
    {   for (j = 0; j < length - 1 - i; ++j)
        {
            if (cmpRecords(records[j], records[j + 1]) > 0) {
                memcpy(tmp, records[j], REC_SIZE);
                memcpy(records[j], records[j+1], REC_SIZE);
                memcpy(records[j+1], tmp, REC_SIZE);
            }
        }
    }
}


/***
 * Each slave is responsible for sorting within each of its buckets.
 * Note that each slave is expected to have more than 1 bucket.
 *
 * Once the keys are received, we perform a radix-sort on the highest order
 * byte. We then perform a bubblesort (could do quicksort) within each bucket.
 *
 * To scale this, we could radix-sort on more than just the first byte.
 */
void slave()
{   
    int index[BYTE_VALUES];
    for (int i = 0; i < BYTE_VALUES; ++i) index[i] = 0;
    char records[BYTE_VALUES][MAX_RECORDS_PER_THREAD_PER_BUCKET][REC_SIZE];
    
    MPI_Status status;
    int bytes_received;
    char buffer[REC_SIZE];
    
    while (1)
    {
        
        // If we receive 1 byte from the master, it is done broadcasting data
        MPI_Probe(0, 0, MPI_COMM_WORLD, &status);
        MPI_Get_count(&status, MPI_CHAR, &bytes_received);
        if (bytes_received == 1) {
            break;
        }
        
        // Receive the data
        MPI_Recv(buffer, REC_SIZE, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &status);
        
        // Distribute into the appropriate bucket based on the first byte
        char bucket1 = buffer[0];
        memcpy(records[bucket1 - FIRST_BYTE_VALUE][index[bucket1 - FIRST_BYTE_VALUE]], buffer, REC_SIZE);
        
        // Increment each bucket's end pointer
        // Note that once we are done, these values represent the size of each bucket
        ++index[bucket1 - FIRST_BYTE_VALUE];
    }
    
    // Sort within each bucket
    for (int i = 0; i < BYTE_VALUES; ++i) {
        if (index[i] > 1) {
            bubblesort(records[i], index[i]);
        }
    }
    
    // Send the data back
    for (int i = 0; i < BYTE_VALUES; ++i) {
        for (int j = 0; j < index[i]; ++j) {
            MPI_Send(records[i][j], REC_SIZE, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        }
    }
    
    // Done broadcasting
    char terminate = 0;
    MPI_Send(&terminate, 1, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
}

int main(int argc, char **argv)
{   
    parseArgs(argc, argv);

    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &num_threads);
    MPI_Comm_rank(MPI_COMM_WORLD, &tid);

    if (num_threads <= 0 || num_records <= 0) {
        //printUsage();
        exit(0);
    }
    
    if (tid == 0)
    {
        master();
    } else {
        slave();
    }
    
    MPI_Finalize();
    
    return 0;
}

void parseArgs(int argc, char **argv)
{
    int c;
    num_records = 0;
    while ((c = getopt (argc, argv, "n:i:o:")) != -1)
        switch (c)
        {
            case 'n':
                num_records = atoi(optarg);
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

void printUsage()
{
    
}