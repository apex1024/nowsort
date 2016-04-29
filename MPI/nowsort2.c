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
#define BUCKET_SIZE 20
#define MAX_VALUE 512

struct node{
    unsigned char data;
    struct node *next;
};

/* HDFS functions */
void read_data(hdfsFS, char *, unsigned char *, int);
void write_data(hdfsFS, char *, unsigned char *, int);

/* Sort functions */
void sort(struct node **);
struct node *quicksort(struct node *, struct node *);
struct node *partition(struct node *, struct node *,struct node **, struct node **);
int cmp_records(unsigned char *, unsigned char *);

/* Linkedlist functions */
struct node *getTail(struct node *cur);
void push(struct node**, unsigned char *);

/* Bucket functions */
void setup_bucket(struct node **, unsigned char *, int);

/* MPI variables */
int nprocs;
int rank;

/* 
 * Insert a node to bucket
 */
void push(struct node** head_ref, unsigned char *new_data){
    struct node* new_node = (struct node*) malloc(sizeof(struct node));
    new_node->data = *new_data;
    new_node->next = (*head_ref);
    (*head_ref) = new_node;
}

/* 
 * Setup bucket
 */
void setup_bucket(struct node **bucket,  unsigned char *data, int nrecs){
	int no_bucket, i;
	unsigned char temp[REC_SIZE];
	for (i = 0; i < nrecs; i++){
		memcpy(RECORD(data, i), temp, REC_SIZE);
		no_bucket = ((int)temp * (BUCKET_SIZE - 1) / MAX_VALUE); 
		push(&bucket[i], temp);
	}
}

void copy_data(struct node *node,  unsigned char *data, int *count){
    while (node != NULL){
		memcpy(&node->data, RECORD(data, *count), REC_SIZE);
		(*count)++;
        node = node->next;
    }
}
 

/* 
 * Returns the last node of the list 
 */
struct node *getTail(struct node *cur){
    while (cur != NULL && cur->next != NULL)
        cur = cur->next;
    return cur;
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
 
struct node *partition(struct node *head, struct node *end,
                       struct node **newHead, struct node **newEnd)
{
    struct node *pivot = end;
    struct node *prev = NULL, *cur = head, *tail = pivot;
 
    // During partition, both the head and end of the list might change
    // which is updated in the newHead and newEnd variables
    while (cur != pivot)
    {
        if (cur->data < pivot->data)
        {
            // First node that has a value less than the pivot - becomes
            // the new head
            if ((*newHead) == NULL)
                (*newHead) = cur;
 
            prev = cur;  
            cur = cur->next;
        }
        else // If cur node is greater than pivot
        {
            // Move cur node to next of tail, and change tail
            if (prev)
                prev->next = cur->next;
            struct node *tmp = cur->next;
            cur->next = NULL;
            tail->next = cur;
            tail = cur;
            cur = tmp;
        }
    }
 
    // If the pivot data is the smallest element in the current list,
    // pivot becomes the head
    if ((*newHead) == NULL)
        (*newHead) = pivot;
 
    // Update newEnd to the current last node
    (*newEnd) = tail;
 
    // Return the pivot node
    return pivot;
}

/*
 * Recursive quicksort.
 */
struct node *quicksort(struct node *head, struct node *end) {
    struct node *newHead = NULL, *newEnd = NULL;
	// base condition
    if (!head || head == end)
        return head;
 
    
 
    // Partition the list, newHead and newEnd will be updated
    // by the partition function
    struct node *pivot = partition(head, end, &newHead, &newEnd);
 
    // If pivot is the smallest element - no need to recur for
    // the left part.
    if (newHead != pivot)
    {
        // Set the node before the pivot node as NULL
        struct node *tmp = newHead;
        while (tmp->next != pivot)
            tmp = tmp->next;
        tmp->next = NULL;
 
        // Recur for the list before pivot
        newHead = quicksort(newHead, tmp);
 
        // Change next of last node of the left half to pivot
        tmp = getTail(newHead);
        tmp->next =  pivot;
    }
 
    // Recur for the list after the pivot element
    pivot->next = quicksort(pivot->next, newEnd);
 
    return newHead;
}

/*
 * Sort data.
 */
void sort(struct node **headRef) {
	(*headRef) = quicksort(*headRef, getTail(*headRef));
    return;
}

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "nowsort <file> <nrecords>\n");
        exit(1);
    }
    char *in_path = argv[1];
    char *out_path = "out.dat";
    int nrecs = atoi(argv[2]);
    int data_size = nrecs * REC_SIZE;
	int i, count = 0;
    unsigned char data[data_size];
	struct node *bucket[BUCKET_SIZE];

    /* Setup */
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    hdfsFS fs = hdfsConnect("default", 0);
    if (fs == NULL) {
        fprintf(stderr, "%d: failed to connect to HDFS\n", rank);
        MPI_Finalize();
        exit(1);
    }

    /* Read data and setup bucket */
    read_data(fs, in_path, data, data_size);
    
	/* Setup bucket */
	setup_bucket(bucket, data, nrecs);	
	
	for (i = 0; i <BUCKET_SIZE; i++){	
		/* Sort data */	
		sort(&bucket[i]);
		
		/* Copy data back to unsigned char */
		copy_data(bucket[i], data, &count);
	}   

	
	/* Write data */
	write_data(fs, out_path, data, data_size);
		
    /* Cleanup */
    MPI_Finalize();

    hdfsDisconnect(fs);

    return 0;
}
