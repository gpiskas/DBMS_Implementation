/*
 *  ==========================================================================================
 *  Database Technology 2012-2013
 *  Header file dbtproj.h to be used by your code.
 *  It is not permitted to change this file. You can make changes to perform tests, 
 *  but please rely on the values and typedefs shown below.
 * 
 *  ==========================================================================================
 *
 */


#ifndef _DBTPROJ_H
#define _DBTPROJ_H

#define STR_LENGTH 120
#define MAX_RECORDS_PER_BLOCK 100

// This is the definition of a record. Contains three fields, recid, num and str

typedef struct {
    unsigned int recid;
    unsigned int num;
    char str[STR_LENGTH];
    bool valid; // if set, then this record is valid
} record_t;


// This is the definition of a block, which contains a number of fixed-sized records

typedef struct {
    unsigned int blockid;
    unsigned int nreserved; // how many reserved entries
    record_t entries[MAX_RECORDS_PER_BLOCK]; // array of records
    bool valid; // if set, then this block is valid
    unsigned char misc;
    unsigned int next_blockid;
    unsigned int dummy;
} block_t;


// Functions that must be implemented in your code


/* ----------------------------------------------------------------------------------------------------------------------
   infile: the name of the input file
   field: which field will be used for sorting: 0 is for recid, 1 is for num, 2 is for str and 3 is for both num and str
   buffer: pointer to memory buffer
   nmem_blocks: number of blocks in memory
   outfile: the name of the output file
   nsorted_segs: number of sorted segments produced (this should be set by you)
   npasses: number of passes required for sorting (this should be set by you)
   nios: number of IOs performed (this should be set by you)
   ----------------------------------------------------------------------------------------------------------------------
 */
void MergeSort(char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nsorted_segs, unsigned int *npasses, unsigned int *nios);


/* ----------------------------------------------------------------------------------------------------------------------
   infile: the name of the input file
   field: which field will be used for sorting: 0 is for recid, 1 is for num, 2 is for str and 3 is for both num and str
   buffer: pointer to memory buffer
   nmem_blocks: number of blocks in memory
   outfile: the name of the output file 
   nunique: number of unique records in file (this should be set by you)
   nios: number of IOs performed (this should be set by you)
   ----------------------------------------------------------------------------------------------------------------------
 */
void EliminateDuplicates(char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nunique, unsigned int *nios);


/* ----------------------------------------------------------------------------------------------------------------------
   infile1: the name of the first input file
   infile2: the name of the second input file
   field: which field will be used for the join: 0 is for recid, 1 is for num, 2 is for str and 3 is for both num and str
   buffer: pointer to memory buffer
   nmem_blocks: number of blocks in memory
   outfile: the name of the output file
   nres: number of pairs in output (this should be set by you)
   nios: number of IOs performed (this should be set by you)
   ----------------------------------------------------------------------------------------------------------------------
 */
void MergeJoin(char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios);


/* ----------------------------------------------------------------------------------------------------------------------
   infile1: the name of the first input file
   infile2: the name of the second input file
   field: which field will be used for the join: 0 is for recid, 1 is for num, 2 is for str and 3 is for both num and str
   buffer: pointer to memory buffer
   nmem_blocks: number of blocks in memory
   outfile: the name of the output file
   nres: number of pairs in output (this should be set by you)
   nios: number of IOs performed (this should be set by you)
   ----------------------------------------------------------------------------------------------------------------------
 */
void HashJoin(char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios);


#endif
