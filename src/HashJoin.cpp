#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h> 
#include <unistd.h>
#include <string.h>
#include <vector>

#include "dbtproj.h"
#include "recordOps.h"
#include "bufferOps.h"
#include "fileOps.h"

/*
 * seed: seed to use in hash function
 * buffer: buffer used, already loaded with a relation to hash
 * size: the size in blocks of the relation loaded on buffer
 * field: which field will be used for joining
 * 
 * returns the pointer to the hash index
 */
linkedRecordPtr** createHashIndex(char *seed, block_t *buffer, uint size, unsigned char field) {
    // the hash index consists of a maximum of hashSize linked lists where
    // each list has pointers to the records with common hash value

    // the size of hashIndex
    uint hashSize = size * MAX_RECORDS_PER_BLOCK;
    linkedRecordPtr **hashIndex = (linkedRecordPtr**) malloc(hashSize * sizeof (linkedRecordPtr*));
    for (uint i = 0; i < hashSize; i++) {
        hashIndex[i] = NULL;
    }

    recordPtr start = newPtr(0);
    recordPtr end = newPtr(size * MAX_RECORDS_PER_BLOCK - 1);

    // starting from the very first record, all valid records in valid blocks
    // are hashed
    for (; start <= end; incr(start)) {
        if (!buffer[start.block].valid) {
            start.record = MAX_RECORDS_PER_BLOCK - 1;
            continue;
        }
        record_t record = getRecord(buffer, start);
        if (record.valid) {
            uint index = hashRecord(seed, record, hashSize, field);
            linkedRecordPtr *ptr = (linkedRecordPtr*) malloc(sizeof (linkedRecordPtr));
            ptr->ptr = start;
            ptr->next = hashIndex[index];
            hashIndex[index] = ptr;
        }
    }
    // returns hashIndex
    return hashIndex;
}

/*
 * infile: filename of the file whose records will be joined with the ones on buffer
 * inBlocks: size of infile
 * buffer: the buffer that is used (a file is already loaded on it)
 * nmem_blocks: size of buffer
 * size: the size of the file already loaded on buffer
 * out: file descriptor of the outfile
 * nres: number of pairs
 * nios: number of ios
 * field: which field will be used for joining
 */
void hashAndProbe(char *infile, uint inBlocks, block_t *buffer, uint nmem_blocks, uint size, int &out, uint *nres, uint *nios, unsigned char field) {
    // hash index for the records already on buffer is created
    linkedRecordPtr **hashIndex = createHashIndex(infile, buffer, size, field);
    // pointer to the buffer block where blocks of infile are loaded
    block_t *bufferIn = buffer + nmem_blocks - 2;
    // pointer to the last buffer block, where pairs for output are written
    block_t *bufferOut = buffer + nmem_blocks - 1;

    int in = open(infile, O_RDONLY, S_IRWXU);
    for (uint i = 0; i < inBlocks; i++) {
        // if the block loaded is invalid, loads the next one
        (*nios) += readBlocks(in, bufferIn, 1);
        if (!(*bufferIn).valid) {
            continue;
        }
        // each record of the loaded block is hashed
        // then the linked list of the hash index for the corresponding hash value
        // is examined, and if a record has same value as the current one, both
        // are written to the output block
        for (int j = 0; j < MAX_RECORDS_PER_BLOCK; j++) {
            record_t record = (*bufferIn).entries[j];
            if (record.valid) {
                uint index = hashRecord(infile, record, size*MAX_RECORDS_PER_BLOCK, field);
                linkedRecordPtr *element = hashIndex[index];
                while (element) {
                    record_t tmp = getRecord(buffer, element->ptr);
                    if (compareRecords(record, tmp, field) == 0) {
                        (*bufferOut).entries[(*bufferOut).nreserved++] = record;
                        (*bufferOut).entries[(*bufferOut).nreserved++] = tmp;
                        (*nres) += 1;
                        // if output block becomes full, writes it to the outfile
                        // and empties it
                        if ((*bufferOut).nreserved == MAX_RECORDS_PER_BLOCK) {
                            (*nios) += writeBlocks(out, bufferOut, 1);
                            emptyBlock(bufferOut);
                            (*bufferOut).blockid += 1;
                        }
                    }
                    element = element->next;
                }
            }
        }
    }
    close(in);
    destroyHashIndex(hashIndex, size);
}

/*
 * filename: the name of the file to be partitioned
 * size: the size of the file
 * seed: a seed for the hash function
 * buffer: the buffer that is used
 * nmem_blocks: size of buffer
 * bucketFilenames: array with the filenames of the bucket files to be produced
 * mod: to be used for hashing
 * nios: number of ios
 * field: which field will be used for joining
 */
void createBucketFiles(char* filename, uint size, char* seed, block_t *buffer, uint nmem_blocks, char **bucketFilenames, uint mod, uint *nios, unsigned char field) {
    // each block of the infile is loaded on the last block of buffer and each of its
    // records is hashed to one of the other buffer blocks. if a buffer block
    // becomes full, it is written to the correspoding bucket file

    // pointer to the last block of buffer, for convenience
    block_t *bufferIn = buffer + nmem_blocks - 1;
    int file = open(filename, O_RDONLY, S_IRWXU);
    for (uint i = 0; i < size; i++) {
        // if the block loaded is invalid, loads the next one
        (*nios) += readBlocks(file, bufferIn, 1);
        if (!(*bufferIn).valid) {
            continue;
        }
        // each record of the current block is hashed
        for (int j = 0; j < MAX_RECORDS_PER_BLOCK; j++) {
            record_t record = (*bufferIn).entries[j];
            if (record.valid) {
                uint index = hashRecord(seed, record, mod, field);
                buffer[index].entries[buffer[index].nreserved++] = record;
                // if a buffer block becomes full, writes it to the corresponding
                // bucket file
                if (buffer[index].nreserved == MAX_RECORDS_PER_BLOCK) {
                    (*nios) += writeBlocks(bucketFilenames[index], buffer + index, 1);
                    emptyBlock(buffer + index);
                }
            }
        }
    }

    // if any block has records left, writes them to the corresponding file
    for (uint i = 0; i < nmem_blocks - 1; i++) {
        if (buffer[i].nreserved != 0) {
            (*nios) += writeBlocks(bucketFilenames[i], buffer + i, 1);
            emptyBlock(buffer + i);
        }
    }
    close(file);
}

// using the infile's name, generates the name of its bucket file and returns it

inline char* extendFilename(const char* parentFilename, uint i) {
    char* str = (char*) malloc((strlen(parentFilename) + 11) * sizeof (char));
    sprintf(str, "%s_%u", parentFilename, i);
    return str;
}

/*
 * infile1: the first relation, or a part of it
 * infile2: the first relation, or a part of it
 * field: which field will be used for joining
 * buffer: the buffer that is used
 * memSize: size of buffer minus output spot
 * nres: number of pairs
 * nios: number of ios
 * firstCall: true if partition is called for the first time, meaning infile1 and infile2 are the original files
 * filenames: vector that holds the filenames of files that can be joined in a single pass
 */
void partition(char *infile1, char *infile2, unsigned char field, block_t *buffer, uint memSize, uint *nres, uint *nios, bool firstCall, std::vector<char*> &filenames) {
    uint size1 = getSize(infile1);
    uint size2 = getSize(infile2);

    // if either of the infiles fits in nmem_blocks - 2,
    // then they can be joined in a single pass so pushes the filenames
    // on the vector. the filename of the smaller file is pushed first
    if (size1 < memSize || size2 < memSize) {
        if (size1 <= size2) {
            filenames.push_back(infile1);
            filenames.push_back(infile2);
        } else {
            filenames.push_back(infile2);
            filenames.push_back(infile1);
        }
    } else {
        // if the infiles cannot be joined in a single pass, creates subfiles (bucketfiles)
        // and recursively calls partition for each pair of them

        // figure out how many buckets to create
        uint smallSize = size1;
        if (size2 < smallSize) {
            smallSize = size2;
        }
        uint bucketCount = smallSize / (memSize - 1);
        if (smallSize % (memSize - 1)) {
            bucketCount += 1;
        }
        if (bucketCount > memSize) {
            bucketCount = memSize;
        }
        
        // arrays with the filenames for the subfiles to be produced
        char **bucketFilenames1 = (char**) malloc(bucketCount * sizeof (char*));
        char **bucketFilenames2 = (char**) malloc(bucketCount * sizeof (char*));

        if (firstCall) {
            for (uint i = 0; i < bucketCount; i++) {
                bucketFilenames1[i] = extendFilename(".hj1", i);
                bucketFilenames2[i] = extendFilename(".hj2", i);
            }
        } else {
            for (uint i = 0; i < bucketCount; i++) {
                bucketFilenames1[i] = extendFilename(infile1, i);
                bucketFilenames2[i] = extendFilename(infile2, i);
            }
        }
        // calls createBucketFiles for infile1
        createBucketFiles(infile1, size1, infile1, buffer, memSize + 1, bucketFilenames1, bucketCount, nios, field);
        // after the files are created, removes infile1 if it's not the original one
        if (!firstCall) {
            remove(infile1);
        }
        // same for infile2
        createBucketFiles(infile2, size2, infile1, buffer, memSize + 1, bucketFilenames2, bucketCount, nios, field);
        if (!firstCall) {
            remove(infile2);
            free(infile1);
            free(infile2);
        }
        // for each pair of bucket files, if both of them exist, partition is called.
        // otherwise they are both removed.
        for (uint i = 0; i < bucketCount; i++) {
            if (!exists(bucketFilenames1[i]) || !exists(bucketFilenames2[i])) {
                remove(bucketFilenames1[i]);
                remove(bucketFilenames2[i]);
                free(bucketFilenames1[i]);
                free(bucketFilenames2[i]);
            } else {
                partition(bucketFilenames1[i], bucketFilenames2[i], field, buffer, memSize, nres, nios, false, filenames);
            }
        }
        // memory allocated for the arrays with the bucket filenames is freed
        free(bucketFilenames1);
        free(bucketFilenames2);
    }
}

void HashJoin(char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios) {
    if (nmem_blocks < 3) {
        printf("At least 3 blocks are required.");
        return;
    }
    system("rm .hj* -f");
    emptyBuffer(buffer, nmem_blocks);

    (*nres) = 0;
    (*nios) = 0;

    // pointer to the last block of buffer, for convenience
    block_t *bufferOut = buffer + nmem_blocks - 1;
    // vector that holds pairs of filenames  of files where at least one
    // of them fits on nmem_blocks - 2 blocks. each pair will be joined
    // using single-pass hashing
    std::vector<char*> filenames;
    // partitions the original files in smaller ones that can be joined in as single pass
    partition(infile1, infile2, field, buffer, nmem_blocks - 1, nres, nios, true, filenames);
    emptyBlock(bufferOut);
    (*bufferOut).valid = true;
    (*bufferOut).blockid = 0;

    int out = open(outfile, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
    if (filenames.size() != 0) {
        // joins the pairs of files and the writes the pairs on the outfile
        for (uint i = 0; i < filenames.size() - 1; i += 2) {
            uint size1 = getSize(filenames[i]);
            (*nios) += readBlocks(filenames[i], buffer, size1);

            hashAndProbe(filenames[i + 1], getSize(filenames[i + 1]), buffer, nmem_blocks, size1, out, nres, nios, field);

            // if the files joined are not the original ones, remove them and free
            // memory allocated for their names
            if (!strcmp(filenames[i], infile1) == 0 && !strcmp(filenames[i + 1], infile1) == 0) {
                remove(filenames[i]);
                remove(filenames[i + 1]);
                free(filenames[i]);
                free(filenames[i + 1]);
            }
        }
        filenames.clear();
        // if there are pairs left on the buffer, writes them to the output
        if ((*bufferOut).nreserved != 0) {
            (*nios) += writeBlocks(out, bufferOut, 1);
        }
    }
    close(out);
}