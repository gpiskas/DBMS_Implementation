#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h> 
#include <unistd.h>

#include "dbtproj.h"
#include "recordOps.h"
#include "bufferOps.h"
#include "fileOps.h"
#include "sortBuffer.h"

/*
 * infile: input filename
 * size: size in blocks of input file
 * outfile: output filename
 * field: which field will be used for sorting
 * buffer: the buffer that is used
 * memSize: number of buffer blocks available for use, without counting the last one, which is for output
 * nunique: number of unique values
 * nios: number of ios
 * 
 * when the input file fits the buffer and there's still a block available for output,
 * hashes each record and writes it to the output, if a record of same value is not
 * found on the corresponding bucket.
 */
void hashElimination(char *infile, uint size, char *outfile, unsigned char field, block_t *buffer, uint memSize, uint *nunique, uint *nios) {
    int out = open(outfile, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
    block_t *bufferOut = buffer + memSize;
    emptyBlock(bufferOut);
    (*bufferOut).valid = true;
    (*bufferOut).blockid = 0;

    (*nunique) = 0;
    (*nios) += readBlocks(infile, buffer, size);

    // creates a hash index. for each value returned from the hash function,
    // there is a linkedList of pointers to the records with that specific hash
    // value
    uint hashSize = size*MAX_RECORDS_PER_BLOCK;
    linkedRecordPtr **hashIndex = (linkedRecordPtr**) malloc(hashSize * sizeof (linkedRecordPtr*));
    for (uint i = 0; i < hashSize; i++) {
        hashIndex[i] = NULL;
    }

    recordPtr start = newPtr(0);
    recordPtr end = newPtr(size * MAX_RECORDS_PER_BLOCK - 1);

    for (; start <= end; incr(start)) {
        if (!buffer[start.block].valid) {
            start.record = MAX_RECORDS_PER_BLOCK - 1;
            continue;
        }
        record_t record = getRecord(buffer, start);
        if (record.valid) {
            // hashes the record being examined
            uint index = hashRecord(infile, record, hashSize, field);
            linkedRecordPtr *element = hashIndex[index];
            // goes through the linked list for the hash value of the record
            // if a record with same value is not found, then a recordPtr is
            // added to the linked list and the record itself is written to
            // the output. otherwise, it is ignored.
            while (element) {
                if (compareRecords(record, getRecord(buffer, element->ptr), field) == 0) {
                    break;
                }
                element = element->next;
            }
            if (!element) {
                element = (linkedRecordPtr*) malloc(sizeof (linkedRecordPtr));
                element->ptr = start;
                element->next = hashIndex[index];
                hashIndex[index] = element;
                (*bufferOut).entries[(*bufferOut).nreserved++] = record;
                (*nunique) += 1;
                if ((*bufferOut).nreserved == MAX_RECORDS_PER_BLOCK) {
                    (*nios) += writeBlocks(out, bufferOut, 1);
                    emptyBlock(bufferOut);
                    (*bufferOut).blockid += 1;
                }
            }
        }
    }
    // writes records left in buffer to the outfile
    if ((*bufferOut).nreserved != 0) {
        (*nios) += writeBlocks(out, bufferOut, 1);
    }
    destroyHashIndex(hashIndex, size);
    close(out);
}

/*
 * infile: filename of the input file
 * outfile: filename of the output file
 * field: which field will be used for sorting
 * buffer: the buffer used
 * nmem_blocks: size of buffer
 * nunique: number of unique values
 * nios: number of ios
 * 
 * when the input file size is equal to buffer, the whole file is loaded and
 * sorted. then the first block is used as output where only unique values are
 * written
 */
void useFirstBlock(char *infile, char *outfile, unsigned char field, block_t *buffer, uint nmem_blocks, uint *nunique, uint *nios) {
    int out = open(outfile, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
    (*nios) += readBlocks(infile, buffer, nmem_blocks);
    if (sortBuffer(buffer, nmem_blocks, field)) {
        // all the unique values of the first block are shifted to the start
        // of it. the rest are marked as invalid
        recordPtr i = newPtr(1);
        recordPtr j = newPtr(1);
        (*nunique) += 1;
        buffer[0].nreserved = 1;
        for (; j.block < 1; incr(j)) {
            record_t record = getRecord(buffer, j);
            if (record.valid && compareRecords(record, getRecord(buffer, i - 1), field) != 0) {
                setRecord(buffer, record, i);
                (*nunique) += 1;
                incr(i);
                buffer[0].nreserved += 1;
            }
        }

        j = newPtr(i, 0);
        for (; j.block < 1; incr(j)) {
            buffer[j.block].entries[j.record].valid = false;
        }

        record_t *lastRecordAdded = (record_t*) malloc(sizeof (record_t));
        record_t lastUnique = getRecord(buffer, i - 1);
        memcpy(lastRecordAdded, &lastUnique, sizeof (record_t));
        // if the first block is full after the shifting (meaning that all its
        // values were actually unique), writes it to the outfile and empties it
        if (buffer[0].nreserved == MAX_RECORDS_PER_BLOCK) {
            i.block -= 1;
            (*nios) += writeBlocks(out, buffer, 1);
            emptyBlock(buffer);
            buffer[0].blockid += 1;
        }

        // write the unique values of the other blocks to the first one. if it
        // becomes full writes it to outfile and empties it. at the end, if it
        // has records not writtend yet, writes them to the outfile as well.
        j = newPtr(MAX_RECORDS_PER_BLOCK);
        while (buffer[j.block].valid && j.block < nmem_blocks) {
            record_t record = getRecord(buffer, j);
            if (!record.valid) {
                break;
            }
            if (compareRecords(record, (*lastRecordAdded), field) != 0) {
                setRecord(buffer, record, i);
                memcpy(lastRecordAdded, &record, sizeof (record_t));
                (*nunique) += 1;
                incr(i);
                buffer[0].nreserved += 1;
            }
            if (buffer[0].nreserved == MAX_RECORDS_PER_BLOCK) {
                i.block -= 1;
                (*nios) += writeBlocks(out, buffer, 1);
                emptyBlock(buffer);
                buffer[0].blockid += 1;
            }
            incr(j);
        }
        if (buffer[0].nreserved != 0) {
            (*nios) += writeBlocks(out, buffer, 1);
        }
        free(lastRecordAdded);
    }
    close(out);
}

// the following code is similar to merge from MergeSort.cpp
// the only difference is that if lastPass is true, then each unique value is
// written only once to the output

uint mergeElimination(int &input, int &output, block_t *buffer, uint memSize, uint segsToMerge, uint *blocksLeft, uint segmentSize, uint firstSegOffset, unsigned char field, bool lastPass, bool lastMergeOfPass, uint *nunique) {
    uint ios = 0;
    block_t *bufferOut = buffer + memSize;
    uint blocksWritten = 0;
    uint sizeOfLastSeg;
    if (lastMergeOfPass) {
        sizeOfLastSeg = blocksLeft[segsToMerge - 1] + 1;
    }
    // holds the last unique value written to the output
    record_t *lastRecordAdded = NULL;

    recordPtr *nextRecord = (recordPtr*) malloc(segsToMerge * sizeof (recordPtr));
    for (uint i = 0; i < segsToMerge; i++) {
        nextRecord[i].block = i;
        nextRecord[i].record = 0;
    }
    emptyBlock(bufferOut);
    (*bufferOut).blockid = 0;

    uint segsToMergeCopy = segsToMerge;
    while (segsToMergeCopy != 0) {
        uint i;
        for (i = 0; i < segsToMerge; i++) {
            if (buffer[i].valid) {
                break;
            }
        }
        record_t minRec = getRecord(buffer, nextRecord[i]);
        uint minBuffIndex = i;

        for (uint j = i + 1; j < segsToMerge; j++) {
            if (buffer[j].valid && compareRecords(getRecord(buffer, nextRecord[j]), minRec, field) < 0) {
                minRec = getRecord(buffer, nextRecord[j]);
                minBuffIndex = j;
            }
        }

        if (!lastPass) {
            (*bufferOut).entries[(*bufferOut).nreserved++] = minRec;
        } else {
            if (!lastRecordAdded) {
                (*bufferOut).entries[(*bufferOut).nreserved++] = minRec;
                (*nunique) += 1;
                lastRecordAdded = (record_t*) malloc(sizeof (record_t));
                memcpy(lastRecordAdded, &minRec, sizeof (record_t));
            } else {
                if (compareRecords(*lastRecordAdded, minRec, field) != 0) {
                    (*bufferOut).entries[(*bufferOut).nreserved++] = minRec;
                    (*nunique) += 1;
                    memcpy(lastRecordAdded, &minRec, sizeof (record_t));
                }
            }
        }

        if ((*bufferOut).nreserved == MAX_RECORDS_PER_BLOCK) {
            ios += writeBlocks(output, bufferOut, 1);
            (*bufferOut).blockid += 1;
            blocksWritten += 1;
            emptyBlock(bufferOut);
        }

        incr(nextRecord[minBuffIndex]);

        if (nextRecord[minBuffIndex].record == 0) {
            nextRecord[minBuffIndex].block -= 1;
            if (blocksLeft[minBuffIndex] > 0) {
                uint blockOffset;
                if (lastMergeOfPass && minBuffIndex == segsToMerge - 1) {
                    blockOffset = firstSegOffset + segmentSize * minBuffIndex + sizeOfLastSeg - blocksLeft[minBuffIndex];
                } else {
                    blockOffset = firstSegOffset + segmentSize * minBuffIndex + segmentSize - blocksLeft[minBuffIndex];
                }
                ios += preadBlocks(input, buffer + minBuffIndex, blockOffset, 1);
                blocksLeft[minBuffIndex] -= 1;
                if (!buffer[minBuffIndex].valid) {
                    segsToMergeCopy -= 1;
                }
            } else {
                buffer[minBuffIndex].valid = false;
                segsToMergeCopy -= 1;
            }
        } else {
            if (!getRecord(buffer, nextRecord[minBuffIndex]).valid) {
                buffer[minBuffIndex].valid = false;
                segsToMergeCopy -= 1;
            }
        }
    }
    free(nextRecord);
    if (lastRecordAdded) {
        free(lastRecordAdded);
    }

    if ((*bufferOut).nreserved != 0) {
        ios += writeBlocks(output, bufferOut, 1);
        (*bufferOut).blockid += 1;
        blocksWritten += 1;
    }

    if (!lastPass && !lastMergeOfPass) {
        for (uint i = 0; i < segmentSize * segsToMerge - blocksWritten; i++) {
            ios += writeBlocks(output, buffer, 1);
        }
    }
    return ios;
}

void EliminateDuplicates(char *infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nunique, unsigned int *nios) {

    if (nmem_blocks < 3) {
        printf("At least 3 blocks are required.");
        return;
    }

    // empties the buffer
    emptyBuffer(buffer, nmem_blocks);
    uint memSize = nmem_blocks - 1;
    *nunique = 0;
    *nios = 0;

    uint fileSize = getSize(infile);

    // if the relation fits on the buffer and leaves one block free for output,
    // loads it to the buffer and eliminates duplicates using hashing
    if (fileSize <= memSize) {
        hashElimination(infile, fileSize, outfile, field, buffer, memSize, nunique, nios);
    } else if (fileSize == nmem_blocks) {
        // if the relation completely fits the buffer, calls useFirstBlock
        useFirstBlock(infile, outfile, field, buffer, nmem_blocks, nunique, nios);
    } else {
        // if the relation is larger than the buffer, then sort it using mergesort,
        // BUT during the final merging (during last pass) write to the output
        // only one time each value

        // the following code is similar to that of MergeSort:

        int input, output;
        char tmpFile1[] = ".ed1";
        char tmpFile2[] = ".ed2";

        uint fullSegments = fileSize / nmem_blocks;
        uint remainingSegment = fileSize % nmem_blocks;

        input = open(infile, O_RDONLY, S_IRWXU);
        output = open(tmpFile1, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);

        uint nSortedSegs = 0;
        uint segmentSize = nmem_blocks;
        for (uint i = 0; i <= fullSegments; i++) {
            if (fullSegments == i) {
                if (remainingSegment != 0) {
                    segmentSize = remainingSegment;
                } else {
                    break;
                }
            }
            (*nios) += readBlocks(input, buffer, segmentSize);
            if (sortBuffer(buffer, segmentSize, field)) {
                (*nios) += writeBlocks(output, buffer, segmentSize);
                nSortedSegs += 1;
            }
        }
        close(input);
        close(output);

        segmentSize = nmem_blocks;
        uint lastSegmentSize;
        if (remainingSegment == 0) {
            lastSegmentSize = nmem_blocks;
        } else {
            lastSegmentSize = remainingSegment;
        }

        buffer[memSize].valid = true;
        while (nSortedSegs != 1) {
            input = open(tmpFile1, O_RDONLY, S_IRWXU);
            output = open(tmpFile2, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);

            uint newSortedSegs = 0;
            uint fullMerges = nSortedSegs / memSize;
            uint lastMergeSegs = nSortedSegs % memSize;
            uint *blocksLeft = (uint*) malloc(memSize * sizeof (uint));
            uint segsToMerge = memSize;
            bool lastMerge = false;

            for (uint mergeCounter = 0; mergeCounter <= fullMerges; mergeCounter++) {
                uint firstSegOffset = mergeCounter * memSize * segmentSize;

                if (mergeCounter == fullMerges - 1 && lastMergeSegs == 0) {
                    lastMerge = true;
                } else if (mergeCounter == fullMerges) {
                    if (lastMergeSegs != 0) {
                        segsToMerge = lastMergeSegs;
                        lastMerge = true;
                    } else {
                        break;
                    }
                }

                for (uint i = 0; i < segsToMerge; i++) {
                    (*nios) += preadBlocks(input, buffer + i, (firstSegOffset + i * segmentSize), 1);
                    blocksLeft[i] = segmentSize - 1;
                }

                if (lastMerge) {
                    blocksLeft[segsToMerge - 1] = lastSegmentSize - 1;
                }

                (*nios) += mergeElimination(input, output, buffer, memSize, segsToMerge, blocksLeft, segmentSize, firstSegOffset, field, nSortedSegs <= memSize, lastMerge, nunique);
                newSortedSegs += 1;
            }
            free(blocksLeft);

            if (lastMergeSegs == 0) {
                lastSegmentSize = (memSize - 1) * segmentSize + lastSegmentSize;
            } else {
                lastSegmentSize = (lastMergeSegs - 1) * segmentSize + lastSegmentSize;
            }
            segmentSize *= memSize;
            nSortedSegs = newSortedSegs;
            close(input);
            close(output);

            char tmp = tmpFile1[3];
            tmpFile1[3] = tmpFile2[3];
            tmpFile2[3] = tmp;
        }
        rename(tmpFile1, outfile);
        remove(tmpFile2);
    }
}