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
 * input: file descriptor to the input file with the segments for merging
 * output: file descriptor to the output file where the one sorted segment to be produced will be written
 * buffer: the buffer used. the first blocks of each segment are already loaded
 * memSize: number of blocks in buffer to be used for merging. eg if memSize = 2, 2-way merge is used
 * segsToMerge: number of segments to merge. most times it will be equal to memSize.
 * blocksLeft: array that stores the number of blocks not yet loaded on buffer for each segment
 * segmentSize: the size of each segment in blocks. if that is the last merge of the pass, last segment may have fewer blocks
 * firstSegOffset: the offset of the first segment in the input file
 * field: which field will be used for sorting
 * lastPass: true if this is the last pass, meaning that after this merge the output file will be fully sorted
 * lastMergeOfPass: true if this is the last merge of the current pass
 
 * returns the number of ios done during merge
 */
uint merge(int &input, int &output, block_t *buffer, uint memSize, uint segsToMerge, uint *blocksLeft, uint segmentSize, uint firstSegOffset, unsigned char field, bool lastPass, bool lastMergeOfPass) {

    uint ios = 0;
    // pointer to the last block of buffer, for convenience
    block_t *bufferOut = buffer + memSize;
    // number of blocks written to the output file during this merge
    uint blocksWritten = 0;
    // if that's the last merge of the current pass, last segment may have less than segmentSize blocks
    uint sizeOfLastSeg;
    if (lastMergeOfPass) {
        sizeOfLastSeg = blocksLeft[segsToMerge - 1] + 1;
    }

    // array of recordPtrs, one for each segment, that shows to the next record of the segment that is to be merged
    recordPtr *nextRecord = (recordPtr*) malloc(segsToMerge * sizeof (recordPtr));
    for (uint i = 0; i < segsToMerge; i++) {
        nextRecord[i] = newPtr(i * MAX_RECORDS_PER_BLOCK);
    }
    emptyBlock(bufferOut);
    (*bufferOut).blockid = 0;

    uint segsToMergeCopy = segsToMerge;
    while (segsToMergeCopy != 0) {
        uint i;
        // finds the first valid block and setting its record as min, so that there is a value to compare to later
        for (i = 0; i < segsToMerge; i++) {
            if (buffer[i].valid) {
                break;
            }
        }
        record_t minRec = getRecord(buffer, nextRecord[i]);
        uint minBuffIndex = i;

        // compares the previously found min with the next records of each segment and
        // finds the record with minimum value
        for (uint j = i + 1; j < segsToMerge; j++) {
            if (buffer[j].valid && compareRecords(getRecord(buffer, nextRecord[j]), minRec, field) < 0) {
                minRec = getRecord(buffer, nextRecord[j]);
                minBuffIndex = j;
            }
        }

        // min record is written to the last block, which is used as output
        (*bufferOut).entries[(*bufferOut).nreserved++] = minRec;

        // if the last block is full, write it to the outfile and empty it
        if ((*bufferOut).nreserved == MAX_RECORDS_PER_BLOCK) {
            ios += writeBlocks(output, bufferOut, 1);
            (*bufferOut).blockid += 1;
            blocksWritten += 1;
            emptyBlock(bufferOut);
        }

        // increases the recordPtr of the segment whose record was written
        // to the last block
        incr(nextRecord[minBuffIndex]);

        // if the current block of that segment is over, loads the next one
        // if there is one left or set it as invalid
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

    // after all segments are done, if there are records on the last block,
    // writes them on the output
    if ((*bufferOut).nreserved != 0) {
        ios += writeBlocks(output, bufferOut, 1);
        (*bufferOut).blockid += 1;
        blocksWritten += 1;
    }

    // if that was not the last pass or the last merge of the current pass,
    // adds to the output file dummy blocks, so that the offset of the sorted
    // segments can be calculated on the next passes
    // that is needed in case the blocks are not 100% utilised
    if (!lastPass && !lastMergeOfPass) {
        for (uint i = 0; i < (segmentSize * segsToMerge - blocksWritten); i++) {
            ios += writeBlocks(output, buffer, 1);
        }
    }
    // return the number of ios done during this merge
    return ios;
}

void MergeSort(char* infile, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char* outfile, unsigned int* nsorted_segs, unsigned int* npasses, unsigned int* nios) {

    if (nmem_blocks < 3) {
        printf("At least 3 blocks are required.");
        return;
    }

    // empties the buffer
    emptyBuffer(buffer, nmem_blocks);

    uint memSize = nmem_blocks - 1;
    int input, output;
    char tmpFile1[] = ".ms1";
    char tmpFile2[] = ".ms2";

    (*nsorted_segs) = 0;
    (*npasses) = 0;
    (*nios) = 0;

    uint infileBlocks = getSize(infile);

    // # of segments that completely fill the buffer
    uint fullSegments = infileBlocks / nmem_blocks;
    // # of blocks the last segment will have in case it doesn't fill the buffer
    // completely
    uint remainingSegment = infileBlocks % nmem_blocks;

    input = open(infile, O_RDONLY, S_IRWXU);
    output = open(tmpFile1, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);

    // sorts each segment in memory, then writes it to ".ms1"
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
            (*nsorted_segs) += 1;
        }
    }
    (*npasses) += 1;
    close(input);
    close(output);


    // # of blocks each sorted segment has (with the exception of the last segment)
    segmentSize = nmem_blocks;
    // # of blocks the last sorted segment will have in case it didn't fill the buffer completely
    uint lastSegmentSize;
    if (remainingSegment == 0) {
        lastSegmentSize = nmem_blocks;
    } else {
        lastSegmentSize = remainingSegment;
    }


    // two intermediate files, ".ms1" and ".ms2" are being used, the one as
    // input and the other as output. after a pass is over, they switch roles.
    // at the end, the sorted file, whether it is ".ms1" or ".ms2", is renamed
    // to outfile while the other one is deleted
    // the outfile will always be 100% utilised (with the possible exception of
    // the last block), meaning that it may be smaller than the infile
    buffer[memSize].valid = true;
    uint nSortedSegs = (*nsorted_segs);
    while (nSortedSegs > 1) {
        input = open(tmpFile1, O_RDONLY, S_IRWXU);
        output = open(tmpFile2, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);
        uint newSortedSegs = 0;
        // # of merges that utilise the buffer completely (memSize-way merge)
        uint fullMerges = nSortedSegs / memSize;
        // # of sorted segments the last merge will merge in case it doesn't
        // utilise the buffer completely
        uint lastMergeSegs = nSortedSegs % memSize;
        // array that holds the number of blocks left to a sorted segment
        // during merging
        uint *blocksLeft = (uint*) malloc(memSize * sizeof (uint));

        uint segsToMerge = memSize;
        bool lastMerge = false;
        for (uint mergeCounter = 0; mergeCounter <= fullMerges; mergeCounter++) {
            uint firstSegOffset = mergeCounter * memSize * segmentSize;

            if (mergeCounter == fullMerges - 1 && lastMergeSegs == 0) {
                lastMerge = true;
            } else if (mergeCounter == fullMerges) {
                // if during the last merge the buffer is not fully utilised,
                // changes the numbers of segsToMerge and sets lastMerge as true
                if (lastMergeSegs != 0) {
                    segsToMerge = lastMergeSegs;
                    lastMerge = true;
                } else {
                    break;
                }
            }

            // loads the first block of each segment to merge on the buffer
            for (uint i = 0; i < segsToMerge; i++) {
                (*nios) += preadBlocks(input, buffer + i, (firstSegOffset + i * segmentSize), 1);
                blocksLeft[i] = segmentSize - 1;
            }

            // if that's the last merge of the current pass, the last segment may have less blocks
            if (lastMerge) {
                blocksLeft[segsToMerge - 1] = lastSegmentSize - 1;
            }

            (*nios) += merge(input, output, buffer, memSize, segsToMerge, blocksLeft, segmentSize, firstSegOffset, field, nSortedSegs <= memSize, lastMerge);
            newSortedSegs += 1;
        }
        free(blocksLeft);

        // updates variables for the next pass
        if (lastMergeSegs == 0) {
            lastSegmentSize = (memSize - 1) * segmentSize + lastSegmentSize;
        } else {
            lastSegmentSize = (lastMergeSegs - 1) * segmentSize + lastSegmentSize;
        }
        segmentSize *= memSize;
        nSortedSegs = newSortedSegs;
        (*npasses) += 1;
        close(input);
        close(output);

        // swaps the files e.g if during this pass ".ms1" was used as input, next
        // pass it will be used as output
        char tmp = tmpFile1[3];
        tmpFile1[3] = tmpFile2[3];
        tmpFile2[3] = tmp;
    }
    rename(tmpFile1, outfile);
    remove(tmpFile2);
}