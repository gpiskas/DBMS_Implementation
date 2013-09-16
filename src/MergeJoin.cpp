#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h> 
#include <unistd.h>

#include "dbtproj.h"
#include "recordOps.h"
#include "bufferOps.h"
#include "fileOps.h"
#include "sortBuffer.h"

// struct that holds the last value joined (the whole record is stored but only
// the value of a field is needed) and the blockId of the block this value
// was first found

struct blockToLoad {
    uint blockId;
    record_t *lastValueJoined;
};

// called if at least one of the files fits in nmem_blocks - 2.
// sorts one file using MergeSort, while the other is loaded on buffer and sorted there

void fitCase(char *infile1, char *infile2, unsigned char field, block_t *buffer, uint nmem_blocks, char *outfile, uint *nres, uint *nios) {

    uint memSize = nmem_blocks - 1;
    uint fileSize1 = getSize(infile1);
    uint fileSize2 = getSize(infile2);

    // file1 is the one loaded on the buffer
    // file2 is the one that is sorted using MergeSort and one block
    // of the sorted file is loaded on buffer at a time
    char *file1, *file2;
    // size of the file to be loaded on buffer
    uint memSize1;

    // if both fit in memSize - 1 blocks, file1 is the larger one,
    // else file1 is the smaller of them
    if (fileSize1 < memSize && fileSize2 < memSize) {
        if (fileSize1 >= fileSize2) {
            file1 = infile1;
            file2 = infile2;
            memSize1 = fileSize1;
        } else {
            file1 = infile2;
            file2 = infile1;
            memSize1 = fileSize2;
        }
    } else {
        if (fileSize1 < fileSize2) {
            file1 = infile1;
            file2 = infile2;
            memSize1 = fileSize1;
        } else {
            file1 = infile2;
            file2 = infile1;
            memSize1 = fileSize2;
        }
    }

    uint ios, dummy1, dummy2;
    char tmpFile[] = ".mj";

    // sorts file2 using mergesort
    MergeSort(file2, field, buffer, nmem_blocks, tmpFile, &dummy1, &dummy2, &ios);
    (*nios) += ios;

    uint tmpFileSize = getSize(tmpFile);

    // the whole file1 is loaded on buffer
    (*nios) += readBlocks(file1, buffer, memSize1);

    int out = open(outfile, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);

    // if file on buffer has valid records and the sorted one has at least one block...
    if (sortBuffer(buffer, memSize1, field) && tmpFileSize != 0) {
        // recordPtr pointing to the last valid record of file1 on the buffer
        recordPtr end = newPtr(0);
        for (; end.block != memSize1; incr(end)) {
            if (!getRecord(buffer, end).valid) {
                break;
            }
        }
        decr(end);

        // pointers for convenience
        block_t *bufferIn = buffer + memSize - 1;
        block_t *bufferOut = buffer + memSize;
        emptyBlock(bufferOut);
        (*bufferOut).blockid = 0;
        (*bufferOut).valid = true;

        int in = open(tmpFile, O_RDONLY, S_IRWXU);

        // first block of file2 is loaded on buffer
        (*nios) += readBlocks(in, bufferIn, 1);
        // the id of the block of file2 currently on buffer
        uint currentInBlockId = 0;

        recordPtr ptr = newPtr(0);
        recordPtr backUp;
        record_t *lastRecordJoined = NULL;

        // becomes true when there are no records left to join
        bool joinIsOver = false;

        while (!joinIsOver) {

            // for each record of the current block loaded in bufferIn...
            for (int i = 0; i < MAX_RECORDS_PER_BLOCK; i++) {
                record_t rec = (*bufferIn).entries[i];

                // if the record is invalid, the end of file2 is reached, so join is over
                if (!rec.valid) {
                    joinIsOver = true;
                    break;
                }

                // if the previous record joined from file2 has the
                // same value as the current, sets the ptr pointer to the record
                // of the buffer where that value is first encountered.
                if (lastRecordJoined) {
                    if (compareRecords(*lastRecordJoined, rec, field) == 0) {
                        ptr = copyPtr(backUp);
                    }
                }

                // scans the blocks of file1, until a record with higher or equal
                // value with the current is found.
                while (compareRecords(getRecord(buffer, ptr), rec, field) < 0) {
                    incr(ptr);
                    // if ptr moves past end, all records of file1 have been
                    // examined, so join is over
                    if (ptr > end) {
                        joinIsOver = true;
                        break;
                    }
                }

                if (joinIsOver) {
                    break;
                }

                // if the first record (of file1) with not lower value than the
                // current, has actually higher value than the current, continues
                // to the next record of file2.
                // there are no records in the smaller to join with the current
                if (compareRecords(getRecord(buffer, ptr), rec, field) > 0) {
                    continue;
                }

                // the recordPtr which points to the first record of file1
                // with value equal to the current record of file2 is kept as backup,
                // as well as the record itself, so that if the next record
                // of file2 has the same value, the ptr can be set to record
                if (!lastRecordJoined) {
                    lastRecordJoined = (record_t*) malloc(sizeof (record_t));
                }
                memcpy(lastRecordJoined, &rec, sizeof (record_t));
                backUp = copyPtr(ptr);

                // starting from the record ptr points to, all the following records
                // with equal value to the current are written as pairs to the output.
                while (compareRecords(getRecord(buffer, ptr), rec, field) == 0) {
                    (*bufferOut).entries[(*bufferOut).nreserved++] = rec;
                    (*bufferOut).entries[(*bufferOut).nreserved++] = getRecord(buffer, ptr);
                    (*nres) += 1;

                    // if the buffer block used for output becomes full, writes it to
                    // the outfile and empties it.
                    if ((*bufferOut).nreserved == MAX_RECORDS_PER_BLOCK) {
                        (*nios) += writeBlocks(out, bufferOut, 1);
                        emptyBlock(bufferOut);
                        (*bufferOut).blockid += 1;
                    }

                    // if ptr moves past end, stops searching for records.
                    // there are no more
                    incr(ptr);
                    if (ptr > end) {
                        decr(ptr);
                        break;
                    }
                }
            }
            if (joinIsOver) {
                break;
            }
            // after all records of file2's block currently loaded
            // are examined, if there are blocks left to file2, loads
            // the next one, otherwise join is over.
            if (currentInBlockId < tmpFileSize - 1) {
                (*nios) += readBlocks(in, bufferIn, 1);
                currentInBlockId += 1;
            } else {
                joinIsOver = true;
            }
        }
        if (lastRecordJoined) {
            free(lastRecordJoined);
        }
        // if the are pairs left in the buffer, writes them to the outfile
        if ((*bufferOut).nreserved != 0) {
            (*nios) += writeBlocks(out, bufferOut, 1);
        }
        close(in);
    }
    remove(tmpFile);
    close(out);
}

void MergeJoin(char *infile1, char *infile2, unsigned char field, block_t *buffer, unsigned int nmem_blocks, char *outfile, unsigned int *nres, unsigned int *nios) {

    if (nmem_blocks < 3) {
        printf("At least 3 blocks are required.");
        return;
    }
    
    emptyBuffer(buffer, nmem_blocks);

    // memSize blocks will be used for joining while the other is used for output
    uint memSize = nmem_blocks - 1;
    uint fileSize1 = getSize(infile1);
    uint fileSize2 = getSize(infile2);

    *nres = 0;
    *nios = 0;

    if (fileSize1 != 0 && fileSize2 != 0) {
        // if at least one of the two files fits in memSize - 1 blocks, calls fitCase
        if ((fileSize1 < memSize || fileSize2 < memSize)) {
            fitCase(infile1, infile2, field, buffer, nmem_blocks, outfile, nres, nios);
        } else {
            char tmpFile1[] = ".mj1";
            char tmpFile2[] = ".mj2";

            // each one of the infiles is sorted using MergeSort.
            // the files produced (".mj1" and ".mj2") are 100% utilised (with the possible
            // exception of the last block of each) so no measures for invalid blocks need
            // to be taken
            uint dummy1, dummy2, ios;
            MergeSort(infile1, field, buffer, nmem_blocks, tmpFile1, &dummy1, &dummy2, &ios);
            (*nios) += ios;
            MergeSort(infile2, field, buffer, nmem_blocks, tmpFile2, &dummy1, &dummy2, &ios);
            (*nios) += ios;

            int out = open(outfile, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU);

            fileSize1 = getSize(tmpFile1);
            fileSize2 = getSize(tmpFile2);

            // if non of the sorted files are empty...
            if (fileSize1 != 0 && fileSize2 != 0) {
                // the first file is considered to be the smaller one.
                // if that's not the case, swaps them
                if (fileSize1 > fileSize2) {
                    uint tmp = fileSize1;
                    fileSize1 = fileSize2;
                    fileSize2 = tmp;
                    tmpFile1[3] = '2';
                    tmpFile2[3] = '1';
                }

                // of the memSize available blocks, memSize - 1 are given to the smaller
                // relation and 1 to the big relation
                int in1 = open(tmpFile1, O_RDONLY, S_IRWXU);
                int in2 = open(tmpFile2, O_RDONLY, S_IRWXU);

                // pointers for convenience
                block_t *bufferIn = buffer + memSize - 1;
                block_t *bufferOut = buffer + memSize;
                emptyBlock(bufferOut);
                (*bufferOut).blockid = 0;
                (*bufferOut).valid = true;

                // the number of blocks in buffer given to the smaller relation
                uint memSize1;
                if (fileSize1 < memSize - 1) {
                    memSize1 = fileSize1;
                } else {
                    memSize1 = memSize - 1;
                }

                // the memSize1 first blocks of the smaller and the first
                // block of the bigger are loaded on buffer
                (*nios) += readBlocks(in1, buffer, memSize1);
                (*nios) += readBlocks(in2, bufferIn, 1);

                // the lowest block id of the blocks of the smaller file currenty loaded on buffer
                uint firstBlockId = 0;
                // the highest block id of the blocks of the smaller file currently loaded on buffer
                uint lastBlockId = memSize1 - 1;
                // the block id of the current block loaded from the bigger file
                uint currentInBlockId = 0;

                // pointer to a record of the smaller relation currently on buffer
                // initialises to point to the very first record of the file
                recordPtr ptr = newPtr(0);

                blockToLoad backUp;
                backUp.lastValueJoined = NULL;

                // becomes true when there are no records left to join
                bool joinIsOver = false;

                while (!joinIsOver) {

                    // for each record of the current block loaded in bufferIn (this is the block
                    // the bigger relation uses)...
                    for (int i = 0; i < MAX_RECORDS_PER_BLOCK; i++) {
                        record_t rec = (*bufferIn).entries[i];
                        // if an invalid record is found, that means the end of the bigger
                        // relation is reached, so sets joinIsOver as true and breaks the loop
                        if (!rec.valid) {
                            joinIsOver = true;
                            break;
                        }
                        // if the previous record joined from the bigger relation has the
                        // same value as the current, sets the ptr pointer to the block
                        // where that value is first encountered. if that block is no longer
                        // in the buffer, reloads it as well as the others after it, if they
                        // were removed as well
                        if (backUp.lastValueJoined) {
                            if (compareRecords(rec, *backUp.lastValueJoined, field) == 0) {
                                if (backUp.blockId < firstBlockId) {
                                    uint blocksToLoad;
                                    if (firstBlockId - backUp.blockId <= memSize1) {
                                        blocksToLoad = firstBlockId - backUp.blockId;
                                    } else {
                                        blocksToLoad = memSize1;
                                    }
                                    for (uint i = 0; i < blocksToLoad; i++) {
                                        (*nios) += preadBlocks(in1, buffer + (backUp.blockId + i) % memSize1, backUp.blockId + i, 1);
                                    }
                                    firstBlockId = backUp.blockId;
                                    lastBlockId = firstBlockId + memSize1 - 1;
                                }
                                ptr.block = backUp.blockId % memSize1;
                                ptr.record = 0;
                            }
                        }

                        // scans the blocks of the smaller relation currently loaded, until
                        // a record with higher or equal value with the current is found.
                        // if needed, blocks from the smaller relation are loaded 
                        while (compareRecords(getRecord(buffer, ptr), rec, field) < 0) {
                            // ptr increases by one
                            incr(ptr);
                            // if the increment caused ptr to change block, makes sure
                            // that the ptr stays in just the memSize1 first blocks of the buffer
                            if (ptr.record == 0) {
                                ptr.block = ptr.block % memSize1;
                                // if the ptr after the block change has completed a full circle
                                // of the blocks currently loaded, if there are any more blocks
                                // left to the small relation, load the next one.
                                // if there are no blocks left, join is over, because the last record
                                // of the smaller relation has lower value than the current record
                                // of the bigger, and consiquently from the rest as well
                                if (ptr.block == firstBlockId % memSize1) {
                                    if (lastBlockId < fileSize1 - 1) {
                                        (*nios) += preadBlocks(in1, buffer + firstBlockId % memSize1, lastBlockId + 1, 1);
                                        firstBlockId += 1;
                                        lastBlockId += 1;
                                    } else {
                                        joinIsOver = true;
                                        break;
                                    }
                                }
                            }
                            // if the record ptr points to is invalid, the end of the
                            // smaller relation has been reached. for the same reason
                            // as above, join is over.
                            if (!getRecord(buffer, ptr).valid) {
                                joinIsOver = true;
                                break;
                            }
                        }
                        if (joinIsOver) {
                            break;
                        }
                        // if the first record (of the smaller relation) with
                        // not lower value than the current, has actually higher value
                        // than the current, continues to the next record of the
                        // bigger relation. there are no records in the smaller to join
                        // with the current
                        if (compareRecords(getRecord(buffer, ptr), rec, field) > 0) {
                            continue;
                        }

                        // the block id of the smaller relation where the first record
                        // with value equal to the current of the big relation is encountered
                        // , is kept, as well as the record itself, so that if the next record
                        // of the bigger relation has the same value, the ptr can be set to that block
                        if (!backUp.lastValueJoined) {
                            backUp.lastValueJoined = (record_t*) malloc(sizeof (record_t));
                        }
                        record_t tmp = getRecord(buffer, ptr);
                        memcpy(backUp.lastValueJoined, &tmp, sizeof (record_t));
                        if (ptr.block >= firstBlockId % memSize1) {
                            backUp.blockId = firstBlockId + ptr.block - firstBlockId % memSize1;
                        } else {
                            backUp.blockId = firstBlockId + ptr.block + memSize1 - firstBlockId % memSize1;
                        }

                        // starting from the record ptr points to, all the following records
                        // with equal value to the current are written as pairs to the output.
                        while (compareRecords(getRecord(buffer, ptr), rec, field) == 0) {
                            (*bufferOut).entries[(*bufferOut).nreserved++] = rec;
                            (*bufferOut).entries[(*bufferOut).nreserved++] = getRecord(buffer, ptr);
                            (*nres) += 1;

                            // if the buffer block used for output becomes full, writes it to
                            // the outfile and empties it.
                            if ((*bufferOut).nreserved == MAX_RECORDS_PER_BLOCK) {
                                (*nios) += writeBlocks(out, bufferOut, 1);
                                emptyBlock(bufferOut);
                                (*bufferOut).blockid += 1;
                            }

                            incr(ptr);
                            if (ptr.record == 0) {
                                // makes sure the ptr doesn't exceed the first memSize1 blocks.
                                ptr.block = ptr.block % memSize1;
                                // if needed, loads next block from smaller relation
                                if (ptr.block == firstBlockId % memSize1) {
                                    if (lastBlockId < fileSize1 - 1) {
                                        (*nios) += preadBlocks(in1, buffer + firstBlockId % memSize1, lastBlockId + 1, 1);
                                        firstBlockId += 1;
                                        lastBlockId += 1;
                                    } else {
                                        if (ptr.block == 0) {
                                            ptr.block = memSize1 - 1;
                                        } else {
                                            ptr.block -= 1;
                                        }
                                        ptr.record = MAX_RECORDS_PER_BLOCK - 1;
                                        break;
                                    }
                                }
                            }
                            if (!getRecord(buffer, ptr).valid) {
                                decr(ptr);
                                break;
                            }
                        }
                    }
                    if (joinIsOver) {
                        break;
                    }
                    // after all records of the bigger relation's block currently loaded
                    // are examined, if there are blocks left to the bigger relation, loads
                    // the next one, otherwise join is over.
                    if (currentInBlockId < fileSize2 - 1) {
                        (*nios) += readBlocks(in2, bufferIn, 1);
                        currentInBlockId += 1;
                    } else {
                        joinIsOver = true;
                    }
                }
                if (backUp.lastValueJoined) {
                    free(backUp.lastValueJoined);
                }
                // if the are pairs left in the buffer, writes them to the outfile
                if ((*bufferOut).nreserved != 0) {
                    (*nios) += writeBlocks(out, bufferOut, 1);
                }
                close(in1);
                close(in2);
            }
            remove(tmpFile1);
            remove(tmpFile2);
            close(out);
        }
    }
}