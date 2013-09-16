/*
* DBMS Implementation
* Copyright (C) 2013 George Piskas, George Economides
*
* This program is free software; you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation; either version 2 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License along
* with this program; if not, write to the Free Software Foundation, Inc.,
* 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*
* Contact: geopiskas@gmail.com
*/

#include "sortBuffer.h"

#include <math.h>

#include "recordPtr.h"
#include "recordOps.h"

// insertionsort sorting algorithm implementation

void insertionSort(block_t *buffer, recordPtr left, recordPtr right, unsigned char field) {
    for (recordPtr i = left + 1; i <= right; incr(i)) {
        record_t target = getRecord(buffer, i);
        recordPtr holePos = i;

        while (holePos > left && compareRecords(target, getRecord(buffer, holePos - 1), field) < 0) {
            setRecord(buffer, getRecord(buffer, holePos - 1), holePos);
            decr(holePos);
        }

        setRecord(buffer, target, holePos);
    }
}

// after a change in the heap, gives it again heap structure

void siftDown(block_t *buffer, uint offset, recordPtr left, recordPtr right, unsigned char field) {
    recordPtr root = copyPtr(left);

    while (newPtr((root.block * MAX_RECORDS_PER_BLOCK + root.record) * 2 + 1) <= right) {
        recordPtr child = newPtr((root.block * MAX_RECORDS_PER_BLOCK + root.record) * 2 + 1 + offset);
        recordPtr swap = root + offset;

        if (compareRecords(getRecord(buffer, swap), getRecord(buffer, child), field) < 0) {
            swap = copyPtr(child);
        }
        if (child + 1 <= right + offset && compareRecords(getRecord(buffer, swap), getRecord(buffer, child + 1), field) < 0) {
            swap = child + 1;
        }
        if (swap != root + offset) {
            swapRecords(buffer, root + offset, swap);
            root = swap - offset;
        } else {
            return;
        }
    }
}

// reorganises records so that heap structure is achieved

void heapify(block_t *buffer, uint offset, recordPtr right, unsigned char field) {

    recordPtr zero = newPtr(0);
    recordPtr start = newPtr(getOffset(right) / 2);
    while (start >= zero) {
        siftDown(buffer, offset, start, right, field);
        if (start == zero) {
            break;
        }
        decr(start);
    }
}

// heapsort sorting algorithm implementation

void heapSort(block_t *buffer, recordPtr left, recordPtr right, unsigned char field) {

    recordPtr zero = newPtr(0);
    uint offset = getOffset(left);
    recordPtr end = right - offset;

    heapify(buffer, offset, end, field);

    while (end > zero) {
        swapRecords(buffer, end + offset, zero + offset);
        decr(end);
        siftDown(buffer, offset, zero, end, field);
    }
}

// introsort sorting algorithm implementation.
// uses quicksort until either the records to be sorted are <=10, where insertion
// sort is used, or the recursion depth exceeds a limit, where heapsort is used

void introSort(block_t *buffer, recordPtr left, recordPtr right, unsigned char field, uint depth) {
    if (left < right) {
        if (right - left < 10) {
            insertionSort(buffer, left, right, field);
        } else if (depth == 0) {
            heapSort(buffer, left, right, field);
        } else {
            record_t pivot = getRecord(buffer, left + (right - left) / 2);
            recordPtr start = copyPtr(left);
            recordPtr end = copyPtr(right);
            while (left <= right) {
                while (compareRecords(getRecord(buffer, left), pivot, field) < 0) {
                    incr(left);
                }
                while (compareRecords(getRecord(buffer, right), pivot, field) > 0) {
                    decr(right);
                }
                if (left <= right) {
                    swapRecords(buffer, left, right);
                    incr(left);
                    decr(right);
                }
            }
            depth -= 1;
            introSort(buffer, start, right, field, depth);
            introSort(buffer, left, end, field, depth);
        }

    }
}

// arranges the blocks in the buffer by placing the non-valid ones at the end of it
// returns the number of valid blocks (<=nmem_blocks)
// linear cost achieved using horspool algorithm

uint arrangeBlocks(block_t* buffer, uint bufferSize) {
    uint start = 0;
    uint end = bufferSize - 1;

    for (; start < end; start++) {
        if (!buffer[start].valid) {
            while (!buffer[end].valid && end > start) {
                end -= 1;
            }
            if (start == end) {
                break;
            }
            block_t temp = buffer[start];
            buffer[start] = buffer[end];
            buffer[end] = temp;
            end -= 1;
        }
    }
    if (buffer[start].valid) {
        start += 1;
    }
    return start;
}

// arranges the records in a segment of valid blocks by placing the non-valid records
// at the end of the segment
// returns the number of valid records and sets requiredBlocks to the number of blocks
// required to store just the valid records
// linear cost achieved using horspool algorithm

uint arrangeRecords(block_t* buffer, uint bufferSize, recordPtr &lastRecord) {
    if (bufferSize == 0) {
        return 0;
    }
    recordPtr start = newPtr(0);
    recordPtr end = newPtr(bufferSize * MAX_RECORDS_PER_BLOCK - 1);

    for (; start < end; incr(start)) {
        if (!getRecord(buffer, start).valid) {
            while (!getRecord(buffer, end).valid && end > start) {
                decr(end);
            }
            if (start == end) {
                break;
            }
            swapRecords(buffer, start, end);
            decr(end);
        }
    }
    if (getRecord(buffer, start).valid) {
        incr(start);
    }
    if (start.record == 0) {
        lastRecord.block = start.block - 1;
        lastRecord.record = MAX_RECORDS_PER_BLOCK - 1;
    } else {
        lastRecord.block = start.block;
        lastRecord.record = start.record - 1;
    }
    return 1;
}

// creates a sorted segment of records in buffer
// returns false if the buffer has no valid records, true otherwise

bool sortBuffer(block_t* buffer, uint bufferSize, unsigned char field) {
    // all the valid records of all tha valid blocks are gathered at the beginning
    // of the buffer and are then sorted using introsort. the remaining blocks
    // are invalidated.
    recordPtr end;
    if (arrangeRecords(buffer, arrangeBlocks(buffer, bufferSize), end) == 0) {
        return false;
    }
    introSort(buffer, newPtr(0), end, field, 2 * ((uint) floor(log2(end.block * MAX_RECORDS_PER_BLOCK + end.record + 1))));
    uint i = 0;
    for (; i < end.block; i++) {
        buffer[i].nreserved = MAX_RECORDS_PER_BLOCK;
        buffer[i].blockid = i;
    }
    buffer[end.block].nreserved = end.record + 1;
    buffer[end.block].blockid = i;

    for (i += 1; i < bufferSize; i++) {
        buffer[i].valid = false;
        buffer[i].nreserved = 0;
        buffer[i].blockid = i;
    }
    return true;
}