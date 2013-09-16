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

#ifndef RECORDPTR_H
#define	RECORDPTR_H

#include <sys/types.h>

#include "dbtproj.h"

// location of a specific record in the buffer
// block is the index number of the block the record is in
// record is the index number of the record in the entries table

typedef struct {
    uint block;
    int record;
} recordPtr;

// struct for making linked lists of recordPtrs

struct linkedRecordPtr {
    recordPtr ptr;
    linkedRecordPtr *next;
};

// overloading of some operators so that comparisons can be made
// between two recordPtr variables
bool operator==(const recordPtr &ptr1, const recordPtr &ptr2);

bool operator!=(const recordPtr &ptr1, const recordPtr &ptr2);

bool operator>(const recordPtr &ptr1, const recordPtr &ptr2);

bool operator<(const recordPtr &ptr1, const recordPtr &ptr2);

bool operator>=(const recordPtr &ptr1, const recordPtr &ptr2);

bool operator<=(const recordPtr &ptr1, const recordPtr &ptr2);

uint operator-(const recordPtr &ptr1, const recordPtr &ptr2);

recordPtr operator+(const recordPtr &ptr, int offset);

recordPtr operator-(const recordPtr &ptr, int offset);

inline recordPtr copyPtr(recordPtr ptr) {
    return ptr + 0;
}

inline recordPtr newPtr(recordPtr ptr, uint offset) {
    return ptr + offset;
}

inline recordPtr newPtr(uint offset) {
    recordPtr zero;
    zero.block = 0;
    zero.record = 0;
    return zero + offset;
}

inline uint getOffset(recordPtr ptr) {
    return ptr - newPtr(0);
}

// increases the ptr so that it points to the next record
// if pointing at the end of a block, moves to the start of the next

inline void incr(recordPtr &ptr) {
    if (ptr.record < MAX_RECORDS_PER_BLOCK - 1) {
        ptr.record += 1;
    } else {
        ptr.record = 0;
        ptr.block += 1;
    }
}

// decreases the ptr so that it points to the next record
// if pointing at the start of a block, moves to the end of the previous

inline void decr(recordPtr &ptr) {
    if (ptr.record > 0) {
        ptr.record -= 1;
    } else if (ptr.block > 0) {
        ptr.record = MAX_RECORDS_PER_BLOCK - 1;
        ptr.block -= 1;
    }
}

#endif