#ifndef RECORDOPS_H
#define	RECORDOPS_H

#include <string.h>

#include "dbtproj.h"
#include "recordPtr.h"

// given a buffer and a recordPtr, returns corresponding record

inline record_t getRecord(block_t *buffer, recordPtr ptr) {
    return buffer[ptr.block].entries[ptr.record];
}

// given a buffer, a record and a recordPtr, places the record where recordPtr points

inline void setRecord(block_t *buffer, record_t rec, recordPtr ptr) {
    buffer[ptr.block].entries[ptr.record] = rec;
}

// given a buffer and 2 recordPtrs, swaps the records where ptrs point

inline void swapRecords(block_t *buffer, recordPtr ptr1, recordPtr ptr2) {
    record_t tmp = getRecord(buffer, ptr1);
    setRecord(buffer, getRecord(buffer, ptr2), ptr1);
    setRecord(buffer, tmp, ptr2);
}

// given 2 records and field, compares them
// -1 is returned if rec1 has lower field value than rec2
// 0 is returned if rec1 and rec2 have equal field values
// 1 is returned if rec1 has higher field value than rec2

inline int compareRecords(record_t rec1, record_t rec2, unsigned char field) {
    switch (field) {
        case 0:
            if (rec1.recid < rec2.recid) {
                return -1;
            } else if (rec1.recid > rec2.recid) {
                return 1;
            }
            return 0;
        case 1:
            if (rec1.num < rec2.num) {
                return -1;
            } else if (rec1.num > rec2.num) {
                return 1;
            }
            return 0;
        case 2:
            if (strcmp(rec1.str, rec2.str) < 0) {
                return -1;
            } else if (strcmp(rec1.str, rec2.str) > 0) {
                return 1;
            }
            return 0;
        case 3:
            if ((rec1.num < rec2.num || (rec1.num == rec2.num && strcmp(rec1.str, rec2.str) < 0))) {
                return -1;
            } else if ((rec1.num > rec2.num || (rec1.num == rec2.num && strcmp(rec1.str, rec2.str) > 0))) {
                return 1;
            }
            return 0;
    }
}

// hash function for integers

inline uint hashInt(uint num, uint mod, uint seed) {
    num += seed;
    num = (num + 0x7ed55d16) + (num << 12);
    num = (num^0xc761c23c) ^ (num >> 19);
    num = (num + 0x165667b1) + (num << 5);
    num = (num + 0xd3a2646c) ^ (num << 9);
    num = (num + 0xfd7046c5) + (num << 3);
    num = (num^0xb55a4f09) ^ (num >> 16);
    return num % mod;
}

// hash function for strings

inline uint hashString(char *str, uint mod, uint seed) {
    unsigned long hash = 5381;
    int c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;
    }

    return hashInt(hash, 8701123, seed) % mod;
}

// given a record and the field of interest, hashes it and returns a value

inline uint hashRecord(char* seed, record_t rec, uint mod, unsigned char field) {
    uint s = hashString(seed, 8701123, 0);
    switch (field) {
        case 0:
            return hashInt(rec.recid, mod, s);
        case 1:
            return hashInt(rec.num, mod, s);
        case 2:
            return hashString(rec.str, mod, s);
        case 3:
            return hashInt(rec.num + hashString(rec.str, mod, s), mod, s);
    }
}

// frees memory allocated for a hash index
void destroyHashIndex(linkedRecordPtr **hashIndex, uint size);

#endif
