#include "recordPtr.h"

// overloading operators for use with struct recordPtr

bool operator==(const recordPtr &ptr1, const recordPtr &ptr2) {
    if (ptr1.block == ptr2.block && ptr1.record == ptr2.record) {
        return true;
    } else {
        return false;
    }
}

bool operator!=(const recordPtr &ptr1, const recordPtr &ptr2) {
    return !(ptr1 == ptr2);
}

bool operator>(const recordPtr &ptr1, const recordPtr &ptr2) {
    if (ptr1.block == ptr2.block) {
        if (ptr1.record > ptr2.record) {
            return true;
        } else {
            return false;
        }
    } else {
        if (ptr1.block > ptr2.block) {
            return true;
        } else {
            return false;
        }
    }
}

bool operator<(const recordPtr &ptr1, const recordPtr &ptr2) {
    if (ptr1.block == ptr2.block) {
        if (ptr1.record < ptr2.record) {
            return true;
        } else {
            return false;
        }
    } else {
        if (ptr1.block < ptr2.block) {
            return true;
        } else {
            return false;
        }
    }
}

bool operator>=(const recordPtr &ptr1, const recordPtr &ptr2) {
    if (ptr1 > ptr2 || ptr1 == ptr2) {
        return true;
    } else {
        return false;
    }
}

bool operator<=(const recordPtr &ptr1, const recordPtr &ptr2) {
    if (ptr1 < ptr2 || ptr1 == ptr2) {
        return true;
    } else {
        return false;
    }
}

uint operator-(const recordPtr &ptr1, const recordPtr &ptr2) {
    return (ptr1.block * MAX_RECORDS_PER_BLOCK + ptr1.record) - (ptr2.block * MAX_RECORDS_PER_BLOCK + ptr2.record);
}

recordPtr operator+(const recordPtr &ptr, int offset) {
    recordPtr result;
    result.block = ptr.block + offset / MAX_RECORDS_PER_BLOCK;
    int rest = offset % MAX_RECORDS_PER_BLOCK;

    if (ptr.record + rest >= MAX_RECORDS_PER_BLOCK) {
        result.block += 1;
        result.record = ptr.record + rest - MAX_RECORDS_PER_BLOCK;
    } else {
        result.record = ptr.record + rest;
    }
    return result;
}

recordPtr operator-(const recordPtr &ptr, int offset) {
    recordPtr result;

    result.block = ptr.block - offset / MAX_RECORDS_PER_BLOCK;
    int rest = offset % MAX_RECORDS_PER_BLOCK;
    if (ptr.record - rest < 0) {
        result.block -= 1;
        result.record = MAX_RECORDS_PER_BLOCK + ptr.record - rest;
    } else {
        result.record = ptr.record - rest;
    }
    return result;
}