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