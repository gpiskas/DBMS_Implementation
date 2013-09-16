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

#include "fileOps.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <fcntl.h> 
#include <unistd.h>
#include <time.h>

// generates random string

char* getRandomString() {
    char* str;
    str = (char*) malloc(STR_LENGTH);

    const char text[] = "abcdefghijklmnopqrstuvwxyz";

    int i;
    int len = rand() % (STR_LENGTH - 1);
    len += 1;
    for (i = 0; i < len; ++i) {
        str[i] = text[rand() % (sizeof (text) - 1)];
    }
    str[i] = '\0';
    return str;
}

// generates random file

void create(char *filename, uint size) {
    int outfile = creat(filename, S_IRWXU);

    uint recid = 0;
    record_t record;
    block_t block;
    for (uint b = 0; b < size; ++b) { // for each block
        block.blockid = b;
        for (int r = 0; r < MAX_RECORDS_PER_BLOCK; ++r) { // for each record

            // prepare a record
            record.recid = recid += 1;
            record.num = rand() % 100000;
            strcpy(record.str, getRandomString()); // put a random string to record
            if (((double) rand() / (RAND_MAX)) >= 0.02) {
                record.valid = true;
            } else {
                record.valid = false;
            }
            memcpy(&block.entries[r], &record, sizeof (record_t)); // copy record to block
        }
        block.nreserved = MAX_RECORDS_PER_BLOCK;
        block.valid = true;
        write(outfile, &block, sizeof (block_t));
    }
    close(outfile);
}

// creates one input file

void createFile(char *filename, uint size) {
    srand(time(NULL));
    create(filename, size);
}

// creates two input files

void createTwoFiles(char *filename1, uint size1, char *filename2, uint size2) {
    srand(time(NULL));
    create(filename1, size1);
    create(filename2, size2);
}

// prints file

void printFile(char *filename) {

    int infile = open(filename, O_RDONLY, S_IRWXU);

    block_t block;
    while (read(infile, &block, sizeof (block_t)) > 0) {
        for (uint i = 0; i < block.nreserved; ++i) {
            if (block.entries[i].valid) {
                printf("BL %d, RC %d, %d, %s\n", block.blockid, block.entries[i].recid, block.entries[i].num, block.entries[i].str);
            }

        }
    }
    close(infile);
}