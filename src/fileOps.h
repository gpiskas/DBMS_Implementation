#ifndef FILEOPS_H
#define	FILEOPS_H

#include <sys/types.h>
#include <sys/stat.h>

#include "dbtproj.h"

void createFile(char *filename, uint size);

void createTwoFiles(char *filename1, uint size1, char *filename2, uint size2);

void printFile(char *filename);

// returns the size of a file

inline int getSize(char *filename) {
    struct stat st;
    stat(filename, &st);
    return st.st_size / sizeof (block_t);
}

// returns true if file exists, false otherwise

inline int exists(char *filename) {
    struct stat st;
    return stat(filename, &st) == 0;
}

#endif

