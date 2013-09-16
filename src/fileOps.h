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

