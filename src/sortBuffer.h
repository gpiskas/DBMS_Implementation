#ifndef SORTBUFFER_H
#define	SORTBUFFER_H

#include <sys/types.h>

#include "dbtproj.h"

// sorts the records in the buffer
bool sortBuffer(block_t* buffer, uint bufferSize, unsigned char field);

#endif

