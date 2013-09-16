#include "recordOps.h"

#include <stdlib.h>

// frees the memory allocated to a hash index

void destroyHashIndex(linkedRecordPtr **hashIndex, uint size) {
    for (uint i = 0; i < size; i++) {
        if (!hashIndex[i]) {
            continue;
        }
        linkedRecordPtr *temp1 = hashIndex[i];
        linkedRecordPtr *temp2 = temp1->next;
        while (temp1) {
            free(temp1);
            temp1 = temp2;
            if (temp2) {
                temp2 = temp2->next;
            }
        }
    }
    free(hashIndex);
}
