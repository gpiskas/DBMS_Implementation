#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>

#include "dbtproj.h"
#include "fileOps.h"

int main(int argc, char** argv) {

    char infile1[] = "infile1.bin";
    char infile2[] = "infile2.bin";
    char outfile[] = "output.bin";

    //reateFile(infile1, 7600); //76=~1MB, 76000=~1GB
    createTwoFiles(infile1, 76, infile2, 60);

    //printFile(infile1);
    //printFile(infile2);

    uint nmem_blocks = 22;
    block_t* buffer = (block_t*) malloc(nmem_blocks * sizeof (block_t));
    uint nsorted_segs = 0, npasses = 0, nios = 0, nres = 0, nunique = 0;

    MergeSort(infile1, 1, buffer, nmem_blocks, outfile, &nsorted_segs, &npasses, &nios);
    printf("nios = %d, npasses = %d, nsorted_segs = %d\n", nios, npasses, nsorted_segs);
    //printFile(outfile);

    HashJoin(infile1, infile2, 2, buffer, nmem_blocks, outfile, &nres, &nios);
    printf("nios = %d, nres = %d\n", nios, nres);
    //printFile(outfile);

    EliminateDuplicates(infile1, 3, buffer, nmem_blocks, outfile, &nunique, &nios);
    printf("nios = %d, nunique = %d\n", nios, nunique);
    //printFile(outfile);

    MergeJoin(infile1, infile2, 0, buffer, nmem_blocks, outfile, &nres, &nios);
    printf("nios = %d, nres = %d\n", nios, nres);
    //printFile(outfile);

    return 0;
}

