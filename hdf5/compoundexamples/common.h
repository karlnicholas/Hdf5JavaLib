#ifndef COMMON_H
#define COMMON_H

#include <hdf5.h>

#define FILENAME "compound_example.h5"
#define DATASETNAME "CompoundData"
#define NUM_RECORDS 1000

struct Record {
    uint64_t recordId;
    char fixedStr[10];
    hvl_t varStr; // Variable-length string
    float floatVal;
    double doubleVal;
    int8_t int8_Val;
    uint8_t uint8_Val;
    int16_t int16_Val;
    uint16_t uint16_Val;
    int32_t int32_Val;
    uint32_t uint32_Val;
    int64_t int64_Val;
    uint64_t uint64_Val;
    uint64_t bitfieldVal; // Bitfield: offset 7, precision 57
};

#endif // COMMON_H