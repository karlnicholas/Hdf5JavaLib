// common.h
#ifndef COMMON_H
#define COMMON_H

#include <hdf5.h>   // For HOFFSET
#include <stdint.h> // For fixed-width integer types like uint64_t
#include <stddef.h> // For size_t

// YOUR ORIGINAL MACROS FOR C++ to C++ SUCCESS
#define FILENAME "compound_example.h5"
#define DATASETNAME "CompoundData"
// From your common_cpp.h that worked with the successful C++ reader/writer
#define ATTRIBUTE_NAME_MACRO "GIT root revision"
#define NUM_RECORDS 1000

struct Record {
    uint64_t recordId;
    char fixedStr[10];
#ifdef __cplusplus
    const char* varStr; // C++ uses const char* for VLEN string data pointer
#else
    // This C-side definition is for if a C program were to try and match
    // this specific C++ memory layout (sizeof approx 88 bytes).
    // This is NOT for C/C++ interop where C uses hvl_t.
    char* varStr;
#endif
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
    uint64_t scaledUintVal; // Application handles bitfield packing/unpacking
};

#endif // COMMON_H