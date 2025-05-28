// common_cpp.h
#ifndef COMMON_CPP_H
#define COMMON_CPP_H

#include "common.h" // Defines Record struct (varStr as const char* for C++) and macros
#include <H5Cpp.h>
#include <string>
#include <vector>

using namespace H5;

// Extern declarations for constants defined in common.cpp
// Matching your original common_cpp.h names
extern const H5std_string FILE_NAME;
extern const H5std_string DATASET_NAME;
extern const H5std_string ATTRIBUTE_NAME;

// Function prototype for creating the HDF5 compound type for C++
CompType createCompoundType(); // Matching your original function name

#endif // COMMON_CPP_H