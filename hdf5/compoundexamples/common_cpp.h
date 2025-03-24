#ifndef COMMON_CPP_H
#define COMMON_CPP_H

#include "common.h"
#include <H5Cpp.h>
#include <vector> // Added for std::vector

using namespace H5;

// C++-specific constants
extern const H5std_string FILE_NAME;
extern const H5std_string DATASET_NAME;
extern const H5std_string ATTRIBUTE_NAME;

// Function to create the compound type (C++ only)
CompType createCompoundType();

#endif // COMMON_CPP_H