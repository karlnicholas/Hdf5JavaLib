// common.cpp
#include "common_cpp.h"
#include <iostream>
#include <stddef.h> // For HOFFSET

// Define global H5std_string constants, initialized by macros from common.h
const H5std_string FILE_NAME(FILENAME);                 // Uses "compound_example.h5"
const H5std_string DATASET_NAME(DATASETNAME);           // Uses "CompoundData"
const H5std_string ATTRIBUTE_NAME(ATTRIBUTE_NAME_MACRO); // Uses "GIT root revision"

CompType createCompoundType() { // Your original function name
    CompType compound_type(sizeof(Record)); // Record::varStr is const char*
    std::cout << "Info (common.cpp): sizeof(Record) for C++ [varStr is const char*] = " << sizeof(Record) << std::endl;

    compound_type.insertMember("recordId", HOFFSET(Record, recordId), PredType::NATIVE_UINT64);

    StrType fixed_str_hdf5_type(PredType::C_S1, sizeof(Record::fixedStr));
    fixed_str_hdf5_type.setCset(H5T_CSET_UTF8);
    fixed_str_hdf5_type.setStrpad(H5T_STR_NULLTERM);
    compound_type.insertMember("fixedStr", HOFFSET(Record, fixedStr), fixed_str_hdf5_type);

    StrType var_str_hdf5_type(PredType::C_S1);
    var_str_hdf5_type.setSize(H5T_VARIABLE);
    var_str_hdf5_type.setCset(H5T_CSET_UTF8);
    var_str_hdf5_type.setStrpad(H5T_STR_NULLTERM);
    compound_type.insertMember("varStr", HOFFSET(Record, varStr), var_str_hdf5_type);
    if (!var_str_hdf5_type.isVariableStr() || var_str_hdf5_type.getClass() != H5T_STRING) {
         std::cerr << "ERROR (common.cpp): varStr HDF5 type NOT correctly VLEN H5T_STRING!" << std::endl;
    } else {
         std::cout << "Info (common.cpp): varStr HDF5 type is VLEN H5T_STRING (ClassID: "
                   << var_str_hdf5_type.getClass() << ")." << std::endl;
    }

    compound_type.insertMember("floatVal", HOFFSET(Record, floatVal), PredType::NATIVE_FLOAT);
    compound_type.insertMember("doubleVal", HOFFSET(Record, doubleVal), PredType::NATIVE_DOUBLE);
    compound_type.insertMember("int8_Val", HOFFSET(Record, int8_Val), PredType::NATIVE_INT8);
    compound_type.insertMember("uint8_Val", HOFFSET(Record, uint8_Val), PredType::NATIVE_UINT8);
    compound_type.insertMember("int16_Val", HOFFSET(Record, int16_Val), PredType::NATIVE_INT16);
    compound_type.insertMember("uint16_Val", HOFFSET(Record, uint16_Val), PredType::NATIVE_UINT16);
    compound_type.insertMember("int32_Val", HOFFSET(Record, int32_Val), PredType::NATIVE_INT32);
    compound_type.insertMember("uint32_Val", HOFFSET(Record, uint32_Val), PredType::NATIVE_UINT32);
    compound_type.insertMember("int64_Val", HOFFSET(Record, int64_Val), PredType::NATIVE_INT64);
    compound_type.insertMember("uint64_Val", HOFFSET(Record, uint64_Val), PredType::NATIVE_UINT64);
    
    // --- MODIFIED: scaledUintVal HDF5 type definition ---
    // To store a 57-bit field starting at bit offset 7 within the uint64_t member.
    IntType scaledUint_hdf5_type(PredType::NATIVE_UINT64);
    scaledUint_hdf5_type.setPrecision(57); // The field is 57 bits wide
    scaledUint_hdf5_type.setOffset(7);     // The field starts at bit 7 (0-indexed from LSB)
                                           // This means HDF5 will work with bits 7 through 63.
    compound_type.insertMember("scaledUintVal", HOFFSET(Record, scaledUintVal), scaledUint_hdf5_type);
    std::cout << "Info (common.cpp): scaledUintVal HDF5 type: precision=57, offset=7." << std::endl;
    
    // size_t hdf5_internal_type_size = compound_type.getSize();
    // std::cout << "Info (common.cpp): HDF5 CompType internal size = " << hdf5_internal_type_size << std::endl;

    return compound_type;
}