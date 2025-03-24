#include "common_cpp.h"

// Define constants
const H5std_string FILE_NAME(FILENAME);
const H5std_string DATASET_NAME(DATASETNAME);
const H5std_string ATTRIBUTE_NAME("GIT root revision");

CompType createCompoundType() {
    CompType compound_type(sizeof(Record));
    
    compound_type.insertMember("recordId", HOFFSET(Record, recordId), PredType::NATIVE_UINT64);
    StrType fixed_str_type(PredType::C_S1, 10);
    fixed_str_type.setStrpad(H5T_STR_NULLTERM);
    compound_type.insertMember("fixedStr", HOFFSET(Record, fixedStr), fixed_str_type);
    VarLenType var_str_type(PredType::C_S1);
    compound_type.insertMember("varStr", HOFFSET(Record, varStr), var_str_type);
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
    IntType bitfield_type(PredType::NATIVE_UINT64);
    bitfield_type.setPrecision(57);
    bitfield_type.setOffset(7);
    compound_type.insertMember("bitfieldVal", HOFFSET(Record, bitfieldVal), bitfield_type);
    
    return compound_type;
}