// common.cpp
#include "common_cpp.h"
#include <iostream> // For diagnostic output

// Define constants
const H5std_string FILE_NAME(FILENAME);
const H5std_string DATASET_NAME(DATASETNAME);
const H5std_string ATTRIBUTE_NAME("GIT root revision");

CompType createCompoundType() {
    // Create compound datatype for memory layout
    CompType compound_type(sizeof(Record));
    std::cout << "Info: sizeof(Record) = " << sizeof(Record) << std::endl;

    // Insert members with their memory types and offsets
    compound_type.insertMember("recordId", HOFFSET(Record, recordId), PredType::NATIVE_UINT64);
    std::cout << "Info: Offset recordId = " << HOFFSET(Record, recordId) << std::endl;


    // --- Fixed String Member ---
    StrType fixed_str_type(PredType::C_S1, 10); // Base type C string, max length 10
    fixed_str_type.setCset(H5T_CSET_ASCII);     // Character set ASCII
    fixed_str_type.setStrpad(H5T_STR_NULLTERM); // Null-terminated
    compound_type.insertMember("fixedStr", HOFFSET(Record, fixedStr), fixed_str_type);
    std::cout << "Info: Offset fixedStr = " << HOFFSET(Record, fixedStr) << std::endl;


    // --- Variable Length String Member --- MODIFIED ---
    // 1. Create a base C string type
    StrType var_str_type(PredType::C_S1);
    // 2. Set size to variable
    var_str_type.setSize(H5T_VARIABLE);
    // 3. Set character set to ASCII
    var_str_type.setCset(H5T_CSET_ASCII);
    // 4. Set padding/termination to null-terminated
    var_str_type.setStrpad(H5T_STR_NULLTERM);
    // Insert the defined VLEN string type
    compound_type.insertMember("varStr", HOFFSET(Record, varStr), var_str_type);
    // Verify class type (should be 9 for H5T_STRING)
     if (var_str_type.getClass() != H5T_STRING) {
          std::cerr << "Warning: VLEN String Datatype class is NOT H5T_STRING (ID: "
                    << var_str_type.getClass() << ")" << std::endl;
     } else {
          std::cout << "Info: VLEN String Datatype class is H5T_STRING (ID: "
                    << var_str_type.getClass() << "), as expected." << std::endl;
     }
    std::cout << "Info: Offset varStr = " << HOFFSET(Record, varStr) << std::endl;
    // --- End VLEN String Modification ---


    // --- Other Members ---
    compound_type.insertMember("floatVal", HOFFSET(Record, floatVal), PredType::NATIVE_FLOAT);
     std::cout << "Info: Offset floatVal = " << HOFFSET(Record, floatVal) << std::endl;
    compound_type.insertMember("doubleVal", HOFFSET(Record, doubleVal), PredType::NATIVE_DOUBLE);
     std::cout << "Info: Offset doubleVal = " << HOFFSET(Record, doubleVal) << std::endl;
    compound_type.insertMember("int8_Val", HOFFSET(Record, int8_Val), PredType::NATIVE_INT8);
     std::cout << "Info: Offset int8_Val = " << HOFFSET(Record, int8_Val) << std::endl;
    compound_type.insertMember("uint8_Val", HOFFSET(Record, uint8_Val), PredType::NATIVE_UINT8);
     std::cout << "Info: Offset uint8_Val = " << HOFFSET(Record, uint8_Val) << std::endl;
    compound_type.insertMember("int16_Val", HOFFSET(Record, int16_Val), PredType::NATIVE_INT16);
     std::cout << "Info: Offset int16_Val = " << HOFFSET(Record, int16_Val) << std::endl;
    compound_type.insertMember("uint16_Val", HOFFSET(Record, uint16_Val), PredType::NATIVE_UINT16);
     std::cout << "Info: Offset uint16_Val = " << HOFFSET(Record, uint16_Val) << std::endl;
    compound_type.insertMember("int32_Val", HOFFSET(Record, int32_Val), PredType::NATIVE_INT32);
     std::cout << "Info: Offset int32_Val = " << HOFFSET(Record, int32_Val) << std::endl;
    compound_type.insertMember("uint32_Val", HOFFSET(Record, uint32_Val), PredType::NATIVE_UINT32);
     std::cout << "Info: Offset uint32_Val = " << HOFFSET(Record, uint32_Val) << std::endl;
    compound_type.insertMember("int64_Val", HOFFSET(Record, int64_Val), PredType::NATIVE_INT64);
     std::cout << "Info: Offset int64_Val = " << HOFFSET(Record, int64_Val) << std::endl;
    compound_type.insertMember("uint64_Val", HOFFSET(Record, uint64_Val), PredType::NATIVE_UINT64);
     std::cout << "Info: Offset uint64_Val = " << HOFFSET(Record, uint64_Val) << std::endl;


    // // --- Bitfield Member --- MODIFIED ---
    // // Store as a simple uint64. The C++ code handles the bit logic.
    // // Using IntType with setPrecision/setOffset here is incorrect for this struct layout
    // // and likely caused previous runtime errors.
    // compound_type.insertMember("bitfieldVal", HOFFSET(Record, bitfieldVal), PredType::NATIVE_UINT64);
    // std::cout << "Info: Offset bitfieldVal = " << HOFFSET(Record, bitfieldVal) << std::endl;
    // // --- End Bitfield Modification ---
    IntType scaledUint_type(PredType::NATIVE_UINT64);
    scaledUint_type.setPrecision(57);
    scaledUint_type.setOffset(7);
    compound_type.insertMember("scaledUintVal", HOFFSET(Record, scaledUintVal), scaledUint_type);
    

    // Optional: Verify HDF5 type size against C++ struct size
    size_t hdf5_type_size = compound_type.getSize();
    std::cout << "Info: HDF5 CompType Size = " << hdf5_type_size << std::endl;
    if (hdf5_type_size != sizeof(Record)) {
        std::cerr << "Warning: HDF5 compound type size (" << hdf5_type_size
                  << ") does not match sizeof(Record) (" << sizeof(Record)
                  << "). Check struct padding/alignment differences." << std::endl;
        // Note: Differences can occur due to compiler padding. If they exist and cause issues,
        //       you might need to use H5Tpack or ensure manual padding matches.
        //       However, for this structure, they often align on 64-bit systems.
    }

    return compound_type;
}