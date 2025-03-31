#include "H5Cpp.h"
#include <iostream>
#include <cstring>
#include <vector>

using namespace H5;

const std::string FILE_NAME = "all_types_separate.h5";

int main() {
    H5File file(FILE_NAME, H5F_ACC_TRUNC);
    DataSpace scalar = DataSpace(H5S_SCALAR);

    // 1. Fixed-Point
    int32_t fixed_val = 42;
    file.createDataSet("/fixed_point", PredType::NATIVE_INT32, scalar).write(&fixed_val, PredType::NATIVE_INT32);

    // 2. Floating-Point
    float float_val = 3.14f;
    file.createDataSet("/float", PredType::NATIVE_FLOAT, scalar).write(&float_val, PredType::NATIVE_FLOAT);

    // 3. Time (HDF5 Class 2 Time datatype)
    int64_t time_val = 1672531200; // 2023-01-01T00:00:00Z
    hid_t time_tid = H5Tcreate(H5T_TIME, 8); // Create Time datatype with 8-byte size
    if (time_tid < 0) {
        throw H5::DataTypeIException("H5Tcreate", "Failed to create Time datatype");
    }
    H5Tset_precision(time_tid, 64); // 64-bit precision
    H5Tset_order(time_tid, H5T_ORDER_LE); // Little-endian
    DataType time_type(time_tid); // Wrap in C++ DataType object
    file.createDataSet("/time", time_type, scalar).write(&time_val, time_type);
    H5Tclose(time_tid); // Clean up the raw hid_t

    // 4. String (fixed-length)
    char string[16] = "Hello HDF5!";
    StrType str_type(PredType::C_S1, 16);
    file.createDataSet("/string", str_type, scalar).write(string, str_type);

    // 5. Bit Field (use HDF5's built-in bitfield type)
    uint8_t bits = 0b10101010;
    file.createDataSet("/bitfield", PredType::STD_B8LE, scalar).write(&bits, PredType::STD_B8LE);

    // 6. Opaque
    uint8_t opaque_buf[4] = {0xDE, 0xAD, 0xBE, 0xEF};
    DataType opaque_type(H5Tcreate(H5T_OPAQUE, 4));
    opaque_type.setTag("4-byte hex");
    file.createDataSet("/opaque", opaque_type, scalar).write(opaque_buf, opaque_type);

    // 7. Compound
    struct Compound {
        int16_t a;
        double b;
    } compound_val = {123, 9.81};

    CompType compound_type(sizeof(Compound));
    compound_type.insertMember("a", HOFFSET(Compound, a), PredType::NATIVE_INT16);
    compound_type.insertMember("b", HOFFSET(Compound, b), PredType::NATIVE_DOUBLE);
    file.createDataSet("/compound", compound_type, scalar).write(&compound_val, compound_type);

    // 8. Reference
    int dummy = 1;
    file.createDataSet("/target", PredType::NATIVE_INT, scalar).write(&dummy, PredType::NATIVE_INT);
    hobj_ref_t ref;
    H5Rcreate(&ref, file.getId(), "/target", H5R_OBJECT, -1);
    file.createDataSet("/reference", PredType::STD_REF_OBJ, scalar).write(&ref, PredType::STD_REF_OBJ);

    // 9. Enumerated
    uint8_t red = 0, green = 1, blue = 2;
    uint8_t color = green;
    EnumType enum_type(sizeof(uint8_t));
    enum_type.insert("RED", &red);
    enum_type.insert("GREEN", &green);
    enum_type.insert("BLUE", &blue);
    file.createDataSet("/enum", enum_type, scalar).write(&color, enum_type);

    // 10. Variable-Length
    int vdata[] = {7, 8, 9};
    hvl_t vlen;
    vlen.len = 3;
    vlen.p = vdata;
    VarLenType vlen_type(PredType::NATIVE_INT);
    file.createDataSet("/vlen", vlen_type, scalar).write(&vlen, vlen_type);

    // 11. Array
    hsize_t dims[1] = {3};
    ArrayType array_type(PredType::NATIVE_INT, 1, dims);
    int arr[3] = {10, 20, 30};
    file.createDataSet("/array", array_type, scalar).write(arr, array_type);

    file.close();
    std::cout << "✅ Created file: " << FILE_NAME << std::endl;
    return 0;
}