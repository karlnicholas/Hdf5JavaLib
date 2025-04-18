#include <iostream>
#include <H5Cpp.h>
#include <random>
#include <vector>
#include <cstring>

using namespace H5;
const H5std_string ATTRIBUTE_NAME("GIT root revision");

int main() {
    // Create or open an HDF5 file with truncate to overwrite existing file
    H5::H5File file("scalar.h5", H5F_ACC_TRUNC);

    // Create a scalar dataspace for all datasets
    H5::DataSpace scalarSpace(H5S_SCALAR);

    // Define the attribute value once
    H5std_string attribute_value = "Revision: , URL: ";
    StrType attr_type(PredType::C_S1, attribute_value.size());
    DataSpace attr_space(H5S_SCALAR);

    // // 1. "byte" dataset (8-bit signed integer)
    // H5::IntType byteType(PredType::NATIVE_INT8); // 1 byte, signed
    // byteType.setOrder(H5T_ORDER_LE);
    // H5::DataSet byteDataset = file.createDataSet("byte", byteType, scalarSpace);
    // int8_t byteValue = 42;
    // byteDataset.write(&byteValue, PredType::NATIVE_INT8);
    // Attribute byteAttr = byteDataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
    // byteAttr.write(attr_type, attribute_value);
    // byteAttr.close();

    // // 2. "short" dataset (16-bit signed integer)
    // H5::IntType shortType(PredType::NATIVE_INT16); // 2 bytes, signed
    // shortType.setOrder(H5T_ORDER_LE);
    // H5::DataSet shortDataset = file.createDataSet("short", shortType, scalarSpace);
    // int16_t shortValue = 42;
    // shortDataset.write(&shortValue, PredType::NATIVE_INT16);
    // Attribute shortAttr = shortDataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
    // shortAttr.write(attr_type, attribute_value);
    // shortAttr.close();

    // 3. "integer" dataset (32-bit signed integer)
    H5::IntType intType(PredType::NATIVE_INT32); // 4 bytes, signed
    intType.setOrder(H5T_ORDER_LE);
    H5::DataSet intDataset = file.createDataSet("FixedPointValue", intType, scalarSpace);
    int32_t intValue = 42;
    intDataset.write(&intValue, PredType::NATIVE_INT32);
    Attribute intAttr = intDataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
    intAttr.write(attr_type, attribute_value);
    intAttr.close();

    // // 4. "long" dataset (64-bit signed integer)
    // H5::IntType longType(PredType::NATIVE_INT64); // 8 bytes, signed
    // longType.setOrder(H5T_ORDER_LE);
    // H5::DataSet longDataset = file.createDataSet("long", longType, scalarSpace);
    // int64_t longValue = 42;
    // longDataset.write(&longValue, PredType::NATIVE_INT64);
    // Attribute longAttr = longDataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
    // longAttr.write(attr_type, attribute_value);
    // longAttr.close();

    std::cout << "Created scalar.h5 with datasets: byte, short, integer, long" << std::endl;
    return 0;
}