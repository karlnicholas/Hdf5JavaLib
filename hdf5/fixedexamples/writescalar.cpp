#include <iostream>
#include <H5Cpp.h>
#include <random>
#include <vector>
#include <cstring>  // For memcpy

using namespace H5;
const H5std_string ATTRIBUTE_NAME("GIT root revision");


int main() {
    // Create or open an HDF5 file.
    H5::H5File file("scalar.h5", H5F_ACC_TRUNC);

    // Create a scalar dataspace for a single value.
    H5::DataSpace scalarSpace(H5S_SCALAR);

    // Create a fixed-point datatype (e.g., 32-bit little-endian integer).
    H5::IntType intType(H5::PredType::NATIVE_INT);
    intType.setOrder(H5T_ORDER_LE);

    // Create the dataset with the fixed-point datatype.
    H5::DataSet dataset = file.createDataSet("FixedPointValue", intType, scalarSpace);

    // // ✅ ADD ATTRIBUTE: "GIT root revision"
    // H5std_string attribute_value = "Revision: , URL: ";
    // StrType attr_type(PredType::C_S1, attribute_value.size());
    // DataSpace attr_space(H5S_SCALAR);
    // Attribute attribute = dataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
    // attribute.write(attr_type, attribute_value);
    // attribute.close();

    // Write a single fixed-point value.
    int value = 42;
    dataset.write(&value, H5::PredType::NATIVE_INT);

    return 0;
}
