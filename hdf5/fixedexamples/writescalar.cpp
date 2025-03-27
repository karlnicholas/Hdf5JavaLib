#include "H5Cpp.h"

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

    // Write a single fixed-point value.
    int value = 42;
    dataset.write(&value, H5::PredType::NATIVE_INT);

    return 0;
}
