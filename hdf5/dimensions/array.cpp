#include <H5Cpp.h>
#include <iostream>
#include <vector>

using namespace H5;

int main() {
    try {
        H5File file("array.h5", H5F_ACC_TRUNC);
        hsize_t array_dims[2] = {2, 3};
        ArrayType array_type(PredType::NATIVE_INT, 2, array_dims);
        DataSpace dataspace(H5S_SCALAR);
        DataSet dataset = file.createDataSet("array_dataset", array_type, dataspace);
        int data[2][3] = {{1, 2, 3}, {4, 5, 6}};
        dataset.write(&data, array_type);
        std::cout << "Created dataset 'array_dataset' with a 2x3 array of integers\n";
    } catch (H5::Exception& e) {
        std::cerr << "HDF5 error: " << e.getDetailMsg() << "\n";
        return 1;
    }
    std::cout << "HDF5 file 'array.h5' created successfully.\n";
    return 0;
}