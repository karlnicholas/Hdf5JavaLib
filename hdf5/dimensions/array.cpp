#include <H5Cpp.h>
#include <iostream>
#include <vector>
#include <cstring>

using namespace H5;

int main() {
    try {
        H5File file("array_datasets.h5", H5F_ACC_TRUNC);

        // 1. Dataset with 2x3 array of integers
        {
            hsize_t array_dims[2] = {2, 3};
            ArrayType array_type(PredType::NATIVE_INT, 2, array_dims);
            DataSpace dataspace(H5S_SCALAR);
            DataSet dataset = file.createDataSet("int_array", array_type, dataspace);
            int data[2][3] = {{1, 2, 3}, {4, 5, 6}};
            dataset.write(&data, array_type);
            std::cout << "Created dataset 'int_array' with 2x3 array of integers\n";
        }

        // 2. Dataset with 1x4 array of floats
        {
            hsize_t array_dims[1] = {4};
            ArrayType array_type(PredType::NATIVE_FLOAT, 1, array_dims);
            DataSpace dataspace(H5S_SCALAR);
            DataSet dataset = file.createDataSet("float_array", array_type, dataspace);
            float data[4] = {1.1f, 2.2f, 3.3f, 4.4f};
            dataset.write(&data, array_type);
            std::cout << "Created dataset 'float_array' with 1x4 array of floats\n";
        }

        // 3. Dataset with 2x2 array of doubles
        {
            hsize_t array_dims[2] = {2, 2};
            ArrayType array_type(PredType::NATIVE_DOUBLE, 2, array_dims);
            DataSpace dataspace(H5S_SCALAR);
            DataSet dataset = file.createDataSet("double_array", array_type, dataspace);
            double data[2][2] = {{1.11, 2.22}, {3.33, 4.44}};
            dataset.write(&data, array_type);
            std::cout << "Created dataset 'double_array' with 2x2 array of doubles\n";
        }

        // 4. Dataset with 1x2 array of fixed-length strings
        {
            hsize_t array_dims[1] = {2};
            StrType str_type(PredType::C_S1, 10);
            ArrayType array_type(str_type, 1, array_dims);
            DataSpace dataspace(H5S_SCALAR);
            DataSet dataset = file.createDataSet("string_array", array_type, dataspace);
            char data[2][10];
            std::strncpy(data[0], "Label1", 10);
            std::strncpy(data[1], "Label2", 10);
            dataset.write(&data, array_type);
            std::cout << "Created dataset 'string_array' with 1x2 array of strings\n";
        }

    } catch (H5::Exception& e) {
        std::cerr << "HDF5 error: " << e.getDetailMsg() << "\n";
        return 1;
    }

    std::cout << "HDF5 file 'array_datasets.h5' created successfully.\n";
    return 0;
}