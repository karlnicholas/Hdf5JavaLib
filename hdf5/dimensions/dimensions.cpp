#include <H5Cpp.h>
#include <iostream>
#include <vector>

using namespace H5;

int main() {
    try {
        // Create an HDF5 file
        H5File file("dimensions.h5", H5F_ACC_TRUNC);

        // 1. Scalar dataset (no dimensionality)
        {
            // Create scalar dataspace
            DataSpace scalar_space(H5S_SCALAR);
            
            // Create dataset with a single double value
            DataSet dataset = file.createDataSet("scalar_dataset", PredType::NATIVE_DOUBLE, scalar_space);
            
            // Write a single value
            double value = 42.0;
            dataset.write(&value, PredType::NATIVE_DOUBLE);
            // No permutation attribute (scalar has no dimensions)
            std::cout << "Created scalar dataset 'scalar_dataset' with value: " << value << "\n";
        }

        // 2. 1D dataset (simple array, dimension size = 5)
        {
            // Define dimensions
            hsize_t dims[1] = {5};
            DataSpace dataspace(1, dims);
            
            // Create dataset
            DataSet dataset = file.createDataSet("1d_dataset", PredType::NATIVE_DOUBLE, dataspace);
            
            // Write data
            std::vector<double> data = {1.0, 2.0, 3.0, 4.0, 5.0};
            dataset.write(data.data(), PredType::NATIVE_DOUBLE);
            // No permutation attribute (canonical order: [0])
            std::cout << "Created 1D dataset '1d_dataset' with size 5\n";
        }

        // 3. 2D dataset (2x3 array)
        {
            // Define dimensions
            hsize_t dims[2] = {2, 3};
            DataSpace dataspace(2, dims);
            
            // Create dataset
            DataSet dataset = file.createDataSet("2d_dataset", PredType::NATIVE_DOUBLE, dataspace);
            
            // Write data
            std::vector<double> data = {1.1, 2.2, 3.3, 4.4, 5.5, 6.6};
            dataset.write(data.data(), PredType::NATIVE_DOUBLE);
            // No permutation attribute (canonical order: [0,1])
            std::cout << "Created 2D dataset '2d_dataset' with size 2x3\n";
        }

        // 4. 2D dataset with different size (3x2, permuted dimensions)
        {
            // Define dimensions
            hsize_t dims[2] = {3, 2};
            DataSpace dataspace(2, dims);
            
            // Create dataset
            DataSet dataset = file.createDataSet("2d_dataset_permuted", PredType::NATIVE_DOUBLE, dataspace);
            
            // Write data
            std::vector<double> data = {7.7, 8.8, 9.9, 10.0, 11.1, 12.2};
            dataset.write(data.data(), PredType::NATIVE_DOUBLE);
            
            // Add permutation index attribute: [1,0] (dim1 slow, dim0 fast)
            int perm_index[2] = {1, 0};
            hsize_t attr_dims[1] = {2};
            DataSpace attr_space(1, attr_dims);
            Attribute attr = dataset.createAttribute("permutation_index", PredType::NATIVE_INT, attr_space);
            attr.write(PredType::NATIVE_INT, perm_index);
            std::cout << "Created 2D dataset '2d_dataset_permuted' with size 3x2, permutation: [1,0]\n";
        }

    } catch (H5::Exception& e) {
        std::cerr << "HDF5 error: " << e.getDetailMsg() << "\n";
        return 1;
    }

    std::cout << "HDF5 file 'dimensions.h5' created successfully.\n";
    return 0;
}
