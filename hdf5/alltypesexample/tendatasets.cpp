#include "H5Cpp.h"
#include <iostream>
#include <string>

const H5std_string FILE_NAME("scalar_datasets.h5");

int main() {
    try {
        // Create a new file using default properties.
        H5::H5File file(FILE_NAME, H5F_ACC_TRUNC);

        // Define scalar dataspace (rank 0).
        H5::DataSpace scalarSpace;

        // Create 10 datasets, each containing a single integer value from 1 to 10.
        for (int i = 1; i <= 10; ++i) {
            std::string datasetName = "dataset_" + std::to_string(i);
            H5::DataSet dataset = file.createDataSet(datasetName, H5::PredType::NATIVE_INT, scalarSpace);
            dataset.write(&i, H5::PredType::NATIVE_INT);
        }

        std::cout << "HDF5 file '" << FILE_NAME << "' created with 10 scalar datasets." << std::endl;
    } catch (H5::Exception& e) {
        std::cerr << "HDF5 Error: " << e.getDetailMsg() << std::endl;
        return 1;
    }

    return 0;
}
