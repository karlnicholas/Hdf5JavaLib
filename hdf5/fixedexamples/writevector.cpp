#include <iostream>
#include <H5Cpp.h>
#include <random>
#include <vector>
#include <cstring>  // For memcpy

using namespace H5;

const H5std_string FILE_NAME("vector.h5");
const H5std_string DATASET_NAME("vector");
const H5std_string ATTRIBUTE_NAME("GIT root revision");
const uint32_t NUM_RECORDS = 1000;

int main() {
    try {
        // Set up random number generation
        // std::random_device rd;
        // std::mt19937 gen(rd());
        // std::uniform_int_distribution<> dis(10, 50);

        // Create vector to hold 100 random values
        std::vector<int64_t> temp_values(NUM_RECORDS);
        for (int i = 0; i < NUM_RECORDS; i++) {
            temp_values[i] = i+1;
        }

        // Create a new HDF5 file (overwrite if exists)
        H5::H5File file(FILE_NAME, H5F_ACC_TRUNC);

        // Define the data type (int64)
        H5::IntType datatype(H5::PredType::NATIVE_INT64);

        // Create dataspace for RECORDS values
        hsize_t dim[1] = { NUM_RECORDS };
        hsize_t maxdim[1] = { NUM_RECORDS }; // Explicitly fix maxdims
        DataSpace dataspace(1, dim, maxdim);

        // Create the dataset
        H5::DataSet dataset = file.createDataSet(DATASET_NAME, datatype, dataspace);

        // ✅ ADD ATTRIBUTE: "GIT root revision"
        H5std_string attribute_value = "Revision: , URL: ";
        StrType attr_type(PredType::C_S1, attribute_value.size());
        DataSpace attr_space(H5S_SCALAR);
        Attribute attribute = dataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
        attribute.write(attr_type, attribute_value);
        attribute.close();

        // Write the data
        dataset.write(temp_values.data(), H5::PredType::NATIVE_INT64);

        // Explicitly close the dataset and file
        dataset.close();
        file.close();

        std::cout << "HDF5 file '" << FILE_NAME << "' created with dataset '/temperature' containing RECORDS random values successfully.\n";

    } catch (H5::Exception &error) {
        std::cerr << "HDF5 Exception: " << error.getDetailMsg() << std::endl;
        return -1;
    }

    return 0;
}