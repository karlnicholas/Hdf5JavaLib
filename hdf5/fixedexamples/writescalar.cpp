#include <H5Cpp.h> // HDF5 C++ API
#include <cstdlib> // For rand() and srand()
#include <ctime>   // For time() to seed random number
#include <iostream>

using namespace H5;

int main() {
    // Seed the random number generator with the current time
    srand(static_cast<unsigned>(time(0)));

    // Generate a random integer (e.g., between 0 and 99)
    int random_value = rand() % 100;

    try {
        // Create a new HDF5 file (overwrites if it already exists)
        H5File file("scalar.h5", H5F_ACC_TRUNC);

        // Define the dataspace for a scalar (single value)
        hsize_t dims[1] = {1}; // Scalar is a 1-element dataset
        DataSpace dataspace(1, dims);

        // Create the dataset named "data" with integer type
        DataSet dataset = file.createDataSet("scalar", PredType::NATIVE_INT, dataspace);

        // Write the random integer to the dataset
        dataset.write(&random_value, PredType::NATIVE_INT);

        // Output the value written for confirmation
        std::cout << "Wrote random value " << random_value << " to 'data' in scalar_data.h5" << std::endl;
    }
    catch (H5::FileIException& e) {
        std::cerr << "File error: " << e.getDetailMsg() << std::endl;
        return 1;
    }
    catch (H5::DataSetIException& e) {
        std::cerr << "Dataset error: " << e.getDetailMsg() << std::endl;
        return 1;
    }
    catch (H5::DataSpaceIException& e) {
        std::cerr << "Dataspace error: " << e.getDetailMsg() << std::endl;
        return 1;
    }

    return 0;
}