#include "H5Cpp.h"
#include <iostream>
#include <vector>

const H5std_string FILE_NAME("sales_cube.h5");
const H5std_string DATASET_NAME("sales");

int main() {
    const int TIME = 3, ZIP = 3, PROD = 3;
    hsize_t dims[3] = {TIME, ZIP, PROD};

    // Allocate on heap using std::vector
    std::vector<double> data(TIME * ZIP * PROD, 0.0); // Initialize to 0.0

    // Populate with sample sales data
    for (int t = 0; t < TIME; t++) {
        for (int z = 0; z < ZIP; z++) {
            for (int p = 0; p < PROD; p++) {
                data[t * ZIP * PROD + z * PROD + p] = (t + z + p) * 100.0;
            }
        }
    }

    try {
        H5::H5File file(FILE_NAME, H5F_ACC_TRUNC);
        H5::DataSpace dataspace(3, dims);
        H5::DataSet dataset = file.createDataSet(DATASET_NAME, H5::PredType::NATIVE_DOUBLE, dataspace);
        dataset.write(data.data(), H5::PredType::NATIVE_DOUBLE);
        std::cout << "HDF5 file '" << FILE_NAME << "' created with dataset '" << DATASET_NAME << "'." << std::endl;
    } catch (H5::FileIException &e) {
        e.printErrorStack();
        return -1;
    } catch (H5::DataSetIException &e) {
        e.printErrorStack();
        return -1;
    } catch (H5::DataSpaceIException &e) {
        e.printErrorStack();
        return -1;
    }

    return 0;
}