#include <iostream>
#include "hdf5.h"

#define FILE_NAME    "single_int_v2.h5"
#define DATASET_NAME "MyIntegerValue"

int main() {
    // Correctly name the property list id for clarity
    hid_t file_id, fapl_id, dataspace_id, dataset_id; 
    herr_t status;

    // --- Define File Creation Properties for v2+ Architecture ---

    // 1. Create a **File Access** Property List, not a File Creation one.
    fapl_id = H5Pcreate(H5P_FILE_ACCESS);
    if (fapl_id < 0) {
        std::cerr << "Error creating file access property list." << std::endl;
        return 1;
    }

    // 2. Set the library version bounds on the FAPL. This call is now correct.
    status = H5Pset_libver_bounds(fapl_id, H5F_LIBVER_LATEST, H5F_LIBVER_LATEST);
    if (status < 0) {
        std::cerr << "Error setting library version bounds." << std::endl;
        H5Pclose(fapl_id);
        return 1;
    }
    std::cout << "Configured to use latest HDF5 file format." << std::endl;


    // --- Create the HDF5 File using our new property list ---
    // 3. Pass the custom property list (fapl_id) in the FOURTH argument of H5Fcreate.
    //    The third argument (the FCPL) can now be default.
    file_id = H5Fcreate(FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
    if (file_id < 0) {
        std::cerr << "Error creating file." << std::endl;
        H5Pclose(fapl_id);
        return 1;
    }

    // --- The rest of the code remains the same ---
    int data_to_write = 42;
    dataspace_id = H5Screate(H5S_SCALAR);
    dataset_id = H5Dcreate2(file_id, DATASET_NAME, H5T_NATIVE_INT, dataspace_id,
                              H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    status = H5Dwrite(dataset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL,
                      H5P_DEFAULT, &data_to_write);

    std::cout << "Successfully created '" << FILE_NAME << "' and wrote the value "
              << data_to_write << " to dataset '" << DATASET_NAME << "'." << std::endl;

    // --- Close all HDF5 Resources ---
    // 4. IMPORTANT: Close the property list you created.
    H5Dclose(dataset_id);
    H5Sclose(dataspace_id);
    H5Pclose(fapl_id); // Close the FAPL
    H5Fclose(file_id);

    return 0;
}