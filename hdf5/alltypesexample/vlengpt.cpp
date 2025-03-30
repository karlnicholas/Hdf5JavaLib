#include <iostream>
#include <string>
#include <vector>
#include <hdf5.h>

const char* FILENAME = "vlen_types_example.h5";

int main() {
    hid_t file = H5Fcreate(FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

    // === VLEN Integer ===
    {
        hvl_t vlen_data;
        std::vector<int> vec = {1, 2, 3, 4};
        vlen_data.len = vec.size();
        vlen_data.p = vec.data();

        hid_t space = H5Screate(H5S_SCALAR);
        hid_t base_type = H5T_NATIVE_INT;
        hid_t vlen_type = H5Tvlen_create(base_type);

        hid_t dset = H5Dcreate(file, "vlen_int", vlen_type, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dwrite(dset, vlen_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &vlen_data);

        H5Dclose(dset);
        H5Tclose(vlen_type);
        H5Sclose(space);
    }

    // === VLEN Float ===
    {
        hvl_t vlen_data;
        std::vector<float> vec = {3.14f, 2.71f};
        vlen_data.len = vec.size();
        vlen_data.p = vec.data();

        hid_t space = H5Screate(H5S_SCALAR);
        hid_t base_type = H5T_NATIVE_FLOAT;
        hid_t vlen_type = H5Tvlen_create(base_type);

        hid_t dset = H5Dcreate(file, "vlen_float", vlen_type, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dwrite(dset, vlen_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &vlen_data);

        H5Dclose(dset);
        H5Tclose(vlen_type);
        H5Sclose(space);
    }

    // === VLEN String ===
    {
        const char* str = "Hello VLEN!";
        hid_t space = H5Screate(H5S_SCALAR);

        hid_t str_type = H5Tcopy(H5T_C_S1);
        H5Tset_size(str_type, H5T_VARIABLE);

        hid_t dset = H5Dcreate(file, "vlen_str", str_type, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
        H5Dwrite(dset, str_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &str);

        H5Dclose(dset);
        H5Tclose(str_type);
        H5Sclose(space);
    }

    H5Fclose(file);
    std::cout << "VLEN HDF5 file created: " << FILENAME << std::endl;
    return 0;
}
