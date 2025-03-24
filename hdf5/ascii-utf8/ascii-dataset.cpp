#include <H5Cpp.h>
#include <string>
#include <vector>
#include <iostream>
#include <cstring> // Added for memcpy

#ifndef H5_NO_NAMESPACE
using namespace H5;
#endif

const H5std_string ATTRIBUTE_NAME("GIT root revision");

int main() {
    try {
        // Create an HDF5 file
        H5File file("ascii_dataset.h5", H5F_ACC_TRUNC);

        // Define dataspace: 1D array with 10 elements
        hsize_t dims[1] = {10};
        DataSpace dataspace(1, dims);

        // Define fixed-length string datatype (ASCII, 8 bytes for "label 10")
        StrType datatype(PredType::C_S1, 8);
        datatype.setCset(H5T_CSET_ASCII);
        datatype.setStrpad(H5T_STR_SPACEPAD); // Space-pad for consistency

        // Create dataset
        DataSet dataset = file.createDataSet("strings", datatype, dataspace);

        // ✅ ADD ATTRIBUTE: "GIT root revision"
        H5std_string attribute_value = "Revision: , URL: ";
        StrType attr_type(PredType::C_S1, attribute_value.size());
        DataSpace attr_space(H5S_SCALAR);
        Attribute attribute = dataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
        attribute.write(attr_type, attribute_value);
        attribute.close();

        // Data: "label 1" to "label 10"
        std::vector<std::string> data(10);
        for (int i = 0; i < 10; i++) {
            data[i] = "label " + std::to_string(i + 1);
            data[i].resize(8, ' '); // Pad to 8 chars with spaces
        }

        // Convert to char array for HDF5 (fixed-length strings need contiguous memory)
        char buffer[10][8];
        for (int i = 0; i < 10; i++) {
            memcpy(buffer[i], data[i].c_str(), 8); // Use memcpy without std::
        }

        // Write data to dataset
        dataset.write(buffer, datatype);

        std::cout << "ASCII fixed-length dataset created successfully.\n";
    } catch (Exception& e) {
        std::cerr << "Error: " << e.getDetailMsg() << std::endl;
        return 1;
    }

    return 0;
}