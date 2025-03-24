#include <H5Cpp.h>
#include <string>
#include <vector>
#include <iostream>
#include <cstring>

#ifndef H5_NO_NAMESPACE
using namespace H5;
#endif

const H5std_string ATTRIBUTE_NAME("GIT root revision");

int main() {
    try {
        // Create an HDF5 file
        H5File file("utf8_dataset.h5", H5F_ACC_TRUNC);

        // Define dataspace: 1D array with 10 elements
        hsize_t dims[1] = {10};
        DataSpace dataspace(1, dims);

        // Define fixed-length string datatype (UTF-8, 12 bytes)
        StrType datatype(PredType::C_S1, 12); // Set to 12
        datatype.setCset(H5T_CSET_UTF8);
        datatype.setStrpad(H5T_STR_NULLTERM);

        // Create dataset
        DataSet dataset = file.createDataSet("strings", datatype, dataspace);

        // ✅ ADD ATTRIBUTE: "GIT root revision"
        H5std_string attribute_value = "Revision: , URL: ";
        StrType attr_type(PredType::C_S1, attribute_value.size());
        DataSpace attr_space(H5S_SCALAR);
        Attribute attribute = dataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
        attribute.write(attr_type, attribute_value);
        attribute.close();

        // Data: "ꦠꦤ꧀ꦢ 1" to "ꦠꦤ꧀ꦢ 10"
        std::vector<std::string> data(10);
        const std::string javaneseTanda = u8"\uA9A0\uA9A4\uA9C0"; // ꦠꦤ꧀ꦢ (7 bytes)
        for (int i = 0; i < 10; i++) {
            data[i] = javaneseTanda + " " + std::to_string(i + 1); // 7 + 1 + 1or2 = 9-10 bytes
            // std::cout << "Prepared string " << i << ": " << data[i] << " (length: " << data[i].size() << ")\n";
        }

        // Convert to char array for HDF5
        char buffer[10][12]; // Set to 12
        for (int i = 0; i < 10; i++) {
            memcpy(buffer[i], data[i].c_str(), 12); // Copy 12 bytes
            // Debug bytes
            // std::cout << "Buffer " << i << ": ";
            // for (int j = 0; j < 12; j++) {
            //     printf("%02x ", (unsigned char)buffer[i][j]);
            // }
            // std::cout << "\n";
        }

        // Write data to dataset
        dataset.write(buffer, datatype);

        std::cout << "UTF-8 fixed-length dataset created successfully.\n";
    } catch (Exception& e) {
        std::cerr << "Error: " << e.getDetailMsg() << std::endl;
        return 1;
    }

    return 0;
}