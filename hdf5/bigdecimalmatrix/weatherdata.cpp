#include <H5Cpp.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

const H5std_string FILE_NAME("weather_data.h5");
const H5std_string DATA_DATASET("Data");

int main() {
    try {
        // Open and read the CSV file (unchanged)
        std::ifstream csvFile("weatherdata.csv");
        if (!csvFile.is_open()) {
            throw std::runtime_error("Could not open weatherdata.csv");
        }

        std::vector<std::string> headers;
        std::vector<std::vector<uint32_t>> data;
        std::string line;

        if (std::getline(csvFile, line)) {
            std::stringstream ss(line);
            std::string header;
            while (std::getline(ss, header, ',')) {
                headers.push_back(header);
            }
        }

        while (std::getline(csvFile, line)) {
            std::stringstream ss(line);
            std::string field;
            std::vector<uint32_t> row;
            for (int i = 0; i < 17; ++i) {
                std::getline(ss, field, ',');
                double value = std::stod(field);
                row.push_back(static_cast<uint32_t>(value * 128.0 + 0.5));
            }
            data.push_back(row);
        }
        csvFile.close();

        // Create HDF5 file
        H5::H5File file(FILE_NAME, H5F_ACC_TRUNC);

        // Write Data dataset
        hsize_t dataDims[2] = {data.size(), data[0].size()};
        H5::DataSpace dataSpace(2, dataDims);

        // Define fixed-point datatype
        hid_t nativeType = H5Tcopy(H5T_NATIVE_UINT32);
        H5Tset_precision(nativeType, 25);       // 25 significant bits (32 - 7)
        H5Tset_offset(nativeType, 7);           // 7 fractional bits
        H5Tset_size(nativeType, 4);             // Force 4 bytes (32 bits total)
        H5Tset_order(nativeType, H5T_ORDER_LE);
        H5Tset_pad(nativeType, H5T_PAD_ZERO, H5T_PAD_ZERO);
        H5::DataType dataType(nativeType);

        // Create and write dataset
        H5::DataSet dataDataset = file.createDataSet(DATA_DATASET, dataType, dataSpace);
        std::vector<uint32_t> flatData;
        for (const auto& row : data) {
            flatData.insert(flatData.end(), row.begin(), row.end());
        }
        dataDataset.write(flatData.data(), dataType);
        dataDataset.close();

        H5Tclose(nativeType);
        file.close();

        std::cout << "HDF5 file '" << FILE_NAME << "' created successfully.\n";

    } catch (H5::Exception& error) {
        std::cerr << "HDF5 Exception: " << error.getDetailMsg() << std::endl;
        return -1;
    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return -1;
    }

    return 0;
}