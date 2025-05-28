// reader.cpp
#include "common_cpp.h"
#include <iostream>
#include <iomanip>
#include <vector> // Required for std::vector
#include <algorithm> // Required for std::min

int main() {
    try {
        H5File file(FILE_NAME, H5F_ACC_RDONLY);
        DataSet dataset = file.openDataSet(DATASET_NAME);
        DataSpace filespace = dataset.getSpace(); // Renamed to filespace for clarity
        hsize_t dims[1];
        filespace.getSimpleExtentDims(dims);
        hsize_t numRecordsInFile = dims[0]; // Renamed for clarity
        std::cout << "Total number of records in file: " << numRecordsInFile << std::endl;

        CompType compoundType = createCompoundType(); // Memory type
        
        // Determine how many records to read (max 10 or fewer if file is smaller)
        const hsize_t recordsToRead = std::min<hsize_t>(10, numRecordsInFile);
        if (recordsToRead == 0) {
            std::cout << "No records to read." << std::endl;
            return 0;
        }
        std::vector<Record> records(recordsToRead);

        // Define hyperslab for file dataspace
        hsize_t offset[1] = {0};
        hsize_t count[1] = {recordsToRead};
        filespace.selectHyperslab(H5S_SELECT_SET, count, offset);
        
        // Define memory dataspace
        DataSpace memspace(1, count); // Memory dataspace for the records to be read

        dataset.read(records.data(), compoundType, memspace, filespace);

        std::cout << "\nFirst " << recordsToRead << " records:\n";
        std::cout << std::fixed << std::setprecision(7); // Set precision for floating point numbers

        for (hsize_t i = 0; i < recordsToRead; ++i) {
            // The HDF5 library, due to setPrecision(57) and setOffset(7) on NATIVE_UINT64 for scaledUintVal,
            // reads the (original_writer_loop_idx + 1) value into records[i].scaledUintVal.
            // The calculation below interprets this value as if it were packed.
            double interpretedScaledValue = (static_cast<double>(records[i].scaledUintVal >> 7)) +
                                          (static_cast<double>(records[i].scaledUintVal & 0x7F) / 128.0);

            std::cout << "Record " << i << ":\n";
            std::cout << "  recordId: " << records[i].recordId << "\n";
            std::cout << "  fixedStr: " << records[i].fixedStr << "\n";
            std::cout << "  varStr: ";
            if (records[i].varStr != nullptr) { // For const char*, check for null
                std::cout << records[i].varStr;
            } else {
                std::cout << "(empty/null)";
            }
            std::cout << "\n";
            std::cout << "  floatVal: " << records[i].floatVal << "\n";
            std::cout << "  doubleVal: " << records[i].doubleVal << "\n";
            std::cout << "  int8_Val: " << static_cast<int>(records[i].int8_Val) << "\n"; // Cast to int for printing
            std::cout << "  uint8_Val: " << static_cast<unsigned int>(records[i].uint8_Val) << "\n"; // Cast to unsigned int
            std::cout << "  int16_Val: " << records[i].int16_Val << "\n";
            std::cout << "  uint16_Val: " << records[i].uint16_Val << "\n";
            std::cout << "  int32_Val: " << records[i].int32_Val << "\n";
            std::cout << "  uint32_Val: " << records[i].uint32_Val << "\n";
            std::cout << "  int64_Val: " << records[i].int64_Val << "\n";
            std::cout << "  uint64_Val: " << records[i].uint64_Val << "\n";
            std::cout << "  scaledUintVal (raw value as read): " << records[i].scaledUintVal << "\n";
            std::cout << "  scaledUintVal (interpreted as per formula): " << interpretedScaledValue << "\n\n";
        }

        // Reclaim variable-length string data
        // IMPORTANT: Use memspace.getId() for the dataspace ID
        herr_t status = H5Dvlen_reclaim(compoundType.getId(), memspace.getId(), H5P_DEFAULT, records.data());
        if (status < 0) {
            std::cerr << "Warning: H5Dvlen_reclaim failed." << std::endl;
        } else {
            std::cout << "Successfully reclaimed memory for variable-length string data.\n";
        }
        
        std::cout << "Successfully read and printed the first " << recordsToRead << " records.\n";
    }
    catch (const H5::FileIException& e) {
        std::cerr << "HDF5 File Error: " << e.getDetailMsg() << std::endl;
        // e.printErrorStack(); // More detailed HDF5 error stack
        return 1;
    }
    catch (const H5::DataSetIException& e) {
        std::cerr << "HDF5 Dataset Error: " << e.getDetailMsg() << std::endl;
        // e.printErrorStack();
        return 1;
    }
    catch (const H5::DataSpaceIException& e) {
        std::cerr << "HDF5 DataSpace Error: " << e.getDetailMsg() << std::endl;
        // e.printErrorStack();
        return 1;
    }
    catch (const H5::DataTypeIException& e) {
        std::cerr << "HDF5 DataType Error: " << e.getDetailMsg() << std::endl;
        // e.printErrorStack();
        return 1;
    }
    catch (const H5::Exception& e) { // Catch-all for other HDF5 exceptions
        std::cerr << "HDF5 Exception: " << e.getDetailMsg() << std::endl;
        // e.printErrorStack();
        return 1;
    }
    catch (const std::exception& e) {
        std::cerr << "Standard Library Exception: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}