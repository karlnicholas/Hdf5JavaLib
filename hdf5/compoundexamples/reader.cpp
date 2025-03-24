#include "common_cpp.h"
#include <iostream>
#include <iomanip>

int main() {
    try {
        H5File file(FILE_NAME, H5F_ACC_RDONLY);
        DataSet dataset = file.openDataSet(DATASET_NAME);
        DataSpace dataspace = dataset.getSpace();
        hsize_t dims[1];
        dataspace.getSimpleExtentDims(dims);
        hsize_t numRecords = dims[0];
        std::cout << "Total number of records in file: " << numRecords << std::endl;

        CompType compoundType = createCompoundType();
        const hsize_t recordsToRead = std::min<hsize_t>(10, numRecords);
        std::vector<Record> records(recordsToRead);

        hsize_t offset[1] = {0};
        hsize_t count[1] = {recordsToRead};
        dataspace.selectHyperslab(H5S_SELECT_SET, count, offset);
        DataSpace memspace(1, count);

        dataset.read(records.data(), compoundType, memspace, dataspace);

        std::cout << "\nFirst " << recordsToRead << " records:\n";
        std::cout << std::fixed << std::setprecision(7);
        for (hsize_t i = 0; i < recordsToRead; ++i) {
            double bitfieldValue = (records[i].bitfieldVal >> 7) + (records[i].bitfieldVal & 0x7F) / 128.0;

            std::cout << "Record " << i << ":\n";
            std::cout << "  recordId: " << records[i].recordId << "\n";
            std::cout << "  fixedStr: " << records[i].fixedStr << "\n";
            std::cout << "  varStr: ";
            if (records[i].varStr.p != nullptr && records[i].varStr.len > 0) {
                std::cout.write(static_cast<char*>(records[i].varStr.p), records[i].varStr.len);
            } else {
                std::cout << "(empty)";
            }
            std::cout << "\n";
            std::cout << "  floatVal: " << records[i].floatVal << "\n";
            std::cout << "  doubleVal: " << records[i].doubleVal << "\n";
            std::cout << "  int8_Val: " << (int)records[i].int8_Val << "\n";
            std::cout << "  uint8_Val: " << (unsigned)records[i].uint8_Val << "\n";
            std::cout << "  int16_Val: " << records[i].int16_Val << "\n";
            std::cout << "  uint16_Val: " << records[i].uint16_Val << "\n";
            std::cout << "  int32_Val: " << records[i].int32_Val << "\n";
            std::cout << "  uint32_Val: " << records[i].uint32_Val << "\n";
            std::cout << "  int64_Val: " << records[i].int64_Val << "\n";
            std::cout << "  uint64_Val: " << records[i].uint64_Val << "\n";
            std::cout << "  bitfieldVal: " << bitfieldValue << "\n\n";
        }

        H5Dvlen_reclaim(compoundType.getId(), dataspace.getId(), H5P_DEFAULT, records.data());
        std::cout << "Successfully read and printed the first " << recordsToRead << " records.\n";
    }
    catch (const H5::FileIException& e) {
        std::cerr << "File Error: " << e.getDetailMsg() << std::endl;
        return 1;
    }
    catch (const H5::DataSetIException& e) {
        std::cerr << "Dataset Error: " << e.getDetailMsg() << std::endl;
        return 1;
    }
    catch (const H5::Exception& e) {
        std::cerr << "HDF5 Exception: " << e.getDetailMsg() << std::endl;
        return 1;
    }

    return 0;
}