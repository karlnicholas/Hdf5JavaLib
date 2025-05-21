#include <H5Cpp.h>
#include <iostream>
#include <stdexcept>
#include <string>

const H5std_string FILE_NAME("compound_array.h5");
const H5std_string DATASET_NAME("records");
const int NUM_RECORDS = 10;
const int DATA_ARRAY_SIZE = 10;

// Define the compound datatype structure
struct Record {
    unsigned long recordId;
    unsigned long data[DATA_ARRAY_SIZE];
};

int main() {
    try {
        // Create the HDF5 file
        H5::H5File file(FILE_NAME, H5F_ACC_TRUNC);

        // Define the compound datatype
        H5::CompType compType(sizeof(Record));

        // Insert recordId (unsigned long, scalar)
        compType.insertMember("recordId", HOFFSET(Record, recordId), H5::PredType::NATIVE_ULONG);

        // Create array datatype for data (unsigned long, size 10)
        hsize_t arrayDims[1] = {DATA_ARRAY_SIZE};
        H5::ArrayType arrayType(H5::PredType::NATIVE_ULONG, 1, arrayDims);

        // Insert data array
        compType.insertMember("data", HOFFSET(Record, data), arrayType);

        // Create dataspace for 10 records
        hsize_t dims[1] = {NUM_RECORDS};
        H5::DataSpace dataspace(1, dims);

        // Create the dataset
        H5::DataSet dataset = file.createDataSet(DATASET_NAME, compType, dataspace);

        // Prepare 10 records
        Record records[NUM_RECORDS];
        for (int i = 0; i < NUM_RECORDS; ++i) {
            records[i].recordId = i + 1; // recordId: 1 to 10
            for (int j = 0; j < DATA_ARRAY_SIZE; ++j) {
                records[i].data[j] = (i + 1) * 100 + j; // data: e.g., 101, 102, ..., 110 for recordId=1
            }
        }

        // Write the records to the dataset
        dataset.write(records, compType);

        std::cout << "Compound dataset created successfully with 10 records.\n";

    } catch (const H5::FileIException& e) {
        std::cerr << "File error: " << e.getDetailMsg() << "\n";
        return 1;
    } catch (const H5::DataSetIException& e) {
        std::cerr << "Dataset error: " << e.getDetailMsg() << "\n";
        return 1;
    } catch (const H5::DataTypeIException& e) {
        std::cerr << "Datatype error: " << e.getDetailMsg() << "\n";
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Unexpected error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}