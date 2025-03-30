#include <H5Cpp.h>
#include <vector>
#include <string>
#include <cstring>
#include <iostream>  // Added for std::cout and std::cerr

using namespace H5;

int main() {
    try {
        // Create a new HDF5 file
        H5File file("vlen_types_example.h5", H5F_ACC_TRUNC);

        // Define dataspace (H5S_SCALAR)
        hid_t space = H5Screate(H5S_SCALAR);

        // 1. VLEN Integer Array
        {
            // Create variable length type for integers
            VarLenType vlenIntType(PredType::NATIVE_INT);

            // Create dataset
            DataSet dataset = file.createDataSet("vlen_int", vlenIntType, space);

            // Prepare data
            std::vector<int> intData = {1, 2, 3, 4, 5};
            hvl_t data[1];
            data[0].len = intData.size();
            data[0].p = intData.data();

            // Write data
            dataset.write(data, vlenIntType);
        }

        // 2. VLEN Float Array
        {
            VarLenType vlenFloatType(PredType::NATIVE_FLOAT);
            DataSet dataset = file.createDataSet("vlen_float", vlenFloatType, space);

            std::vector<float> floatData = {1.1f, 2.2f, 3.3f};
            hvl_t data[1];
            data[0].len = floatData.size();
            data[0].p = floatData.data();

            dataset.write(data, vlenFloatType);
        }

        // 3. VLEN Double Array
        {
            VarLenType vlenDoubleType(PredType::NATIVE_DOUBLE);
            DataSet dataset = file.createDataSet("vlen_double", vlenDoubleType, space);

            std::vector<double> doubleData = {1.234, 5.678, 9.101};
            hvl_t data[1];
            data[0].len = doubleData.size();
            data[0].p = doubleData.data();

            dataset.write(data, vlenDoubleType);
        }

        // 4. VLEN String
        {
            StrType vlenStrType(PredType::C_S1, H5T_VARIABLE);
            DataSet dataset = file.createDataSet("vlen_string", vlenStrType, space);

            const char* strData[1] = {"Hello, Variable Length String!"};
            dataset.write(strData, vlenStrType);
        }

        // 5. VLEN Short Array
        {
            VarLenType vlenShortType(PredType::NATIVE_SHORT);
            DataSet dataset = file.createDataSet("vlen_short", vlenShortType, space);

            std::vector<short> shortData = {10, 20, 30};
            hvl_t data[1];
            data[0].len = shortData.size();
            data[0].p = shortData.data();

            dataset.write(data, vlenShortType);
        }

        std::cout << "HDF5 file created successfully with various VLEN types!" << std::endl;

    } catch (Exception& e) {
        std::cerr << "Error: " << e.getDetailMsg() << std::endl;
        return 1;
    }

    return 0;
}