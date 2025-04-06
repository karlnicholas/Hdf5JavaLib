// writer.cpp
#include "common_cpp.h" // Includes common.h, H5Cpp.h, vector, string
#include <iostream>
#include <cstring>     // For strncpy
#include <random>
#include <limits>      // For std::numeric_limits
#include <stdexcept>   // For std::exception
#include <type_traits> // For std::is_signed in getCycledValue
#include <H5Epublic.h> // For H5Eprint (detailed error stack)

// Template function definition needed in this compilation unit
template <typename T>
T getCycledValue(int index, T minValue, T maxValue) {
    // Ensure maxValue is actually greater than minValue to avoid division by zero or weird behavior
    if (minValue >= maxValue) {
        return minValue; // Or throw an exception, depending on desired behavior
    }

    constexpr int cycleLength = 10; // Number of steps in a cycle

    // Use double for intermediate calculations to avoid overflow/precision issues with large integer ranges
    double minD = static_cast<double>(minValue);
    double maxD = static_cast<double>(maxValue);
    double rangeD = maxD - minD;

    // Calculate the step size. Ensure cycleLength > 1 to avoid division by zero.
    double step = (cycleLength > 1) ? (rangeD / (cycleLength - 1)) : 0.0;

    int cycleIndex = index % cycleLength;

    if (cycleIndex == cycleLength - 1 && cycleLength > 1) {
         // Ensure the last step returns exactly maxValue
        return maxValue;
    } else {
        // Calculate the value based on the step
        double calculatedValue = minD + static_cast<double>(cycleIndex) * step;
        // Clamp to min/max before casting, just in case of floating point inaccuracies
        if (calculatedValue < minD) calculatedValue = minD;
        if (calculatedValue > maxD) calculatedValue = maxD;
        // Cast back to the target type T.
        return static_cast<T>(calculatedValue);
    }
}


// writer.cpp

// ... getCycledValue definition ...


int main() {
    try {
        // Optional: Disable automatic HDF5 error printing stack. We'll print manually on catch.
        // H5::Exception::dontPrint();

        // --- HDF5 Setup ---
        H5File file(FILE_NAME, H5F_ACC_TRUNC);
        CompType compound_type = createCompoundType();
        hsize_t dims[1] = {NUM_RECORDS};
        DataSpace dataspace(1, dims);
        DataSet dataset = file.createDataSet(DATASET_NAME, compound_type, dataspace);

        // --- Add Attribute ---
        H5std_string attribute_value = "Revision: 1234, URL: http://example.com"; // The actual string content
        StrType attr_type(PredType::C_S1, H5T_VARIABLE); // Define VLEN string type for attribute
        attr_type.setCset(H5T_CSET_ASCII);
        attr_type.setStrpad(H5T_STR_NULLTERM);
        DataSpace attr_space(H5S_SCALAR); // Scalar dataspace for a single attribute value
        Attribute attribute = dataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);

        // --- CORRECTED Attribute Write for Scalar VLEN String ---
        // 1. Get the pointer to the C-style string data
        const char* attr_data_ptr = attribute_value.c_str();
        // 2. Pass the ADDRESS of that pointer (&attr_data_ptr) to the write function
        attribute.write(attr_type, &attr_data_ptr);
        // --- End Correction ---

        // attribute object goes out of scope and closes automatically (RAII)

        // --- Data Preparation ---
        // ... (rest of the data preparation loop remains the same) ...
        std::vector<Record> records(NUM_RECORDS);
        std::vector<std::string> varStrings(NUM_RECORDS); // Manages string lifetimes
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist(1, 1900);

        std::cout << "Info: Preparing " << NUM_RECORDS << " records..." << std::endl;
        for (size_t i = 0; i < NUM_RECORDS; ++i) {
             // ... (populate record members as before) ...
            records[i].recordId = 1000 + i;
            std::strncpy(records[i].fixedStr, "FixedData", sizeof(records[i].fixedStr) - 1);
            records[i].fixedStr[sizeof(records[i].fixedStr) - 1] = '\0';
            varStrings[i] = "VariableDataString_" + std::to_string(dist(gen)) + "_Index" + std::to_string(i);
            records[i].varStr = const_cast<char*>(varStrings[i].c_str()); // Correct for dataset write
            records[i].floatVal = static_cast<float>(i) * 3.14f;
            records[i].doubleVal = static_cast<double>(i) * 2.718;
            records[i].int8_Val   = getCycledValue<int8_t>(i, std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max());
            records[i].uint8_Val  = getCycledValue<uint8_t>(i, std::numeric_limits<uint8_t>::min(), std::numeric_limits<uint8_t>::max());
            records[i].int16_Val  = getCycledValue<int16_t>(i, std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max());
            records[i].uint16_Val = getCycledValue<uint16_t>(i, std::numeric_limits<uint16_t>::min(), std::numeric_limits<uint16_t>::max());
            records[i].int32_Val  = getCycledValue<int32_t>(i, std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
            records[i].uint32_Val = getCycledValue<uint32_t>(i, std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max());
            records[i].int64_Val  = getCycledValue<int64_t>(i, std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
            records[i].uint64_Val = getCycledValue<uint64_t>(i, std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::max());
            uint64_t value = ((i + 1ULL) << 7) | ((i % 4) * 32);
            records[i].scaledUintVal = value & 0x01FFFFFFFFFFFFFFULL;
        }
         std::cout << "Info: Record preparation complete." << std::endl;


        // --- Write Data ---
        std::cout << "Info: Writing data to dataset '" << DATASET_NAME << "'..." << std::endl;
        dataset.write(records.data(), compound_type); // This part should be correct now
        std::cout << "Info: Data written successfully." << std::endl;

        std::cout << "HDF5 file written successfully: " << FILE_NAME << std::endl;

    } catch (const H5::Exception& e) {
        std::cerr << "HDF5 Exception occurred!" << std::endl;
        std::cerr << "   Error Message: " << e.getDetailMsg() << std::endl;
        std::cerr << "   Function: " << e.getFuncName() << std::endl;
        std::cerr << "Detailed HDF5 Error Stack:" << std::endl;
        H5Eprint(H5E_DEFAULT, stderr);
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Standard Exception: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown exception occurred!" << std::endl;
        return 1;
    }

    // ... (VLEN reclaim note) ...

    return 0;
}