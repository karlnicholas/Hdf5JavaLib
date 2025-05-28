// writer.cpp
#include "common_cpp.h" // Includes Record (varStr is const char*), constants, createCompoundType()

#include <iostream>
#include <vector>
#include <string>
#include <cstring>     // For std::strncpy
#include <limits>      // For std::numeric_limits
#include <stdexcept>   // For std::exception
#include <type_traits> // For std::is_integral_v, std::is_signed_v, std::is_floating_point_v
#include <H5Epublic.h> // For H5Eprint
#include <cstdint>
#include <random>

// 5-step getCycledValue template from your writer.cpp
template <typename T>
T getCycledValue(int index) {
    static_assert(std::is_integral_v<T> || std::is_floating_point_v<T>, "getCycledValue supports integral and fp types.");
    constexpr T min_val = std::numeric_limits<T>::lowest(); 
    constexpr T max_val = std::numeric_limits<T>::max();
    constexpr int cycleLength = 5; 
    int cycleIndex = index % cycleLength;
    T result;

    if constexpr (std::is_floating_point_v<T>) {
        if (cycleLength <= 1) return min_val;
        long double step = (static_cast<long double>(max_val) - static_cast<long double>(min_val)) / (long double)(cycleLength -1) ;
        if (cycleIndex == cycleLength -1) return max_val;
        return static_cast<T>(static_cast<long double>(min_val) + (long double)cycleIndex * step);
    }
    else if constexpr (std::is_integral_v<T>) {
        switch (cycleIndex) {
            case 0: result = min_val; break;
            case 1: 
                if constexpr (std::is_signed_v<T>) {
                    if (max_val <= 0) { result = min_val; } 
                    else { T half_max = max_val / 2; result = -half_max - (max_val % 2 != 0 && min_val != static_cast<T>(0) ? 1 : 0); if (result > static_cast<T>(0) && min_val < static_cast<T>(0)) result = min_val; if (min_val == static_cast<T>(-max_val - 1) && half_max == (max_val / 2) ) {  if (max_val / 2 > 0 ) result = -(max_val / 2) -1; else result = min_val; }}
                } else { result = max_val / 4; }
                break;
            case 2: if constexpr (std::is_signed_v<T>) { result = static_cast<T>(0); } else { result = max_val / 2; } break;
            case 3: if constexpr (std::is_signed_v<T>) { result = max_val / 2; } else { T q1 = max_val / 4; result = q1 * 3; } break;
            case 4: default: result = max_val; break;
        }
    }
    return result;
}

int main() {
    try {
        // H5::Exception::dontPrint(); 

        // FILE_NAME, DATASET_NAME, ATTRIBUTE_NAME are H5std_string constants from common.cpp
        // initialized with your macros "compound_example.h5", "CompoundData", "GIT root revision"
        H5::H5File file(FILE_NAME, H5F_ACC_TRUNC); 
        H5::CompType compound_type = createCompoundType(); // From common.cpp

        hsize_t dims[1] = {NUM_RECORDS}; // NUM_RECORDS macro from common.h
        H5::DataSpace dataspace(1, dims);
        H5::DataSet dataset = file.createDataSet(DATASET_NAME, compound_type, dataspace);
        std::cout << "Info (writer.cpp): Dataset '" << DATASET_NAME.c_str() << "' created." << std::endl;

        // --- Add Attribute ---
        H5std_string attribute_value_content = "Revision: , URL: ";
        H5::StrType attr_type(H5::PredType::C_S1, H5T_VARIABLE); 
        attr_type.setCset(H5T_CSET_UTF8);
        attr_type.setStrpad(H5T_STR_NULLTERM);
        H5::DataSpace attr_space(H5S_SCALAR); 
        H5::Attribute attribute = dataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
        const char* attr_data_ptr = attribute_value_content.c_str();
        attribute.write(attr_type, &attr_data_ptr);
        std::cout << "Info (writer.cpp): Attribute '" << ATTRIBUTE_NAME.c_str() << "' written." << std::endl;

        // --- Data Preparation ---
        std::vector<Record> records_buffer(NUM_RECORDS); // Record::varStr is const char*
        std::vector<std::string> varStr_data_storage(NUM_RECORDS); 

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist_for_str_content(1, 2000);

        std::cout << "Info (writer.cpp): Preparing " << NUM_RECORDS << " records..." << std::endl;
        for (size_t i = 0; i < NUM_RECORDS; ++i) {
            records_buffer[i].recordId = 10000 + i; 
            std::strncpy(records_buffer[i].fixedStr, "FixedByWriterCpp", sizeof(records_buffer[i].fixedStr) - 1);
            records_buffer[i].fixedStr[sizeof(records_buffer[i].fixedStr) - 1] = '\0';

            varStr_data_storage[i] = "varStr:" + std::to_string(dist_for_str_content(gen));
            records_buffer[i].varStr = varStr_data_storage[i].c_str(); // Assign const char*

            // Populate float/double (example values - these don't use the template)
            records_buffer[i].floatVal = static_cast<float>(i) * 3.14f;
            records_buffer[i].doubleVal = static_cast<double>(i) * 2.718;
            records_buffer[i].int8_Val   = getCycledValue<int8_t>(i);
            records_buffer[i].uint8_Val  = getCycledValue<uint8_t>(i);
            records_buffer[i].int16_Val  = getCycledValue<int16_t>(i);
            records_buffer[i].uint16_Val = getCycledValue<uint16_t>(i);
            records_buffer[i].int32_Val  = getCycledValue<int32_t>(i);
            records_buffer[i].uint32_Val = getCycledValue<uint32_t>(i);
            records_buffer[i].int64_Val  = getCycledValue<int64_t>(i);
            records_buffer[i].uint64_Val = getCycledValue<uint64_t>(i);

            // Writer stores the combined bitfield value. HDF5 type is NATIVE_UINT64.
            uint64_t combined_scaled_val = ((static_cast<uint64_t>(i) + 1ULL) << 7) | ((static_cast<uint64_t>(i % 4)) * 32);
            records_buffer[i].scaledUintVal = combined_scaled_val;
        }
         std::cout << "Info (writer.cpp): Record preparation complete." << std::endl;

        std::cout << "Info (writer.cpp): Writing data to dataset '" << DATASET_NAME.c_str() << "'..." << std::endl;
        dataset.write(records_buffer.data(), compound_type);
        std::cout << "Info (writer.cpp): Data written successfully." << std::endl;

        std::cout << "HDF5 file (writer.cpp) written successfully to: " 
                  << FILE_NAME.c_str() << std::endl; // Use the H5std_string constant

    } catch (const H5::Exception& e) {
        std::cerr << "HDF5 Exception (writer.cpp) occurred!" << std::endl;
        std::cerr << "   Error Message: " << e.getDetailMsg() << std::endl;
        std::cerr << "   Function Stack:" << std::endl;
        H5Eprint(H5E_DEFAULT, stderr);
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Standard Exception (writer.cpp): " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown exception (writer.cpp) occurred!" << std::endl;
        return 1;
    }
    return 0;
}