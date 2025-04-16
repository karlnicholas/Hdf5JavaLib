// writer.cpp
#include "common_cpp.h" // Should include: <H5Cpp.h>, <vector>, <string>, <cstdint>, etc.
                       // Should define: Record struct, FILE_NAME, DATASET_NAME, ATTRIBUTE_NAME, NUM_RECORDS
                       // Should declare: H5::CompType createCompoundType();

#include <iostream>
#include <cstring>     // For std::strncpy
#include <limits>      // For std::numeric_limits
#include <stdexcept>   // For std::exception
#include <type_traits> // For std::is_integral_v, std::is_signed_v
#include <H5Epublic.h> // For H5Eprint (detailed error stack)
#include <cstdint>     // Ensure standard integer types are available


// --- Simple Template for 5-Step Cycle Hitting Key Values ---
template <typename T>
T getCycledValue(int index) { // No longer needs min/max arguments
    // Compile-time check to ensure only integral types are used
    static_assert(std::is_integral_v<T>, "getCycledValue only supports integral types.");

    constexpr T min_val = std::numeric_limits<T>::min();
    constexpr T max_val = std::numeric_limits<T>::max();
    constexpr int cycleLength = 5;
    int cycleIndex = index % cycleLength;

    T result;

    switch (cycleIndex) {
        case 0: // Step 1: Minimum Value
            result = min_val;
            break;

        case 1: // Step 2: Approx -Max/2 (signed) or Max/4 (unsigned)
            if constexpr (std::is_signed_v<T>) {
                // Calculate approx -Max/2 carefully to avoid overflow/underflow issues
                // Handle cases where max_val might be 0 or small
                if (max_val <= 0) { // e.g., hypothetical signed type with only non-positives
                     result = min_val;
                } else {
                    T half_max = max_val / 2;
                    // If max_val is odd, -(max_val/2)-1 gives floor(-max/2)
                    // e.g., Max=127 -> half_max=63 -> result = -63-1 = -64 (0xC0)
                    // e.g., Max=128 -> half_max=64 -> result = -64-0 = -64 (0xC0)
                    result = -half_max - (max_val % 2 != 0 ? 1 : 0);
                    // Ensure we didn't wrap around below min_val (unlikely with this formula)
                    if (result > static_cast<T>(0)) result = min_val;
                }
            } else {
                // Approx Max/4 for unsigned
                result = max_val / 4;
            }
            break;

        case 2: // Step 3: Zero (signed) or Approx Max/2 (unsigned)
            if constexpr (std::is_signed_v<T>) {
                result = static_cast<T>(0);
            } else {
                // Approx Max/2 for unsigned
                result = max_val / 2;
            }
            break;

        case 3: // Step 4: Approx +Max/2 (signed) or Approx 3*Max/4 (unsigned)
            if constexpr (std::is_signed_v<T>) {
                 // Approx Max/2 for signed
                 result = max_val / 2;
            } else {
                // Approx 3*Max/4 for unsigned
                 // Calculate carefully to avoid intermediate overflow if max_val is huge
                 T q1 = max_val / 4;
                 result = q1 * 3; // This multiplication is safe
            }
            break;

        case 4: // Step 5: Maximum Value
        default: // Should not happen, but default needed for completeness
            result = max_val;
            break;
    }
    return result;
}


// --- Main Application Logic ---
int main() {
    try {
        // Optional: Disable automatic HDF5 error printing stack for cleaner manual handling.
        // H5::Exception::dontPrint();

        // --- HDF5 Setup ---
        H5::H5File file(FILE_NAME, H5F_ACC_TRUNC);
        H5::CompType compound_type = createCompoundType(); // Get the compound type definition

        // Define dataspace based on NUM_RECORDS
        hsize_t dims[1] = {NUM_RECORDS};
        H5::DataSpace dataspace(1, dims);

        // Create the dataset
        H5::DataSet dataset = file.createDataSet(DATASET_NAME, compound_type, dataspace);
        std::cout << "Info: Dataset '" << DATASET_NAME << "' created." << std::endl;

        // --- Add Attribute ---
        H5std_string attribute_value = "Revision: , URL: "; // Example attribute content
        H5::StrType attr_type(H5::PredType::C_S1, H5T_VARIABLE); // VLEN string attribute type
        attr_type.setCset(H5T_CSET_ASCII);
        attr_type.setStrpad(H5T_STR_NULLTERM);
        H5::DataSpace attr_space(H5S_SCALAR); // Scalar dataspace for one attribute
        H5::Attribute attribute = dataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);

        // Write attribute value (pass address of C-string pointer for VLEN)
        const char* attr_data_ptr = attribute_value.c_str();
        attribute.write(attr_type, &attr_data_ptr);
        std::cout << "Info: Attribute '" << ATTRIBUTE_NAME << "' written." << std::endl;


        // --- Data Preparation ---
        std::vector<Record> records(NUM_RECORDS);
        std::vector<std::string> varStrings(NUM_RECORDS); // To manage lifetimes of varStr data

        std::cout << "Info: Preparing " << NUM_RECORDS << " records..." << std::endl;
        for (size_t i = 0; i < NUM_RECORDS; ++i) {
            // Populate common fields
            records[i].recordId = 1000 + i; // Example record IDs
            std::strncpy(records[i].fixedStr, "FixedData", sizeof(records[i].fixedStr) - 1);
            records[i].fixedStr[sizeof(records[i].fixedStr) - 1] = '\0'; // Ensure null termination

            // Create variable length string data
            varStrings[i] = "varStr:" + std::to_string(i + 1);
            records[i].varStr = const_cast<char*>(varStrings[i].c_str()); // Assign pointer for HDF5 write

            // Populate float/double (example values - these don't use the template)
            records[i].floatVal = static_cast<float>(i) * 3.14f;
            records[i].doubleVal = static_cast<double>(i) * 2.718;

            // --- Populate cycled integer values USING THE NEW 5-STEP TEMPLATE ---
            // No need to pass min/max values anymore
            records[i].int8_Val   = getCycledValue<int8_t>(i);
            records[i].uint8_Val  = getCycledValue<uint8_t>(i);
            records[i].int16_Val  = getCycledValue<int16_t>(i);
            records[i].uint16_Val = getCycledValue<uint16_t>(i);
            records[i].int32_Val  = getCycledValue<int32_t>(i);
            records[i].uint32_Val = getCycledValue<uint32_t>(i);
            records[i].int64_Val  = getCycledValue<int64_t>(i);
            records[i].uint64_Val = getCycledValue<uint64_t>(i);

            // Populate scaledUintVal (example logic - unchanged)
            uint64_t value = ((static_cast<uint64_t>(i) + 1ULL) << 7) | ((i % 4) * 32);
            records[i].scaledUintVal = value & 0x01FFFFFFFFFFFFFFULL; // Example mask
        }
         std::cout << "Info: Record preparation complete." << std::endl;


        // --- Write Data ---
        std::cout << "Info: Writing data to dataset '" << DATASET_NAME << "'..." << std::endl;
        dataset.write(records.data(), compound_type);
        std::cout << "Info: Data written successfully." << std::endl;

        std::cout << "HDF5 file written successfully: " << FILE_NAME << std::endl;

    } catch (const H5::Exception& e) {
        // Error handling...
        std::cerr << "HDF5 Exception occurred!" << std::endl;
        std::cerr << "   Error Message: " << e.getDetailMsg() << std::endl;
        std::cerr << "   Function Stack:" << std::endl;
        H5Eprint(H5E_DEFAULT, stderr);
        return 1;
    } catch (const std::exception& e) {
        std::cerr << "Standard Exception: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown exception occurred!" << std::endl;
        return 1;
    }

    return 0; // Indicate success
}

// --- Placeholder for createCompoundType ---
// You MUST provide a real implementation for this. See previous examples.
// H5::CompType createCompoundType() { /* ... define members ... */ return dtype; }