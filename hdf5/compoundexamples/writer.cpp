#include "common_cpp.h"
#include <iostream>
#include <cstring>
#include <random>
#include <limits>

template <typename T>
T getCycledValue(int index, T minValue, T maxValue) {
    constexpr int cycleLength = 10;
    if constexpr (std::is_signed<T>::value) {
        double minD = static_cast<double>(minValue);
        double maxD = static_cast<double>(maxValue);
        double step = (maxD - minD) / (cycleLength - 1);
        return (index % cycleLength == cycleLength - 1) 
            ? maxValue 
            : static_cast<T>(minD + (index % cycleLength) * step);
    } else {
        T range = maxValue - minValue;
        T step = range / (cycleLength - 1);
        return (index % cycleLength == cycleLength - 1) 
            ? maxValue 
            : minValue + (index % cycleLength) * step;
    }
}

int main() {
    try {
        H5File file(FILE_NAME, H5F_ACC_TRUNC);
        CompType compound_type = createCompoundType();
        hsize_t dims[1] = {NUM_RECORDS};
        DataSpace dataspace(1, dims);
        DataSet dataset = file.createDataSet(DATASET_NAME, compound_type, dataspace);

        // ✅ ADD ATTRIBUTE: "GIT root revision"
        H5std_string attribute_value = "Revision: , URL: ";
        StrType attr_type(PredType::C_S1, attribute_value.size());
        DataSpace attr_space(H5S_SCALAR);
        Attribute attribute = dataset.createAttribute(ATTRIBUTE_NAME, attr_type, attr_space);
        attribute.write(attr_type, attribute_value);
        attribute.close();

        std::vector<Record> records(NUM_RECORDS);
        std::vector<std::string> varStrings(NUM_RECORDS);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist(1, 1900);

        for (size_t i = 0; i < NUM_RECORDS; ++i) {
            records[i].recordId = 1000 + i;
            std::strcpy(records[i].fixedStr, "FixedData");
            varStrings[i] = "varData:" + std::to_string(dist(gen));
            records[i].varStr.len = varStrings[i].size();
            records[i].varStr.p = (void*)varStrings[i].c_str();
            records[i].floatVal = 3.14f;
            records[i].doubleVal = 2.718;
            records[i].int8_Val   = getCycledValue<int8_t>(i, std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max());
            records[i].uint8_Val  = getCycledValue<uint8_t>(i, std::numeric_limits<uint8_t>::min(), std::numeric_limits<uint8_t>::max());
            records[i].int16_Val  = getCycledValue<int16_t>(i, std::numeric_limits<int16_t>::min(), std::numeric_limits<int16_t>::max());
            records[i].uint16_Val = getCycledValue<uint16_t>(i, std::numeric_limits<uint16_t>::min(), std::numeric_limits<uint16_t>::max());
            records[i].int32_Val  = getCycledValue<int32_t>(i, std::numeric_limits<int32_t>::min(), std::numeric_limits<int32_t>::max());
            records[i].uint32_Val = getCycledValue<uint32_t>(i, std::numeric_limits<uint32_t>::min(), std::numeric_limits<uint32_t>::max());
            records[i].int64_Val  = getCycledValue<int64_t>(i, std::numeric_limits<int64_t>::min(), std::numeric_limits<int64_t>::max());
            records[i].uint64_Val = getCycledValue<uint64_t>(i, std::numeric_limits<uint64_t>::min(), std::numeric_limits<uint64_t>::max());
            uint64_t value = ((i + 1ULL) << 7) | ((i % 4) * 32);
            records[i].bitfieldVal = value & 0x01FFFFFFFFFFFFFFULL;
        }

        dataset.write(records.data(), compound_type);
        std::cout << "HDF5 file written successfully: " << FILE_NAME << std::endl;
    }
    catch (const H5::Exception& e) {
        std::cerr << "HDF5 Exception: " << e.getDetailMsg() << std::endl;
        return 1;
    }

    return 0;
}