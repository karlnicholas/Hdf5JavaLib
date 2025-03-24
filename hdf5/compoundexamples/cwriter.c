#include "common.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <limits.h>
#include <time.h>

int64_t getCycledValue(int index, int64_t minValue, int64_t maxValue, int isSigned) {
    const int cycleLength = 10;
    if (isSigned) {
        double minD = (double)minValue;
        double maxD = (double)maxValue;
        double step = (maxD - minD) / (cycleLength - 1);
        return (index % cycleLength == cycleLength - 1) 
            ? maxValue 
            : (int64_t)(minD + (index % cycleLength) * step);
    } else {
        uint64_t range = (uint64_t)maxValue - (uint64_t)minValue;
        uint64_t step = range / (cycleLength - 1);
        return (index % cycleLength == cycleLength - 1) 
            ? maxValue 
            : minValue + (index % cycleLength) * step;
    }
}

int main() {
    hid_t file_id = H5Fcreate(FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

    hid_t compound_type = H5Tcreate(H5T_COMPOUND, sizeof(struct Record));
    H5Tinsert(compound_type, "recordId", HOFFSET(struct Record, recordId), H5T_NATIVE_UINT64);
    hid_t fixed_str_type = H5Tcopy(H5T_C_S1);
    H5Tset_size(fixed_str_type, 10);
    H5Tset_strpad(fixed_str_type, H5T_STR_NULLTERM);
    H5Tinsert(compound_type, "fixedStr", HOFFSET(struct Record, fixedStr), fixed_str_type);
    hid_t var_str_type = H5Tvlen_create(H5T_C_S1);
    H5Tinsert(compound_type, "varStr", HOFFSET(struct Record, varStr), var_str_type);
    H5Tinsert(compound_type, "floatVal", HOFFSET(struct Record, floatVal), H5T_NATIVE_FLOAT);
    H5Tinsert(compound_type, "doubleVal", HOFFSET(struct Record, doubleVal), H5T_NATIVE_DOUBLE);
    H5Tinsert(compound_type, "int8_Val", HOFFSET(struct Record, int8_Val), H5T_NATIVE_INT8);
    H5Tinsert(compound_type, "uint8_Val", HOFFSET(struct Record, uint8_Val), H5T_NATIVE_UINT8);
    H5Tinsert(compound_type, "int16_Val", HOFFSET(struct Record, int16_Val), H5T_NATIVE_INT16);
    H5Tinsert(compound_type, "uint16_Val", HOFFSET(struct Record, uint16_Val), H5T_NATIVE_UINT16);
    H5Tinsert(compound_type, "int32_Val", HOFFSET(struct Record, int32_Val), H5T_NATIVE_INT32);
    H5Tinsert(compound_type, "uint32_Val", HOFFSET(struct Record, uint32_Val), H5T_NATIVE_UINT32);
    H5Tinsert(compound_type, "int64_Val", HOFFSET(struct Record, int64_Val), H5T_NATIVE_INT64);
    H5Tinsert(compound_type, "uint64_Val", HOFFSET(struct Record, uint64_Val), H5T_NATIVE_UINT64);
    hid_t bitfield_type = H5Tcopy(H5T_NATIVE_UINT64);
    H5Tset_precision(bitfield_type, 57);
    H5Tset_offset(bitfield_type, 7);
    H5Tset_size(bitfield_type, 8);
    H5Tinsert(compound_type, "bitfieldVal", HOFFSET(struct Record, bitfieldVal), bitfield_type);

    hsize_t dims[1] = {NUM_RECORDS};
    hid_t dataspace_id = H5Screate_simple(1, dims, NULL);
    hid_t dataset_id = H5Dcreate2(file_id, DATASETNAME, compound_type, dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    const char *attribute_name = "GIT root revision";
    const char *attribute_value = "Revision: , URL: ";
    hid_t attr_dataspace_id = H5Screate(H5S_SCALAR);
    hid_t attr_type_id = H5Tcopy(H5T_C_S1);
    H5Tset_size(attr_type_id, strlen(attribute_value));
    H5Tset_strpad(attr_type_id, H5T_STR_NULLTERM);
    hid_t attr_id = H5Acreate2(dataset_id, attribute_name, attr_type_id, attr_dataspace_id, H5P_DEFAULT, H5P_DEFAULT);
    H5Awrite(attr_id, attr_type_id, attribute_value);
    H5Aclose(attr_id);
    H5Tclose(attr_type_id);
    H5Sclose(attr_dataspace_id);

    struct Record *records = (struct Record *)malloc(NUM_RECORDS * sizeof(struct Record));
    if (!records) {
        fprintf(stderr, "Memory allocation failed for records\n");
        return 1;
    }

    srand(time(NULL));
    for (size_t i = 0; i < NUM_RECORDS; ++i) {
        records[i].recordId = 1000 + i;
        strcpy(records[i].fixedStr, "FixedData");

        char temp[32];
        int randNum = rand() % 1900 + 1;
        snprintf(temp, sizeof(temp), "varData:%d", randNum);
        records[i].varStr.len = strlen(temp);
        records[i].varStr.p = malloc(records[i].varStr.len + 1);
        strcpy((char*)records[i].varStr.p, temp);

        records[i].floatVal = 3.14f;
        records[i].doubleVal = 2.718;
        records[i].int8_Val   = (int8_t)getCycledValue(i, INT8_MIN, INT8_MAX, 1);
        records[i].uint8_Val  = (uint8_t)getCycledValue(i, 0, UINT8_MAX, 0);
        records[i].int16_Val  = (int16_t)getCycledValue(i, INT16_MIN, INT16_MAX, 1);
        records[i].uint16_Val = (uint16_t)getCycledValue(i, 0, UINT16_MAX, 0);
        records[i].int32_Val  = (int32_t)getCycledValue(i, INT32_MIN, INT32_MAX, 1);
        records[i].uint32_Val = (uint32_t)getCycledValue(i, 0, UINT32_MAX, 0);
        records[i].int64_Val  = getCycledValue(i, INT64_MIN, INT64_MAX, 1);
        records[i].uint64_Val = getCycledValue(i, 0, UINT64_MAX, 0);
        records[i].bitfieldVal = (((uint64_t)(i + 1) << 7) | ((i % 4) * 32)) & 0x01FFFFFFFFFFFFFFULL;
    }

    H5Dwrite(dataset_id, compound_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, records);

    H5Tclose(compound_type);
    H5Tclose(fixed_str_type);
    H5Tclose(var_str_type);
    H5Tclose(bitfield_type);
    H5Dclose(dataset_id);
    H5Sclose(dataspace_id);
    H5Fclose(file_id);
    free(records);

    printf("HDF5 file written successfully: %s\n", FILENAME);
    return 0;
}