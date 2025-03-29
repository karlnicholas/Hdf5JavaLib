#include "hdf5.h"
#include <iostream>
#include <string>
#include <vector>
#include <cstring>

const char* FILENAME = "example.h5";
const char* DATASET = "/compound_example";

int main() {
    // Define the struct that matches our compound type
    struct Record {
        int32_t fixed_point;
        float floating_point;
        int64_t time;
        char str[16];
        uint8_t bitfield;
        uint8_t opaque;
        int32_t nested_fixed;
        hobj_ref_t reference;
        uint8_t enum_val;
        hvl_t var_len;
        int array[3];
    };

    // Fill in one example record
    Record record = {
        42,                            // Fixed-Point
        3.14f,                         // Floating-Point
        1234567890LL,                 // Time
        "Hello HDF5",                 // String (fixed-length)
        0b10101010,                   // Bit field
        0xFF,                         // Opaque
        7,                            // Compound (nested)
        {},                           // Reference (set later)
        1,                            // Enum
        {},                           // Variable-Length (set later)
        {1, 2, 3}                     // Array
    };

    // Create file and dataspace
    hid_t file = H5Fcreate(FILENAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
    hid_t space = H5Screate(H5S_SCALAR);  // One record

    // === Build compound datatype ===
    hid_t str_type = H5Tcopy(H5T_C_S1);
    H5Tset_size(str_type, 16);

    hid_t bitfield_type = H5Tcopy(H5T_STD_B8LE);
    hid_t opaque_type = H5Tcreate(H5T_OPAQUE, 1);
    H5Tset_tag(opaque_type, "byte");

    hid_t nested_type = H5Tcreate(H5T_COMPOUND, sizeof(int32_t));
    H5Tinsert(nested_type, "nested_fixed", 0, H5T_NATIVE_INT32);

    hid_t ref_type = H5T_STD_REF_OBJ;

    hid_t enum_type = H5Tcreate(H5T_ENUM, sizeof(uint8_t));
    uint8_t enum_val0 = 0, enum_val1 = 1;
    H5Tenum_insert(enum_type, "ZERO", &enum_val0);
    H5Tenum_insert(enum_type, "ONE", &enum_val1);

    hid_t vlen_type = H5Tvlen_create(H5T_NATIVE_INT);
    int vl_data[] = {10, 20, 30};
    record.var_len.len = 3;
    record.var_len.p = vl_data;

    hid_t array_type = H5Tarray_create(H5T_NATIVE_INT, 1, (hsize_t[]){3});

    hid_t compound_type = H5Tcreate(H5T_COMPOUND, sizeof(Record));
    H5Tinsert(compound_type, "fixed_point", HOFFSET(Record, fixed_point), H5T_NATIVE_INT32);
    H5Tinsert(compound_type, "floating_point", HOFFSET(Record, floating_point), H5T_NATIVE_FLOAT);
    H5Tinsert(compound_type, "time", HOFFSET(Record, time), H5T_NATIVE_INT64);
    H5Tinsert(compound_type, "string", HOFFSET(Record, str), str_type);
    H5Tinsert(compound_type, "bitfield", HOFFSET(Record, bitfield), bitfield_type);
    H5Tinsert(compound_type, "opaque", HOFFSET(Record, opaque), opaque_type);
    H5Tinsert(compound_type, "compound", HOFFSET(Record, nested_fixed), nested_type);
    H5Tinsert(compound_type, "reference", HOFFSET(Record, reference), ref_type);
    H5Tinsert(compound_type, "enum", HOFFSET(Record, enum_val), enum_type);
    H5Tinsert(compound_type, "vlen", HOFFSET(Record, var_len), vlen_type);
    H5Tinsert(compound_type, "array", HOFFSET(Record, array), array_type);

    // Write dummy object to reference
    hid_t ref_space = H5Screate(H5S_SCALAR);
    hid_t ref_dset = H5Dcreate(file, "/dummy", H5T_NATIVE_INT, ref_space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    H5Rcreate(&record.reference, file, "/dummy", H5R_OBJECT, -1);
    H5Sclose(ref_space);
    H5Dclose(ref_dset);

    // Write the compound data
    hid_t dset = H5Dcreate(file, DATASET, compound_type, space, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
    H5Dwrite(dset, compound_type, H5S_ALL, H5S_ALL, H5P_DEFAULT, &record);

    // Cleanup
    H5Dclose(dset);
    H5Sclose(space);
    H5Tclose(compound_type);
    H5Fclose(file);

    std::cout << "Done. Wrote " << FILENAME << std::endl;
    return 0;
}
