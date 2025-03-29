#include <H5Cpp.h>
#include <H5Tpublic.h>
#include <cstring>
#include <iostream>

using namespace H5;
using namespace std;

struct Record {
    int32_t fixed_point;         // 4 bytes, offset 0
    float floating_point;        // 4 bytes, offset 4
    int64_t time;                // 8 bytes, offset 8
    char string[16];             // 16 bytes, offset 16
    uint8_t bit_field;           // 1 byte, offset 32
    uint8_t opaque[4];           // 4 bytes, offset 33
    struct {
        int16_t nested_int;      // 2 bytes, offset 0 (relative)
        double nested_double;    // 8 bytes, offset 8 (padded)
    } compound;                  // 16 bytes total, offset 40
    hobj_ref_t reference;        // 8 bytes, offset 56
    int enumerated;              // 4 bytes, offset 64
    int array[3];                // 12 bytes, offset 68
    hvl_t variable_length;       // 16 bytes, offset 80
};

int main() {
    cout << "sizeof(Record): " << sizeof(Record) << endl;
    cout << "Offsets: " << HOFFSET(Record, fixed_point) << ", " << HOFFSET(Record, floating_point) << ", "
         << HOFFSET(Record, time) << ", " << HOFFSET(Record, string) << ", " << HOFFSET(Record, bit_field) << ", "
         << HOFFSET(Record, opaque) << ", " << HOFFSET(Record, compound) << ", " << HOFFSET(Record, compound.nested_int) << ", "
         << HOFFSET(Record, compound.nested_double) << ", " << HOFFSET(Record, reference) << ", " << HOFFSET(Record, enumerated) << ", "
         << HOFFSET(Record, array) << ", " << HOFFSET(Record, variable_length) << endl;

    try {
        H5File file("compound_example.h5", H5F_ACC_TRUNC);

        // Create a simple dataset in the root group (like /dummy in example.h5)
        hsize_t dims[1] = {1};
        DataSpace scalar_space(1, dims);
        DataSet dummy_dataset = file.createDataSet("/dummy", PredType::NATIVE_INT, scalar_space);
        int dummy_value = 0;
        dummy_dataset.write(&dummy_value, PredType::NATIVE_INT);

        // Define the compound datatype
        CompType compound_type(sizeof(Record));

        compound_type.insertMember("fixed_point", HOFFSET(Record, fixed_point), PredType::NATIVE_INT32);
        compound_type.insertMember("floating_point", HOFFSET(Record, floating_point), PredType::NATIVE_FLOAT);
        compound_type.insertMember("time", HOFFSET(Record, time), PredType::NATIVE_INT64);
        StrType str_type(PredType::C_S1, 16);
        compound_type.insertMember("string", HOFFSET(Record, string), str_type);
        IntType bitfield_type(H5Tcopy(H5T_STD_B8LE));
        compound_type.insertMember("bit_field", HOFFSET(Record, bit_field), bitfield_type);
        DataType opaque_type(H5T_OPAQUE, 4);
        H5Tset_tag(opaque_type.getId(), "4-byte opaque data");
        compound_type.insertMember("opaque", HOFFSET(Record, opaque), opaque_type);
        CompType nested_type(static_cast<size_t>(16));
        nested_type.insertMember("nested_int", 0, PredType::NATIVE_INT16);
        nested_type.insertMember("nested_double", 8, PredType::NATIVE_DOUBLE);
        compound_type.insertMember("compound", HOFFSET(Record, compound), nested_type);
        compound_type.insertMember("reference", HOFFSET(Record, reference), PredType::STD_REF_OBJ);
        EnumType enum_type(PredType::NATIVE_INT);
        int val1 = 0, val2 = 1, val3 = 2;
        enum_type.insert("LOW", &val1);
        enum_type.insert("MEDIUM", &val2);
        enum_type.insert("HIGH", &val3);
        compound_type.insertMember("enumerated", HOFFSET(Record, enumerated), enum_type);
        hsize_t array_dims[1] = {3};
        ArrayType array_type(PredType::NATIVE_INT, 1, array_dims);
        compound_type.insertMember("array", HOFFSET(Record, array), array_type);
        VarLenType vlen_type(PredType::NATIVE_INT);
        compound_type.insertMember("variable_length", HOFFSET(Record, variable_length), vlen_type);

        // Create the dataset with the compound type
        DataSpace dataspace(1, dims);
        DataSet dataset = file.createDataSet("/myDataset", compound_type, dataspace);

        // Prepare one record of data
        Record data{};
        data.fixed_point = 42;
        data.floating_point = 3.14f;
        data.time = 1698765432;
        strcpy(data.string, "Hello HDF5!");
        data.bit_field = 0b10101010; // 170, stored as a bitfield
        memcpy(data.opaque, "ABCD", 4);
        data.compound.nested_int = 123;
        data.compound.nested_double = 2.718;
        hid_t file_id = file.getId();
        H5Rcreate(&data.reference, file_id, "/dummy", H5R_OBJECT, -1); // Point to /dummy dataset
        data.enumerated = 1;
        int vlen_data[3] = {10, 20, 30};
        data.variable_length.p = vlen_data;
        data.variable_length.len = 3;
        data.array[0] = 1;
        data.array[1] = 2;
        data.array[2] = 3;

        dataset.write(&data, compound_type);

        cout << "HDF5 file created successfully with one compound record!" << endl;
    }
    catch (Exception& e) {
        cerr << "Error: " << e.getDetailMsg() << endl;
        return 1;
    }

    return 0;
}