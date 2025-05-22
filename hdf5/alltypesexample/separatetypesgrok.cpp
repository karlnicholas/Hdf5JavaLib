#include <H5Cpp.h>
#include <H5Tpublic.h>
#include <cstring>
#include <iostream>

using namespace H5;
using namespace std;

int main() {
    try {
        // Create the HDF5 file
        H5File file("alltypes_separate.h5", H5F_ACC_TRUNC);

        // True scalar dataspace (rank 0)
        DataSpace scalar_space(H5S_SCALAR);

        // 0: Fixed-Point (int32)
        DataSet fixed_point_ds = file.createDataSet("/fixed_point", PredType::NATIVE_INT32, scalar_space);
        int32_t fixed_point = 42;
        fixed_point_ds.write(&fixed_point, PredType::NATIVE_INT32);

        // 1: Floating-Point (float)
        DataSet floating_point_ds = file.createDataSet("/floating_point", PredType::NATIVE_FLOAT, scalar_space);
        float floating_point = 3.14f;
        floating_point_ds.write(&floating_point, PredType::NATIVE_FLOAT);

        // 2: Time (datatype class 2: TIME)
        // 3. Time (HDF5 Class 2 Time datatype)
        int64_t time_val = 1672531200;
        hid_t time_tid = H5Tcreate(H5T_TIME, 8); // Create Time datatype with 8-byte size
        if (time_tid < 0) {
            throw H5::DataTypeIException("H5Tcreate", "Failed to create Time datatype");
        }
        H5Tset_precision(time_tid, 64); // 64-bit precision
        H5Tset_order(time_tid, H5T_ORDER_LE); // Little-endian
        DataType time_type(time_tid); // Wrap in C++ DataType object
        file.createDataSet("/time", time_type, scalar_space).write(&time_val, time_type);
        H5Tclose(time_tid); // Clean up the raw hid_t
        
        // 3: String (16-char fixed-length)
        StrType str_type(PredType::C_S1, 16);
        DataSet string_ds = file.createDataSet("/string", str_type, scalar_space);
        char string[16] = "Hello HDF5!";
        string_ds.write(string, str_type);

        // 4: Bitfield (8-bit)
        IntType bitfield_type(H5Tcopy(H5T_STD_B8LE));
        DataSet bitfield_ds = file.createDataSet("/bit_field", bitfield_type, scalar_space);
        uint8_t bit_field = 0b10101010; // 0xaa or 170
        bitfield_ds.write(&bit_field, bitfield_type);

        // 5: Opaque (4-byte)
        DataType opaque_type(H5T_OPAQUE, 4);
        H5Tset_tag(opaque_type.getId(), "4-byte opaque data");
        DataSet opaque_ds = file.createDataSet("/opaque", opaque_type, scalar_space);
        uint8_t opaque[4] = {'A', 'B', 'C', 'D'}; // 41:42:43:44
        opaque_ds.write(opaque, opaque_type);

        // 6: Compound (nested int16 and double)
        struct Compound {
            int16_t nested_int;
            double nested_double;
        };
        CompType compound_type(sizeof(Compound));
        compound_type.insertMember("nested_int", HOFFSET(Compound, nested_int), PredType::NATIVE_INT16);
        compound_type.insertMember("nested_double", HOFFSET(Compound, nested_double), PredType::NATIVE_DOUBLE);
        DataSet compound_ds = file.createDataSet("/compound", compound_type, scalar_space);
        Compound compound = {123, 2.718};
        compound_ds.write(&compound, compound_type);

        // 7: Reference (points to /dummy)
        DataSet dummy_ds = file.createDataSet("/dummy", PredType::NATIVE_INT, scalar_space);
        int dummy_value = 0;
        dummy_ds.write(&dummy_value, PredType::NATIVE_INT);
        DataSet reference_ds = file.createDataSet("/reference", PredType::STD_REF_OBJ, scalar_space);
        hobj_ref_t reference;
        hid_t file_id = file.getId();
        H5Rcreate(&reference, file_id, "/dummy", H5R_OBJECT, -1);
        reference_ds.write(&reference, PredType::STD_REF_OBJ);

        // 8: Enumerated (int32 with 3 values)
        EnumType enum_type(PredType::NATIVE_INT);
        int val1 = 0, val2 = 1, val3 = 2;
        enum_type.insert("LOW", &val1);
        enum_type.insert("MEDIUM", &val2);
        enum_type.insert("HIGH", &val3);
        DataSet enum_ds = file.createDataSet("/enumerated", enum_type, scalar_space);
        int enumerated = 1; // MEDIUM
        enum_ds.write(&enumerated, enum_type);

        // 9: Array (3 x int32)
        hsize_t array_dims[1] = {3};
        ArrayType array_type(PredType::NATIVE_INT, 1, array_dims);
        DataSet array_ds = file.createDataSet("/array", array_type, scalar_space);
        int array[3] = {1, 2, 3};
        array_ds.write(array, array_type);

        // 10: Variable-Length (int32 sequence)
        VarLenType vlen_type(PredType::NATIVE_INT);
        DataSet vlen_ds = file.createDataSet("/variable_length", vlen_type, scalar_space);
        hvl_t vlen_data;
        int vlen_values[3] = {10, 20, 30};
        vlen_data.p = vlen_values;
        vlen_data.len = 3;
        vlen_ds.write(&vlen_data, vlen_type);

        cout << "HDF5 file 'alltypes_separate.h5' created successfully with 11 datasets!" << endl;
    }
    catch (Exception& e) {
        cerr << "Error: " << e.getDetailMsg() << endl;
        return 1;
    }
    return 0;
}