#include <H5Cpp.h>
#include <H5Tpublic.h>
#include <H5Opublic.h>
#include <cstring>
#include <iostream>

using namespace H5;
using namespace std;

string get_type_description(const DataType& dtype) {
    H5T_class_t type_class = dtype.getClass();
    switch (type_class) {
        case H5T_INTEGER: return "Integer (" + to_string(dtype.getSize() * 8) + "-bit)";
        case H5T_FLOAT: return "Floating-point (" + to_string(dtype.getSize() * 8) + "-bit)";
        case H5T_STRING: return "String (fixed-length, " + to_string(dtype.getSize()) + " bytes)";
        case H5T_BITFIELD: return "Bitfield (8-bit)";
        case H5T_OPAQUE: return "Opaque (" + to_string(dtype.getSize()) + " bytes)";
        case H5T_COMPOUND: return "Compound";
        case H5T_REFERENCE: return "Object Reference";
        case H5T_ENUM: return "Enumerated (" + to_string(dtype.getSize() * 8) + "-bit)";
        case H5T_ARRAY: return "Array";
        case H5T_VLEN: return "Variable-length";
        default: return "Unknown";
    }
}

void print_data(const DataSet& dataset, const DataType& dtype, hid_t group_id) {
    H5T_class_t type_class = dtype.getClass();
    if (type_class == H5T_INTEGER) {
        int64_t value = 0;
        dataset.read(&value, dtype);
        cout << value;
    }
    else if (type_class == H5T_FLOAT) {
        float value = 0.0f;
        dataset.read(&value, dtype);
        cout << value;
    }
    else if (type_class == H5T_STRING) {
        char value[16] = {0};
        dataset.read(value, dtype);
        cout << "\"" << value << "\"";
    }
    else if (type_class == H5T_BITFIELD) {
        uint8_t value = 0;
        dataset.read(&value, dtype);
        cout << "0x" << hex << (unsigned int)value << dec;
    }
    else if (type_class == H5T_OPAQUE) {
        uint8_t value[4] = {0};
        dataset.read(value, dtype);
        cout << (int)value[0] << ":" << (int)value[1] << ":" << (int)value[2] << ":" << (int)value[3];
    }
    else if (type_class == H5T_COMPOUND) {
        struct Compound { int16_t nested_int; double nested_double; };
        Compound value = {0, 0.0};
        dataset.read(&value, dtype);
        cout << "{ nested_int: " << value.nested_int << ", nested_double: " << value.nested_double << " }";
    }
    else if (type_class == H5T_REFERENCE) {
        hobj_ref_t ref;
        dataset.read(&ref, PredType::STD_REF_OBJ);
        char ref_name[1024];
        ssize_t name_len = H5Rget_name(group_id, H5R_OBJECT, &ref, ref_name, 1024);
        cout << (name_len > 0 ? "DATASET " + string(ref_name) : "Invalid reference");
    }
    else if (type_class == H5T_ENUM) {
        int value = 0;
        dataset.read(&value, dtype);
        char enum_name[32];
        H5Tenum_nameof(dtype.getId(), &value, enum_name, 32);
        cout << enum_name << " (" << value << ")";
    }
    else if (type_class == H5T_ARRAY) {
        int values[3] = {0};
        dataset.read(values, dtype);
        cout << "[" << values[0] << ", " << values[1] << ", " << values[2] << "]";
    }
    else if (type_class == H5T_VLEN) {
        hvl_t vlen_data;
        dataset.read(&vlen_data, dtype);
        int* values = static_cast<int*>(vlen_data.p);
        cout << "(";
        for (size_t i = 0; i < vlen_data.len; ++i) {
            cout << values[i] << (i < vlen_data.len - 1 ? ", " : "");
        }
        cout << ")";
        DataSpace dspace = dataset.getSpace();
        H5Dvlen_reclaim(dtype.getId(), dspace.getId(), H5P_DEFAULT, &vlen_data);
    }
    else {
        cout << "Unsupported type";
    }
}

herr_t dataset_callback(hid_t group_id, const char* name, const H5L_info_t* info, void* op_data) {
    H5O_info2_t obj_info;
    if (H5Oget_info_by_name3(group_id, name, &obj_info, H5O_INFO_BASIC, H5P_DEFAULT) < 0 || obj_info.type != H5O_TYPE_DATASET) {
        return 0; // Skip non-datasets
    }

    try {
        DataSet dataset(H5Dopen(group_id, name, H5P_DEFAULT));
        DataType dtype = dataset.getDataType();
        DataSpace dspace = dataset.getSpace();

        // Check for scalar (rank 0, 1 point) or simple single-element (rank 1, 1 point)
        int ndims = dspace.getSimpleExtentNdims();
        hsize_t dims[1];
        if ((ndims == 0 && dspace.getSimpleExtentNpoints() != 1) || 
            (ndims == 1 && (dspace.getSimpleExtentDims(dims, NULL) < 0 || dims[0] != 1))) {
            cout << "Skipping " << name << ": Not a single-record dataset" << endl;
            return 0;
        }

        cout << "Dataset: " << name << endl;
        cout << "  Type: " << get_type_description(dtype) << endl;
        cout << "  Data: ";
        print_data(dataset, dtype, group_id);
        cout << endl << endl;
    }
    catch (Exception& e) {
        cerr << "Error processing " << name << ": " << e.getDetailMsg() << endl;
    }
    return 0; // Continue iteration
}

int main() {
    try {
        H5File file("alltypes_separate.h5", H5F_ACC_RDONLY);
        H5Literate(file.getId(), H5_INDEX_NAME, H5_ITER_NATIVE, NULL, dataset_callback, NULL);
        cout << "Successfully read all datasets in the root group!" << endl;
    }
    catch (Exception& e) {
        cerr << "Error: " << e.getDetailMsg() << endl;
        return 1;
    }
    return 0;
}