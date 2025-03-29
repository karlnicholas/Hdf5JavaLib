#include "H5Cpp.h"
#include <iostream>
#include <iomanip>

using namespace H5;

const std::string FILE_NAME = "all_types_separate.h5";

void printDataTypeInfo(const DataType& dtype) {
    H5T_class_t type_class = dtype.getClass();
    std::cout << "Type class: ";
    switch (type_class) {
        case H5T_INTEGER: std::cout << "Integer"; break;
        case H5T_FLOAT: std::cout << "Float"; break;
        case H5T_STRING: std::cout << "String"; break;
        case H5T_COMPOUND: std::cout << "Compound"; break;
        case H5T_REFERENCE: std::cout << "Reference"; break;
        case H5T_ENUM: std::cout << "Enum"; break;
        case H5T_VLEN: std::cout << "Variable-Length"; break;
        case H5T_ARRAY: std::cout << "Array"; break;
        case H5T_OPAQUE: std::cout << "Opaque"; break;
        case H5T_BITFIELD: std::cout << "Bitfield"; break;
        default: std::cout << "Other"; break;
    }
    std::cout << std::endl;
}

int main() {
    H5File file(FILE_NAME, H5F_ACC_RDONLY);
    Group root = file.openGroup("/");

    hsize_t numObjs = root.getNumObjs();

    for (hsize_t i = 0; i < numObjs; ++i) {
        std::string name = root.getObjnameByIdx(i);
        H5G_obj_t type = root.getObjTypeByIdx(i);

        if (type != H5G_DATASET) continue;

        std::cout << "📄 Dataset: " << name << std::endl;

        DataSet dataset = root.openDataSet(name);
        DataType dtype = dataset.getDataType();
        printDataTypeInfo(dtype);

        H5T_class_t classType = dtype.getClass();
        DataSpace dspace = dataset.getSpace();

        // === Print first record depending on datatype
        if (classType == H5T_INTEGER) {
            int64_t val;
            dataset.read(&val, PredType::NATIVE_INT64);
            std::cout << "  Value: " << val << std::endl;
        } else if (classType == H5T_FLOAT) {
            double val;
            dataset.read(&val, PredType::NATIVE_DOUBLE);
            std::cout << "  Value: " << val << std::endl;
        } else if (classType == H5T_STRING) {
            std::string str;
            StrType strType = dataset.getStrType();
            char buf[256];
            dataset.read(buf, strType);
            std::cout << "  Value: \"" << buf << "\"" << std::endl;
        } else if (classType == H5T_COMPOUND) {
            struct Compound { int16_t a; double b; } val;
            CompType ct(sizeof(Compound));
            ct.insertMember("a", HOFFSET(Compound, a), PredType::NATIVE_INT16);
            ct.insertMember("b", HOFFSET(Compound, b), PredType::NATIVE_DOUBLE);
            dataset.read(&val, ct);
            std::cout << "  Value: { a = " << val.a << ", b = " << val.b << " }" << std::endl;
        } else if (classType == H5T_REFERENCE) {
            hobj_ref_t ref;
            dataset.read(&ref, PredType::STD_REF_OBJ);
            std::cout << "  Value: object reference (id = " << *(uint64_t*)&ref << ")" << std::endl;
        } else if (classType == H5T_ENUM) {
            uint8_t val;
            dataset.read(&val, dtype);
            std::cout << "  Value (enum index): " << static_cast<int>(val) << std::endl;
        } else if (classType == H5T_OPAQUE) {
            uint8_t buf[4] = {0};
            dataset.read(buf, dtype);
            std::cout << "  Value: 0x";
            for (int j = 0; j < 4; ++j) std::cout << std::hex << std::setw(2) << std::setfill('0') << (int)buf[j];
            std::cout << std::dec << std::endl;
        } else if (classType == H5T_VLEN) {
            hvl_t val;
            dataset.read(&val, dtype);
            std::cout << "  Value: [";
            int* arr = static_cast<int*>(val.p);
            for (size_t j = 0; j < val.len; ++j) {
                std::cout << arr[j];
                if (j + 1 < val.len) std::cout << ", ";
            }
            std::cout << "]" << std::endl;
            free(val.p);
        } else if (classType == H5T_ARRAY) {
            int arr[3];
            dataset.read(arr, dtype);
            std::cout << "  Value: [";
            for (int j = 0; j < 3; ++j) {
                std::cout << arr[j];
                if (j + 1 < 3) std::cout << ", ";
            }
            std::cout << "]" << std::endl;
        }

        std::cout << std::endl;
    }

    return 0;
}
