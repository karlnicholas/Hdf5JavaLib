#include <H5Cpp.h>
#include <string>

using namespace H5;

struct EnvData {
    char site_name[20];  // Fixed-size string
    float aqi;
    double temp;
    int sample_count;
};

int main() {
    H5File file("env_monitoring.h5", H5F_ACC_TRUNC);

    // Define compound datatype
    CompType datatype(sizeof(EnvData));
    datatype.insertMember("siteName", HOFFSET(EnvData, site_name), StrType(PredType::C_S1, 20));
    datatype.insertMember("airQualityIndex", HOFFSET(EnvData, aqi), PredType::NATIVE_FLOAT);
    datatype.insertMember("temperature", HOFFSET(EnvData, temp), PredType::NATIVE_DOUBLE);
    datatype.insertMember("sampleCount", HOFFSET(EnvData, sample_count), PredType::NATIVE_INT);

    // Define dataspace (10 rows)
    hsize_t dims[1] = {10};
    DataSpace dataspace(1, dims);

    // Create dataset
    DataSet dataset = file.createDataSet("monitoring", datatype, dataspace);

    // Example data (manually filled for brevity)
    EnvData data[10] = {
        {"Station A", 45.3f, 12.5678, 25},
        {"Station B", 128.7f, -3.2145, 18},
        {"Station C", 75.9f, 8.9012, -5},
        {"Station D", 22.1f, -15.6789, 12},
        {"Station E", 310.4f, 25.4321, 30},
        {"Station A", 88.6f, -0.9876, -3},
        {"Station B", 150.2f, 18.7654, 22},
        {"Station C", 60.5f, 5.1234, 15},
        {"Station D", 99.8f, -10.4567, -8},
        {"Station E", 200.0f, 20.8901, 28}
    };

    // Write data
    dataset.write(data, datatype);

    return 0;
}