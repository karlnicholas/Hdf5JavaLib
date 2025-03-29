#include "H5Cpp.h"
#include <iostream>

const H5std_string FILE_NAME("tictactoe_4d_state.h5");
const H5std_string DATASET_NAME("game");

int main() {
    const int X = 3, Y = 3, Z = 3, STEPS = 5;
    hsize_t dims[4] = {X, Y, Z, STEPS};
    int data[X][Y][Z][STEPS] = {0}; // Initialize to 0

    // Step 0 – Player 1: (0, 0, 0)
    data[0][0][0][0] = 1;

    // Step 1 – Player 2: (1, 0, 0)
    for (int x=0; x<X; ++x)
        for (int y=0; y<Y; ++y)
            for (int z=0; z<Z; ++z)
                data[x][y][z][1] = data[x][y][z][0];
    data[1][0][0][1] = 2;

    // Step 2 – Player 1: (0, 0, 1)
    for (int x=0; x<X; ++x)
        for (int y=0; y<Y; ++y)
            for (int z=0; z<Z; ++z)
                data[x][y][z][2] = data[x][y][z][1];
    data[0][0][1][2] = 1;

    // Step 3 – Player 2: (1, 1, 0)
    for (int x=0; x<X; ++x)
        for (int y=0; y<Y; ++y)
            for (int z=0; z<Z; ++z)
                data[x][y][z][3] = data[x][y][z][2];
    data[1][1][0][3] = 2;

    // Step 4 – Player 1: (0, 0, 2)
    for (int x=0; x<X; ++x)
        for (int y=0; y<Y; ++y)
            for (int z=0; z<Z; ++z)
                data[x][y][z][4] = data[x][y][z][3];
    data[0][0][2][4] = 1;

    try {
        H5::H5File file(FILE_NAME, H5F_ACC_TRUNC);
        H5::DataSpace dataspace(4, dims);
        H5::DataSet dataset = file.createDataSet(DATASET_NAME, H5::PredType::NATIVE_INT, dataspace);
        dataset.write(data, H5::PredType::NATIVE_INT);
        std::cout << "HDF5 file '" << FILE_NAME << "' created with dataset '" << DATASET_NAME << "'." << std::endl;
    } catch (H5::FileIException &e) {
        e.printErrorStack();
        return -1;
    } catch (H5::DataSetIException &e) {
        e.printErrorStack();
        return -1;
    } catch (H5::DataSpaceIException &e) {
        e.printErrorStack();
        return -1;
    }

    return 0;
}
