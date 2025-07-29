package org.hdf5javalib.examples.h5ex_tutr;

import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.datatype.FixedPointDatatype;
import org.hdf5javalib.examples.ResourceLoader;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;

import java.nio.channels.SeekableByteChannel;
import java.util.Arrays;
import java.util.function.IntBinaryOperator;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import org.junit.jupiter.api.Test;

public class H5exTutrReadTest {

    private int[][] toIntMatrix(HdfData[][] data) {
        int[][] res = new int[data.length][data[0].length];
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < data[i].length; j++) {
                res[i][j] = data[i][j].getInstance(Long.class).intValue();
            }
        }
        return res;
    }

    private int[] toIntArray(HdfData[] data) {
        int[] res = new int[data.length];
        for (int i = 0; i < data.length; i++) {
            res[i] = data[i].getInstance(Long.class).intValue();
        }
        return res;
    }

    private String[] toStringArray(HdfData[] data) {
        String[] res = new String[data.length];
        for (int i = 0; i < data.length; i++) {
            res[i] = data[i].getInstance(String.class);
        }
        return res;
    }

    private int[][] generateExpectedIntMatrix(int rows, int cols, IntBinaryOperator op) {
        int[][] exp = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                exp[i][j] = op.applyAsInt(i, j);
            }
        }
        return exp;
    }

    @Test
    void testCmprss() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/cmprss.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/Compressed_Data").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[][] data = dataSource.readMatrix();
            int[][] expected = generateExpectedIntMatrix(100, 20, (i, j) -> i + j);
            assertArrayEquals(expected, toIntMatrix(data));
        }
    }

    @Test
    void testDset() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/dset.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/dset").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[][] data = dataSource.readMatrix();
            int[][] expected = generateExpectedIntMatrix(4, 6, (i, j) -> i * 6 + j + 1);
            assertArrayEquals(expected, toIntMatrix(data));
        }
    }

    @Test
    void testExtend() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/extend.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/ExtendibleArray").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[][] data = dataSource.readMatrix();
            int[][] expected = new int[10][3];
            for (int i = 0; i < 3; i++) {
                Arrays.fill(expected[i], 1);
            }
            for (int i = 3; i < 10; i++) {
                expected[i][0] = 2;
                expected[i][1] = 3;
                expected[i][2] = 4;
            }
            assertArrayEquals(expected, toIntMatrix(data));
        }
    }

    @Test
    void testGroups() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/groups.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/MyGroup/Group_A/dset2").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[][] data = dataSource.readMatrix();
            int[][] expected = generateExpectedIntMatrix(2, 10, (i, j) -> j + 1);
            assertArrayEquals(expected, toIntMatrix(data));

            dataSet = reader.getDataset("/MyGroup/dset1").orElseThrow();
            dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            data = dataSource.readMatrix();
            expected = generateExpectedIntMatrix(3, 3, (i, j) -> j + 1);
            assertArrayEquals(expected, toIntMatrix(data));
        }
    }

    @Test
    void testMount2() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/mount2.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/D").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[][] data = dataSource.readMatrix();
            int[][] expected = generateExpectedIntMatrix(4, 5, (i, j) -> i + j);
            assertArrayEquals(expected, toIntMatrix(data));
        }
    }

    @Test
    void testRefere() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/refere.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/B").orElseThrow();
            dataSet = reader.getDataset("/R").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[] data1 = dataSource.readVector();
            String[] actual = toStringArray(data1);
            assertArrayEquals(new String[]{"/A", "/B"}, actual);
        }
    }

    @Test
    void testReferDeprec() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/refer_deprec.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/dataset1").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[] data = dataSource.readVector();
            int[] expected = {0, 1, 2, 3};
            assertArrayEquals(expected, toIntArray(data));

            dataSet = reader.getDataset("/references").orElseThrow();
            dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            data = dataSource.readVector();
            String[] actual = toStringArray(data);
            assertArrayEquals(new String[]{"/dataset1"}, actual);
        }
    }

    @Test
    void testReferExtern1() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/refer_extern1.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/dataset1").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[] data = dataSource.readVector();
            int[] expected = {0, 1, 2, 3};
            assertArrayEquals(expected, toIntArray(data));
        }
    }

    @Test
    void testRefReg() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/REF_REG.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/MATRIX").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[][] data = dataSource.readMatrix();
            int[][] expected = {
                    {1, 1, 2, 3, 3, 4, 5, 5, 6},
                    {1, 2, 2, 3, 4, 4, 5, 6, 6}
            };
            assertArrayEquals(expected, toIntMatrix(data));

            // For /REGION_REFERENCES, assuming the library returns specific structures
            // Skipping detailed assertion for region references as the exact structure is unknown
            // Can add if more information is available
        }
    }

    @Test
    void testSDS() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/SDS.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/IntArray").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[][] data = dataSource.readMatrix();
            int[][] expected = generateExpectedIntMatrix(5, 6, (i, j) -> i + j);
            assertArrayEquals(expected, toIntMatrix(data));
        }
    }

    private record DSCompound (Integer a_name, Double c_name, Float b_name) {}
    @Test
    void testSDScompound() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/SDScompound.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/ArrayOfStructures").orElseThrow();
            TypedDataSource<DSCompound> dataSource = new TypedDataSource<>(channel, reader, dataSet, DSCompound.class);
            DSCompound[] data = dataSource.readVector();
            DSCompound[] expected = { new DSCompound(0, 1.0, 0.0F),
                    new DSCompound(1, 0.5, 1.0F),
                    new DSCompound(2, 1.0/3, 4.0F),
                    new DSCompound(3, 1.0/4, 9.0F),
                    new DSCompound(4, 1.0/5, 16.0F),
                    new DSCompound(5, 1.0/6, 25.0F),
                    new DSCompound(6, 1.0/7, 36.0F),
                    new DSCompound(7, 1.0/8, 49.0F),
                    new DSCompound(8, 1.0/9, 64.0F),
                    new DSCompound(9, 1.0/10, 81.0F)};
            assertArrayEquals(expected, data);
        }
    }

    @Test
    void testSDSextendible() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/SDSextendible.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/ExtendibleArray").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[][] data = dataSource.readMatrix();
            int[][] expected = new int[10][5];
            for (int i = 0; i < 2; i++) {
                expected[i] = new int[]{1, 1, 1, 3, 3};
            }
            expected[2] = new int[]{1, 1, 1, 0, 0};
            for (int i = 3; i < 10; i++) {
                expected[i] = new int[]{2, 0, 0, 0, 0};
            }
            assertArrayEquals(expected, toIntMatrix(data));
        }
    }

    @Test
    void testSelect() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/Select.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/Matrix in file").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[][] data = dataSource.readMatrix();
            int[][] expected = {
                    {53, 1, 2, 0, 3, 4, 0, 5, 6, 0, 7, 8},
                    {0, 9, 10, 0, 11, 12, 0, 13, 14, 0, 15, 16},
                    {0, 17, 18, 0, 19, 20, 0, 21, 22, 0, 23, 24},
                    {0, 0, 0, 59, 0, 61, 0, 0, 0, 0, 0, 0},
                    {0, 25, 26, 0, 27, 28, 0, 29, 30, 0, 31, 32},
                    {0, 33, 34, 0, 35, 36, 67, 37, 38, 0, 39, 40},
                    {0, 41, 42, 0, 43, 44, 0, 45, 46, 0, 47, 48},
                    {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
            };
            assertArrayEquals(expected, toIntMatrix(data));
        }
    }

    @Test
    void testSubset() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/subset.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataset dataSet = reader.getDataset("/IntArray").orElseThrow();
            TypedDataSource<HdfData> dataSource = new TypedDataSource<>(channel, reader, dataSet, HdfData.class);
            HdfData[][] data = dataSource.readMatrix();
            int[][] expected = {
                    {1, 1, 1, 1, 1, 2, 2, 2, 2, 2},
                    {1, 1, 5, 5, 5, 5, 2, 2, 2, 2},
                    {1, 1, 5, 5, 5, 5, 2, 2, 2, 2},
                    {1, 1, 5, 5, 5, 5, 2, 2, 2, 2},
                    {1, 1, 1, 1, 1, 2, 2, 2, 2, 2},
                    {1, 1, 1, 1, 1, 2, 2, 2, 2, 2},
                    {1, 1, 1, 1, 1, 2, 2, 2, 2, 2},
                    {1, 1, 1, 1, 1, 2, 2, 2, 2, 2}
            };
            assertArrayEquals(expected, toIntMatrix(data));
        }
    }
}