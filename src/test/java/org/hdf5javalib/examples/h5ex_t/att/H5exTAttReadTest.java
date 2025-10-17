package org.hdf5javalib.examples.h5ex_t.att;

import org.hdf5javalib.dataclass.HdfCompound;
import org.hdf5javalib.dataclass.HdfCompoundMember;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.dataclass.reference.HdfReferenceInstance;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.examples.ResourceLoader;
import org.hdf5javalib.hdffile.dataobjects.messages.AttributeMessage;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.hdf5javalib.dataclass.reference.HdfDataHolder;
import org.junit.jupiter.api.Test;

import java.nio.channels.SeekableByteChannel;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class H5exTAttReadTest {

    private HdfDataHolder getAttributeHolder(HdfFileReader reader, String datasetName, String attrName) {
        HdfDataset dataSet = reader.getDataset(datasetName).orElseThrow();
        AttributeMessage attrMsg = dataSet.getAttributeMessages().stream()
                .filter(msg -> msg.getName().toString().equals(attrName))
                .findFirst()
                .orElseThrow();
        return attrMsg.getHdfDataHolder();
    }

    @Test
    void testArrayAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_arrayatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(1, holder.getDimensionality());
            assertArrayEquals(new int[]{4}, holder.getDimensions());
            HdfData[] data = holder.getAll(HdfData[].class);
            assertEquals(4, data.length);
            int[][] expected = {
                    {0, 0, 0, 0, 0, 0, -1, -2, -3, -4, 0, -2, -4, -6, -8},
                    {0, 1, 2, 3, 4, 1, 1, 1, 1, 1, 2, 1, 0, -1, -2},
                    {0, 2, 4, 6, 8, 2, 3, 4, 5, 6, 4, 4, 4, 4, 4},
                    {0, 3, 6, 9, 12, 3, 5, 7, 9, 11, 6, 7, 8, 9, 10}
            };
            for (int i = 0; i < 4; i++) {
                HdfData[] hdfValues = data[i].getInstance(HdfData[].class);
                for (int j = 0; j < hdfValues.length; j++) {
                    int value = hdfValues[j].getInstance(Long.class).intValue();
                    assertEquals(expected[i][j], value);
                }
            }
        }
    }

    @Test
    void testBitAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_bitatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(2, holder.getDimensionality());
            assertArrayEquals(new int[]{4, 7}, holder.getDimensions());
            HdfData[][] data = holder.getAll(HdfData[][].class);
            String[][] expected = {
                    {"00000000", "01010011", "10100010", "11110001", "00000000", "01010011", "10100010"},
                    {"01000100", "10010100", "11100100", "00110100", "01000100", "10010100", "11100100"},
                    {"10001000", "11011001", "00101010", "01111011", "10001000", "11011001", "00101010"},
                    {"11001100", "00011110", "01101100", "10111110", "11001100", "00011110", "01101100"}
            };
            assertEquals(4, data.length);
            for (int i = 0; i < 4; i++) {
                assertEquals(7, data[i].length);
                for (int j = 0; j < 7; j++) {
                    assertEquals(expected[i][j], data[i][j].getInstance(String.class));
                }
            }
        }
    }

    @Test
    void testCmpdAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_cmpdatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(1, holder.getDimensionality());
            assertArrayEquals(new int[]{4}, holder.getDimensions());
            HdfCompound[] data = holder.getAll(HdfCompound[].class);
            assertEquals(4, data.length);
            // First
            List<HdfCompoundMember> members0 = data[0].getMembers();
            assertEquals(4, members0.size());
            assertEquals(1153L, members0.get(0).getInstance(Long.class));
            assertEquals("Exterior (static)", members0.get(1).getInstance(String.class));
            assertEquals(1.1920392515278585E-14, members0.get(2).getInstance(Double.class), 1e-15);
            assertEquals(3.070733824170739E90, members0.get(3).getInstance(Double.class), 1e90);
            // Second
            List<HdfCompoundMember> members1 = data[1].getMembers();
            assertEquals(1184L, members1.get(0).getInstance(Long.class));
            assertEquals("Intake", members1.get(1).getInstance(String.class));
            assertEquals(-9.539767610322231E-233, members1.get(2).getInstance(Double.class), 1e-233);
            assertEquals(4.6672614692633455E-62, members1.get(3).getInstance(Double.class), 1e-62);
            // Third
            List<HdfCompoundMember> members2 = data[2].getMembers();
            assertEquals(1027L, members2.get(0).getInstance(Long.class));
            assertEquals("Intake manifold", members2.get(1).getInstance(String.class));
            assertEquals(4.667261468365516E-62, members2.get(2).getInstance(Double.class), 1e-62);
            assertEquals(7.688168983161245E284, members2.get(3).getInstance(Double.class), 1e284);
            // Fourth
            List<HdfCompoundMember> members3 = data[3].getMembers();
            assertEquals(1313L, members3.get(0).getInstance(Long.class));
            assertEquals("Exhaust manifold", members3.get(1).getInstance(String.class));
            assertEquals(-2.439312392945428E19, members3.get(2).getInstance(Double.class), 1e19);
            assertEquals(-1.4959242067111115E114, members3.get(3).getInstance(Double.class), 1e114);
        }
    }

    @Test
    void testCpxcmpdAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_cpxcmpdatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(1, holder.getDimensionality());
            assertArrayEquals(new int[]{2}, holder.getDimensions());
            HdfCompound[] compounds = holder.getAll(HdfCompound[].class);
            assertEquals(2, compounds.length);
            // First compound
            List<HdfCompoundMember> members0 = compounds[0].getMembers();
            assertEquals(6, members0.size());
            // Sensors (vlen of compound)
            HdfData sensorsData0 = members0.get(0).getInstance(HdfData.class);
            HdfData[] sensorsArray0 = (HdfData[]) sensorsData0.getInstance(Object.class);
            assertEquals(4, sensorsArray0.length);
            // Check first sensor
            HdfCompound sensor0 = (HdfCompound) sensorsArray0[0];
            List<HdfCompoundMember> sensorMembers0 = sensor0.getMembers();
            assertEquals(1153L, sensorMembers0.get(0).getInstance(Long.class));
            assertEquals("Exterior (static)", sensorMembers0.get(1).getInstance(String.class));
            assertEquals(53.23, sensorMembers0.get(2).getInstance(Double.class), 0.001);
            assertEquals(24.57, sensorMembers0.get(3).getInstance(Double.class), 0.001);
            // Second sensor
            HdfCompound sensor1 = (HdfCompound) sensorsArray0[1];
            List<HdfCompoundMember> sensorMembers1 = sensor1.getMembers();
            assertEquals(1184L, sensorMembers1.get(0).getInstance(Long.class));
            assertEquals("Intake", sensorMembers1.get(1).getInstance(String.class));
            assertEquals(55.12, sensorMembers1.get(2).getInstance(Double.class), 0.001);
            assertEquals(22.95, sensorMembers1.get(3).getInstance(Double.class), 0.001);
            // Third sensor
            HdfCompound sensor2 = (HdfCompound) sensorsArray0[2];
            List<HdfCompoundMember> sensorMembers2 = sensor2.getMembers();
            assertEquals(1027L, sensorMembers2.get(0).getInstance(Long.class));
            assertEquals("Intake manifold", sensorMembers2.get(1).getInstance(String.class));
            assertEquals(103.55, sensorMembers2.get(2).getInstance(Double.class), 0.001);
            assertEquals(31.23, sensorMembers2.get(3).getInstance(Double.class), 0.001);
            // Fourth sensor
            HdfCompound sensor3 = (HdfCompound) sensorsArray0[3];
            List<HdfCompoundMember> sensorMembers3 = sensor3.getMembers();
            assertEquals(1313L, sensorMembers3.get(0).getInstance(Long.class));
            assertEquals("Exhaust manifold", sensorMembers3.get(1).getInstance(String.class));
            assertEquals(1252.89, sensorMembers3.get(2).getInstance(Double.class), 0.001);
            assertEquals(84.11, sensorMembers3.get(3).getInstance(Double.class), 0.001);
            // Name
            assertEquals("Airplane", members0.get(1).getInstance(String.class));
            // Color
            assertEquals("Green", members0.get(2).getInstance(String.class));
            // Location
            HdfData[] location0 = members0.get(3).getInstance(HdfData[].class);
            double[] tValues = new double[]{-103234.21, 422638.78, 5996.43};
            for (int i = 0; i < location0.length; i++) {
                assertEquals(tValues[i], location0[i].getInstance(Double.class), 0.001);
            }
            // Group
            assertEquals("/Air_Vehicles", members0.get(4).getInstance(String.class));
            // Surveyed areas
            HdfReferenceInstance surveyed0 = members0.get(5).getInstance(HdfReferenceInstance.class);
            HdfDataHolder dataHolder = surveyed0.getData();
            tValues = new double[]{67.3, 67.4, 67.6};
            HdfData[] surveyData = (HdfData[]) dataHolder.getArray();
            for (int i = 0; i < surveyData.length; i++) {
                assertEquals(tValues[i], surveyData[i].getInstance(Double.class), 0.001);
            }

//            // Surveyed areas
//            HdfData surveyData0 = members0.get(5).getInstance(HdfData.class);
//            Double[] surveyArray0 = (Double[]) surveyData0.getInstance(Double[].class);
//            tValues = new double[]{67.3, 67.4, 67.6};
//            for (int i = 0; i < surveyArray0.length; i++) {
////                assertEquals(tValues[i], surveyArray0[i].getInstance(Double.class), 0.001);
//                assertEquals(tValues[i], surveyArray0[i], 0.001);
//            }
            // Second compound
            List<HdfCompoundMember> members1 = compounds[1].getMembers();
            assertEquals(6, members1.size());
            // Sensors (vlen of compound)
            HdfData sensorsData1 = members1.get(0).getInstance(HdfData.class);
            HdfData[] sensorsArray1 = (HdfData[]) sensorsData1.getInstance(Object.class);
            assertEquals(1, sensorsArray1.length);
            // First sensor
            HdfCompound sensor4 = (HdfCompound) sensorsArray1[0];
            List<HdfCompoundMember> sensorMembers4 = sensor4.getMembers();
            assertEquals(3244L, sensorMembers4.get(0).getInstance(Long.class));
            assertEquals("Roof", sensorMembers4.get(1).getInstance(String.class));
            assertEquals(83.82, sensorMembers4.get(2).getInstance(Double.class), 0.001);
            assertEquals(29.92, sensorMembers4.get(3).getInstance(Double.class), 0.001);
            // Name
            assertEquals("Automobile", members1.get(1).getInstance(String.class));
            // Color
            assertEquals("Red", members1.get(2).getInstance(String.class));
            // Location
            HdfData[] location1 = members1.get(3).getInstance(HdfData[].class);
            tValues = new double[]{326734.36, 221568.23, 432.36};
            for (int i = 0; i < location1.length; i++) {
                assertEquals(tValues[i], location1[i].getInstance(Double.class), 0.001);
            }
            // Group
            assertEquals("/Land_Vehicles", members1.get(4).getInstance(String.class));
            // Surveyed areas
            HdfReferenceInstance surveyed1 = members1.get(5).getInstance(HdfReferenceInstance.class);
            HdfDataHolder dataHolder1 = surveyed1.getData();
            HdfData[][]  surveyData1 = (HdfData[][]) dataHolder1.getArray();
            assertEquals(4, surveyData1.length);
            double[][] tValuesd = new double[4][];
            tValuesd[0] = new double[]{70.2, 70.3, 70.4};
            tValuesd[1] = new double[]{70.3, 70.4, 70.5};
            tValuesd[2] = new double[]{70.4, 70.5, 70.6};
            tValuesd[3] = new double[]{70.5, 70.6, 70.7};
            for (int i = 0; i < surveyData1.length; i++) {
                for (int j = 0; j < surveyData1[i].length; j++) {
                    assertEquals(tValuesd[i][j], surveyData1[i][j].getInstance(Double.class), 0.001);
                }
            }
        }
    }

    @Test
    void testEnumAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_enumatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(2, holder.getDimensionality());
            assertArrayEquals(new int[]{4, 7}, holder.getDimensions());
            String[][] data = holder.getAll(String[][].class);
            String[][] expected = {
                    {"SOLID", "SOLID", "SOLID", "SOLID", "SOLID", "SOLID", "SOLID"},
                    {"SOLID", "LIQUID", "GAS", "PLASMA", "SOLID", "LIQUID", "GAS"},
                    {"SOLID", "GAS", "SOLID", "GAS", "SOLID", "GAS", "SOLID"},
                    {"SOLID", "PLASMA", "GAS", "LIQUID", "SOLID", "PLASMA", "GAS"}
            };
            assertEquals(4, data.length);
            for (int i = 0; i < 4; i++) {
                assertArrayEquals(expected[i], data[i]);
            }
        }
    }

    @Test
    void testFloatAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_floatatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(2, holder.getDimensionality());
            assertArrayEquals(new int[]{4, 7}, holder.getDimensions());
            Double[][] data = holder.getAll(Double[][].class);
            double[][] expected = {
                    {0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0},
                    {2.0, 1.6666666666666665, 2.4, 3.2857142857142856, 4.222222222222222, 5.181818181818182, 6.153846153846154},
                    {4.0, 2.333333333333333, 2.8, 3.571428571428571, 4.444444444444445, 5.363636363636363, 6.3076923076923075},
                    {6.0, 3.0, 3.2, 3.857142857142857, 4.666666666666667, 5.545454545454545, 6.461538461538462}
            };
            assertEquals(4, data.length);
            for (int i = 0; i < 4; i++) {
                assertEquals(7, data[i].length);
                for (int j = 0; j < 7; j++) {
                    assertEquals(expected[i][j], data[i][j], 1e-10);
                }
            }
        }
    }

    @Test
    void testIntAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_intatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(2, holder.getDimensionality());
            assertArrayEquals(new int[]{4, 7}, holder.getDimensions());
            Long[][] data = holder.getAll(Long[][].class);
            long[][] expected = {
                    {0, -1, -2, -3, -4, -5, -6},
                    {0, 0, 0, 0, 0, 0, 0},
                    {0, 1, 2, 3, 4, 5, 6},
                    {0, 2, 4, 6, 8, 10, 12}
            };
            assertEquals(4, data.length);
            for (int i = 0; i < 4; i++) {
                assertEquals(7, data[i].length);
                for (int j = 0; j < 7; j++) {
                    assertEquals(expected[i][j], data[i][j].longValue());
                }
            }
        }
    }

    @Test
    void testObjrefAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_objrefatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(1, holder.getDimensionality());
            assertArrayEquals(new int[]{2}, holder.getDimensions());
            String[] data = holder.getAll(String[].class);
            assertArrayEquals(new String[]{"/G1", "/DS2"}, data);
            // DS2 has no data, just assert existence
            assertTrue(reader.getDataset("/DS2").isPresent());
        }
    }

    @Test
    void testOpaqueAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_opaqueatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(1, holder.getDimensionality());
            assertArrayEquals(new int[]{4}, holder.getDimensions());
            String[] data = holder.getAll(String[].class);
            assertArrayEquals(new String[]{"4F504151554530", "4F504151554531", "4F504151554532", "4F504151554533"}, data);
        }
    }

    @Test
    void testRegrefAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_regrefatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(1, holder.getDimensionality());
            assertArrayEquals(new int[]{2}, holder.getDimensions());
            HdfData[] refs = holder.getAll(HdfData[].class);
            assertEquals(2, refs.length);
            HdfReferenceInstance ref0 = refs[0].getInstance(HdfReferenceInstance.class);
            HdfData[] ref0Data = (HdfData[]) ref0.getData().getArray();
            int[] tValues = {104, 102, 53, 100};
            for (int i = 0; i < ref0Data.length; i++) {
                assertEquals(tValues[i], ref0Data[i].getInstance(Long.class).intValue());
            }
            HdfReferenceInstance ref1 = refs[1].getInstance(HdfReferenceInstance.class);
            HdfData[] ref1Data = (HdfData[]) ref1.getData().getArray();
            tValues = new int[]{84, 104, 101, 114, 111, 119, 116, 104, 101, 100, 111, 103};
            for (int i = 0; i < ref1Data.length; i++) {
                assertEquals(tValues[i], ref1Data[i].getInstance(Long.class).intValue());
            }
            // DS2 dataset data
            HdfDataset ds2 = reader.getDataset("/DS2").orElseThrow();
            TypedDataSource<Integer> strSource = new TypedDataSource<>(channel, reader, ds2, Integer.class);
            Integer[][] strData = strSource.readMatrix();
            int[][] expectedStr = {
                    {84, 104, 101, 32, 113, 117, 105, 99, 107, 32, 98, 114, 111, 119, 110, 0},
                    {102, 111, 120, 32, 106, 117, 109, 112, 115, 32, 111, 118, 101, 114, 32, 0},
                    {116, 104, 101, 32, 53, 32, 108, 97, 122, 121, 32, 100, 111, 103, 115, 0}
            };
            assertEquals(3, strData.length);
            for (int i = 0; i < 3; i++) {
                assertEquals(16, strData[i].length);
                for (int j = 0; j < 16; j++) {
                    assertEquals(expectedStr[i][j], strData[i][j].intValue());
                }
            }
        }
    }

    @Test
    void testStringAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_stringatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(1, holder.getDimensionality());
            assertArrayEquals(new int[]{4}, holder.getDimensions());
            String[] data = holder.getAll(String[].class);
            assertArrayEquals(new String[]{"Parting", "is such", "sweet  ", "sorrow."}, data);
        }
    }

    @Test
    void testVlenAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_vlenatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(1, holder.getDimensionality());
            assertArrayEquals(new int[]{2}, holder.getDimensions());
            HdfVariableLength[] data = holder.getAll(HdfVariableLength[].class);
            assertEquals(2, data.length);
            // First
            HdfData[] arr0 = (HdfData[]) data[0].getInstance(Object.class);
            assertEquals(3, arr0.length);
            assertEquals(3, arr0[0].getInstance(Integer.class).intValue());
            assertEquals(2, arr0[1].getInstance(Integer.class).intValue());
            assertEquals(1, arr0[2].getInstance(Integer.class).intValue());
            // Second
            HdfData[] arr1 = (HdfData[]) data[1].getInstance(Object.class);
            assertEquals(12, arr1.length);
            int[] expected1 = {1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144};
            for (int k = 0; k < 12; k++) {
                assertEquals(expected1[k], arr1[k].getInstance(Integer.class).intValue());
            }
        }
    }

    @Test
    void testVlstringAtt() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_t/att/h5ex_t_vlstringatt.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            HdfDataHolder holder = getAttributeHolder(reader, "/DS1", "A1");
            assertEquals(1, holder.getDimensionality());
            assertArrayEquals(new int[]{4}, holder.getDimensions());
            String[] data = holder.getAll(String[].class);
            assertArrayEquals(new String[]{"Parting", "is such", "sweet", "sorrow."}, data);
        }
    }
}

