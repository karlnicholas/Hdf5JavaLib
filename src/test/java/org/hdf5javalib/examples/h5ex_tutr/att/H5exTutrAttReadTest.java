package org.hdf5javalib.examples.h5ex_tutr.att;

import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.examples.ResourceLoader;
import org.hdf5javalib.hdffile.dataobjects.messages.AttributeMessage;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.SeekableByteChannel;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class H5exTutrAttReadTest {

    private double[][] toDoubleMatrix(HdfData[][] data) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        double[][] res = new double[data.length][data[0].length];
        for (int i = 0; i < data.length; i++) {
            for (int j = 0; j < data[i].length; j++) {
                res[i][j] = data[i][j].getInstance(Double.class);
            }
        }
        return res;
    }

    private int[] toIntArray(HdfData[] data) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        int[] res = new int[data.length];
        for (int i = 0; i < data.length; i++) {
            res[i] = data[i].getInstance(Long.class).intValue();
        }
        return res;
    }

    @Test
    void testAttributes() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/att/Attributes.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            List<HdfDataset> datasets = reader.getDatasets();
            assertEquals(1, datasets.size());
            HdfDataset ds = datasets.get(0);
            assertEquals("Dataset", ds.getObjectName());
            List<AttributeMessage> attrs = ds.getAttributeMessages();
            assertEquals(3, attrs.size());

            // Float attribute
            AttributeMessage floatAttr = attrs.stream().filter(a -> "Float attribute".equalsIgnoreCase(a.getName().toString())).findFirst().orElse(null);
            assertNotNull(floatAttr);
            assertEquals(2, floatAttr.getHdfDataHolder().getDimensionality());
            HdfData[][] floatData = floatAttr.getHdfDataHolder().getAll(HdfData[][].class);
            double[][] expectedFloat = {{-1.0, -1.0, -1.0}, {-1.0, -1.0, -1.0}};
            assertArrayEquals(expectedFloat, toDoubleMatrix(floatData));

            // Integer attribute (scalar)
            AttributeMessage intAttr = attrs.stream().filter(a -> "Integer attribute".equalsIgnoreCase(a.getName().toString())).findFirst().orElse(null);
            assertNotNull(intAttr);
            assertEquals(0, intAttr.getHdfDataHolder().getDimensionality());
            HdfData intData = intAttr.getHdfDataHolder().getAll(HdfData.class);
            assertEquals(1, intData.getInstance(Long.class).intValue());

            // Character attribute (scalar string)
            AttributeMessage charAttr = attrs.stream().filter(a -> "Character attribute".equalsIgnoreCase(a.getName().toString())).findFirst().orElse(null);
            assertNotNull(charAttr);
            assertEquals(0, charAttr.getHdfDataHolder().getDimensionality());
            HdfData charData = charAttr.getHdfDataHolder().getAll(HdfData.class);
            assertEquals("ABCD", charData.getInstance(String.class));
        }
    }

    @Test
    void testDefaultFile() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_tutr/att/default_file.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            List<HdfDataset> datasets = reader.getDatasets();
            assertFalse(datasets.isEmpty());
            for (HdfDataset ds : datasets) {
                List<AttributeMessage> attrs = ds.getAttributeMessages();
                assertEquals(1, attrs.size());
                AttributeMessage attr = attrs.get(0);
                assertEquals("attr_name", attr.getName().toString());
                assertEquals(1, attr.getHdfDataHolder().getDimensionality());
                HdfData[] data = attr.getHdfDataHolder().getAll(HdfData[].class);
                int[] actual = toIntArray(data);
                int[] expected = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};
                assertArrayEquals(expected, actual);
            }
        }
    }
}