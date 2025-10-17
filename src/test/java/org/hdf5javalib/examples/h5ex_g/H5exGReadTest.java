package org.hdf5javalib.examples.h5ex_g;

import org.hdf5javalib.examples.ResourceLoader;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.junit.jupiter.api.Test;

import java.nio.channels.SeekableByteChannel;

import static org.junit.jupiter.api.Assertions.*;

public class H5exGReadTest {

    @Test
    void testCompact1() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_g/h5ex_g_compact1.h5")) {
            new HdfFileReader(channel).readFile();
        }
    }

    @Test
    void testCompact2() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_g/h5ex_g_compact2.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            reader.getGroup("/G1").orElseThrow();
        }
    }

    @Test
    void testCOrder() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_g/h5ex_g_corder.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            reader.getDataset("/index_group").orElseThrow().close();
        }
    }

    @Test
    void testCreate() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_g/h5ex_g_create.h5")) {
            new HdfFileReader(channel).readFile();
        }
    }

    @Test
    void testIntermediate() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_g/h5ex_g_intermediate.h5")) {
            new HdfFileReader(channel).readFile();
        }
    }

    @Test
    void testIterate() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_g/h5ex_g_iterate.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            reader.getDataset("/DS1").orElseThrow().close();
            reader.getDataset("/DT1").orElseThrow().close();
            reader.getDataset("/G1/DS2").orElseThrow().close();
            HdfDataset dataset = reader.getDataset("/L1").orElseThrow();
            assertEquals("/G1/DS2", dataset.getHardLink());
            dataset.close();
        }
    }

    @Test
    void testPhase() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_g/h5ex_g_phase.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            reader.getGroup("/G0").orElseThrow();
        }
    }

    @Test
    void testTraverse() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_g/h5ex_g_traverse.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            reader.getDataset("/group1/dset1").orElseThrow().close();
            HdfDataset dataSet = reader.getDataset("/group1/group3/dset2").orElseThrow();
            assertEquals("/group1/dset1", dataSet.getHardLink());
        }
    }

    @Test
    void testVisit() throws Exception {
        try (SeekableByteChannel channel = ResourceLoader.loadResourceAsChannel("h5ex_g/h5ex_g_visit.h5")) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();
            reader.getDataset("/group1/dset1").orElseThrow().close();
            HdfDataset dataSet = reader.getDataset("/group1/group3/dset2").orElseThrow();
            assertEquals("/group1/dset1", dataSet.getHardLink());
            dataSet.close();
        }
    }
}