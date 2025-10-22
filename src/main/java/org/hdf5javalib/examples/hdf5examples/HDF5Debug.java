package org.hdf5javalib.examples.hdf5examples;

import org.hdf5javalib.dataclass.HdfFloatPoint;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.hdfjava.HdfFileReader;
import org.hdf5javalib.utils.HdfDisplayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.OptionalDouble;

/**
 * Demonstrates reading and processing compound data from an HDF5 file.
 * <p>
 * The {@code CompoundRead} class serves as an example application that reads
 * a compound dataset from an HDF5 file, processes it using a {@link TypedDataSource},
 * and displays the results. It showcases filtering and mapping operations on the
 * dataset, as well as conversion to a custom Java class.
 * </p>
 */
public class HDF5Debug {
    private static final Logger log = LoggerFactory.getLogger(HDF5Debug.class);
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new HDF5Debug().run();
    }

    /**
     * Executes the main logic of reading and displaying compound data from an HDF5 file.
     */
    private void run() {
        try {
            // List all .h5 files in HDF5Examples resources directory
            // ATL03_20250302235544_11742607_006_01
//            Path dirPath = Paths.get(Objects.requireNonNull(HDF5Debug.class.getClassLoader().getResource("HDF5Examples/h5ex_g_compact2.h5")).toURI());
//            Path dirPath = Paths.get("c:/users/karln/Downloads/ATL03_20250302235544_11742607_007_01.h5");
//            Path dirPath = Paths.get("c:/users/karln/Downloads/ATL03_20250302235544_11742607_006_01.h5");
//            Path dirPath = Paths.get("c:/users/karln/Downloads/SMAP_L1B_TB_57204_D_20251016T224815_R19240_001.h5");
//            Path dirPath = Paths.get("c:/users/karln/Downloads/ATL08_20250610011615_13002704_007_01.h5");
            Path dirPath = Paths.get("c:/users/karln/Downloads/SMAP_L2_SM_P_55348_A_20250612T001323_R19240_001.h5");


            processFile(dirPath);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
    // Generalized method to process the file and apply a custom action per dataset
    private static void processFile(Path filePath) {
        try (SeekableByteChannel channel = Files.newByteChannel(filePath, StandardOpenOption.READ)) {
            HdfFileReader reader = new HdfFileReader(channel).readFile();

            for (HdfDataset dataSet : reader.getDatasets()) {
                System.out.println("{} " + dataSet);
//                log.info("{} ", dataSet);
                HdfDisplayUtils.displayData(channel, dataSet, reader, HdfDisplayUtils.DisplayMode.SUMMARY_STATS);
//                displayScalarData(channel, dataSet, HdfFloatPoint.class, reader);
            }

//            HdfDataset dataSet = reader.getDataset("/gt1l/heights/delta_time").get();
//            System.out.println("{} " + dataSet);
////                System.out.println("{} " + dataSet.getObjectPath());
////                log.info("{} ", dataSet);
//                HdfDisplayUtils.displayData(channel, dataSet, reader, HdfDisplayUtils.DisplayMode.SUMMARY_STATS);
////                displayScalarData(channel, dataSet, HdfFloatPoint.class, reader);

        } catch (Exception e) {
            log.error("Exception in processFile: {}", filePath, e);
        }
    }

    public static <T> void displayScalarData(SeekableByteChannel fileChannel, HdfDataset dataSet, Class<T> clazz, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        TypedDataSource<T> dataSource = new TypedDataSource<>(fileChannel, hdfDataFile, dataSet, clazz);

        T[] resultR = dataSource.readFlattened();
        System.out.println(resultR.length);
        OptionalDouble max = Arrays.stream(resultR).mapToDouble(h -> {
                    try {
                        return ((HdfFloatPoint)h).getInstance(Double.class);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                })
                .max();
        System.out.println("max = " + max.orElseThrow());

//        T result = dataSource.streamFlattened().findFirst().orElseThrow();
//        log.info("{} stream = {}", dataSet.getObjectName(), displayValue(result));
//        dataSource.streamFlattened().limit(10).forEach(result->System.out.println(result));
//        OptionalDouble max = dataSource.streamFlattened().mapToDouble(h -> {
//                    try {
//                        return ((HdfFloatPoint)h).getInstance(Double.class);
//                    } catch (Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                })
//                .max();
//        System.out.println("max = " + max.orElseThrow());
    }

}