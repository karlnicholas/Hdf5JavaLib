package org.hdf5javalib.examples.write;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfDisplayUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Demonstrates writing multiple scalar datasets to an HDF5 file.
 * <p>
 * The {@code HdfTwentyScalarWrite} class is an example application that creates an HDF5 file
 * containing 20 scalar datasets, each storing a 32-bit signed integer value. It uses
 * {@link HdfFile} to manage the file and {@link HdfDataSet} to write scalar data.
 * </p>
 */
public class HdfTwentyScalarWrite {
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new HdfTwentyScalarWrite().run();
    }

    /**
     * Executes the main logic of writing scalar datasets to an HDF5 file.
     */
    private void run() {
        tryHdfApiScalar("twenty_datasets.h5");
    }

    /**
     * Creates an HDF5 file and writes 20 scalar datasets.
     *
     * @param FILE_NAME the name of the output HDF5 file
     */
    private void tryHdfApiScalar(String FILE_NAME) {
        // Create a new file using default properties
        try (HdfFile hdfFile = new HdfFile(Files.newByteChannel(Path.of(FILE_NAME), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))) {
            // Define scalar dataspace (rank 0).
            HdfFixedPoint[] hdfDimensions = {};
            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 0, DataspaceMessage.buildFlagSet(false, false), hdfDimensions, hdfDimensions, false, (byte)0, HdfFixedPointWrite.computeDataSpaceMessageSize(hdfDimensions));
            for (int i = 1; i <= 20; i++) {
                writeInteger(i, "dataset_" + i, hdfFile, dataSpaceMessage);
                hdfFile.getFileAllocation().printBlocks();
            }

            hdfFile.close();
            hdfFile.getFileAllocation().printBlocksSorted();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("HDF5 file " + FILE_NAME + " created and written successfully!");
    }

    /**
     * Writes a single integer value to a scalar dataset.
     *
     * @param count         the integer value to write
     * @param datasetName   the name of the dataset
     * @param hdfFile       the HDF5 file to write to
     * @param dataSpaceMessage the dataspace message for the dataset
     * @throws IOException if an I/O error occurs
     */
    private static void writeInteger(int count, String datasetName, HdfFile hdfFile, DataspaceMessage dataSpaceMessage) throws IOException {
        FixedPointDatatype fixedPointDatatype = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, true),
                (short)4, (short)0, (short)32);
        // Create dataset
        HdfDataSet dataset = hdfFile.createDataSet(datasetName, fixedPointDatatype, dataSpaceMessage);

        ByteBuffer byteBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.putInt(count);
        byteBuffer.flip();
        dataset.write(byteBuffer);
        dataset.close();
    }
}