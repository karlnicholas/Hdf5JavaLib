package org.hdf5javalib.examples.write;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;
import org.hdf5javalib.utils.HdfDisplayUtils;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Demonstrates writing string datasets to HDF5 files.
 * <p>
 * The {@code HdfStringWrite} class is an example application that creates HDF5 files
 * containing string vector datasets. It supports writing strings in ASCII and UTF-8
 * encodings, using both bulk and individual record writing methods. The datasets are
 * configured with specific padding types and character sets.
 * </p>
 */
public class HdfStringWrite {
    /**
     * Entry point for the application.
     *
     * @param args command-line arguments (not used)
     */
    public static void main(String[] args) {
        new HdfStringWrite().run();
    }

    /**
     * Executes the main logic of writing string datasets to HDF5 files.
     */
    private void run() {
        tryHdfApiStrings("string_ascii_all.h5", this::writeAll, StringDatatype.createClassBitField(StringDatatype.PaddingType.SPACE_PAD, StringDatatype.CharacterSet.ASCII), 8);
        tryHdfApiStrings("string_utf8_each.h5", this::writeEach, StringDatatype.createClassBitField(StringDatatype.PaddingType.NULL_TERMINATE, StringDatatype.CharacterSet.UTF8), 12);
    }

    /**
     * Creates and writes a string vector dataset to an HDF5 file.
     *
     * @param FILE_NAME     the name of the output HDF5 file
     * @param writer        the consumer function to write the string data
     * @param classBitField the bit field configuration for the string datatype
     * @param size          the size of the string datatype in bytes
     */
    private void tryHdfApiStrings(String FILE_NAME, Consumer<WriterParams> writer, BitSet classBitField, int size) {
        final StandardOpenOption[] FILE_OPTIONS = {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};
        final String DATASET_NAME = "strings";
        final int NUM_RECORDS = 10;

        try (SeekableByteChannel channel = Files.newByteChannel(new File(FILE_NAME).toPath(), FILE_OPTIONS)) {
            // Create a new HDF5 file
            HdfFile file = new HdfFile(channel);

            // Create data space
            HdfFixedPoint[] hdfDimensions = {
                    HdfWriteUtils.hdfFixedPointFromValue(NUM_RECORDS, file.getFixedPointDatatypeForLength())
            };

            DataspaceMessage dataSpaceMessage = new DataspaceMessage(1, 1, DataspaceMessage.buildFlagSet(hdfDimensions.length > 0, false), hdfDimensions, hdfDimensions, false, (byte)0, HdfFixedPointWrite.computeDataSpaceMessageSize(hdfDimensions));

            StringDatatype stringDatatype = new StringDatatype(
                    StringDatatype.createClassAndVersion(),
                    classBitField,
                    size);

            // Create dataset
            HdfDataSet dataset = file.createDataSet(DATASET_NAME, stringDatatype, dataSpaceMessage);

            HdfDisplayUtils.writeVersionAttribute(file, dataset);

            writer.accept(new HdfStringWrite.WriterParams(NUM_RECORDS, stringDatatype, dataset));

            dataset.close();
            file.close();

            // auto close
            System.out.println("HDF5 file " + FILE_NAME + " created and written successfully!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Writes string data to the dataset record by record.
     *
     * @param writerParams the parameters for writing the string data
     */
    @SneakyThrows
    private void writeEach(HdfStringWrite.WriterParams writerParams) {
        AtomicInteger countHolder = new AtomicInteger(0);
        ByteBuffer byteBuffer = ByteBuffer.allocate(writerParams.stringDatatype.getSize());
        // Write to dataset
        writerParams.dataset.write(() -> {
            int count = countHolder.getAndIncrement();
            if (count >= writerParams.NUM_RECORDS) return ByteBuffer.allocate(0);
            byteBuffer.clear();
            byte[] bytes = ("ꦠꦤ꧀" + " " + (count+1)).getBytes();
            HdfString value = new HdfString(bytes, writerParams.stringDatatype);
            value.writeValueToByteBuffer(byteBuffer);
            byteBuffer.flip();
            return byteBuffer;
        });
    }

    /**
     * Writes all string data to the dataset in bulk.
     *
     * @param writerParams the parameters for writing the string data
     */
    @SneakyThrows
    private void writeAll(HdfStringWrite.WriterParams writerParams) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(writerParams.stringDatatype.getSize() * writerParams.NUM_RECORDS);
        for(int i=0; i<writerParams.NUM_RECORDS; i++) {
            byte[] bytes = ("label " + (i + 1)).getBytes(StandardCharsets.US_ASCII);
            HdfString value = new HdfString(bytes, writerParams.stringDatatype);
            value.writeValueToByteBuffer(byteBuffer);
        }
        byteBuffer.flip();
        // Write to dataset
        writerParams.dataset.write(byteBuffer);
    }

    /**
     * Parameters for writing string data.
     */
    @AllArgsConstructor
    static class WriterParams {
        /** The number of records to write. */
        int NUM_RECORDS;

        /** The string datatype. */
        StringDatatype stringDatatype;

        /** The dataset to write to. */
        HdfDataSet dataset;
    }
}