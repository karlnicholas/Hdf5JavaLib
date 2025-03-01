package com.github.karlnicholas.hdf5javalib.data;

import com.github.karlnicholas.hdf5javalib.file.dataobject.HdfObjectHeaderPrefixV1;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A data source for reading raw fixed-point data from HDF5 files into HdfFixedPoint arrays.
 * Optimized for bulk reading and streaming of raw data without mapping to a specific class.
 *
 * <p><strong>Usage Example:</strong></p>
 * <pre>
 * FixedPointRawDataSource rawSource = new FixedPointRawDataSource(header, "temp", 0, fileChannel, dataAddress);
 * HdfFixedPoint[] rawData = rawSource.readAllRaw();
 * rawSource.stream().forEach(d -> System.out.println(d.toBigInteger()));
 * </pre>
 */
public class FixedPointRawDataSource extends AbstractFixedPointDataSource<HdfFixedPoint> {
    /**
     * Constructs a FixedPointRawDataSource for reading raw data from an HDF5 file.
     *
     * @param headerPrefixV1 the HDF5 object header prefix
     * @param name the name of the dataset field (for metadata consistency)
     * @param scale the scale for BigDecimal values; 0 for BigInteger
     * @param fileChannel the FileChannel for streaming
     * @param startOffset the byte offset where the dataset begins
     * @throws IllegalStateException if metadata is missing
     * @throws IllegalArgumentException if dimensionality is unsupported
     */
    public FixedPointRawDataSource(HdfObjectHeaderPrefixV1 headerPrefixV1, String name, int scale, FileChannel fileChannel, long startOffset) {
        super(headerPrefixV1, name, scale, fileChannel, startOffset);
    }

    /**
     * Reads the entire dataset into an array of HdfFixedPoint objects in memory.
     *
     * @return an array of HdfFixedPoint containing all records
     * @throws IOException if an I/O error occurs
     * @throws IllegalStateException if no FileChannel was provided
     */
    public HdfFixedPoint[] readAllRaw() throws IOException {
        if (fileChannel == null) {
            throw new IllegalStateException("Reading all data requires a FileChannel; use the appropriate constructor.");
        }
        long totalSize = sizeForReadBuffer * readsAvailable;
        if (totalSize > Integer.MAX_VALUE) {
            throw new IllegalStateException("Dataset size exceeds maximum array capacity: " + totalSize);
        }

        ByteBuffer buffer = ByteBuffer.allocate((int) totalSize).order(ByteOrder.LITTLE_ENDIAN);
        synchronized (fileChannel) {
            fileChannel.position(startOffset);
            int totalBytesRead = 0;
            while (totalBytesRead < totalSize) {
                int bytesRead = fileChannel.read(buffer);
                if (bytesRead == -1) {
                    throw new IOException("Unexpected EOF after " + totalBytesRead + " bytes; expected " + totalSize);
                }
                totalBytesRead += bytesRead;
            }
        }
        buffer.flip();

        HdfFixedPoint[] result = new HdfFixedPoint[readsAvailable];
        for (int i = 0; i < readsAvailable; i++) {
            result[i] = fixedPointDatatype.getInstance(buffer);
        }
        return result;
    }

    /**
     * Returns a sequential Stream of HdfFixedPoint for reading raw data from the FileChannel.
     *
     * @return a sequential Stream of HdfFixedPoint
     * @throws IllegalStateException if no FileChannel was provided
     */
    @Override
    public Stream<HdfFixedPoint> stream() {
        if (fileChannel == null) {
            throw new IllegalStateException("Streaming requires a FileChannel; use the appropriate constructor.");
        }
        return StreamSupport.stream(new FixedPointSpliterator(startOffset, endOffset), false);
    }

    /**
     * Returns a parallel Stream of HdfFixedPoint for reading raw data from the FileChannel.
     *
     * @return a parallel Stream of HdfFixedPoint
     * @throws IllegalStateException if no FileChannel was provided
     */
    @Override
    public Stream<HdfFixedPoint> parallelStream() {
        if (fileChannel == null) {
            throw new IllegalStateException("Streaming requires a FileChannel; use the appropriate constructor.");
        }
        return StreamSupport.stream(new FixedPointSpliterator(startOffset, endOffset), true);
    }

    @Override
    protected HdfFixedPoint populateFromBufferRaw(ByteBuffer buffer) {
        return fixedPointDatatype.getInstance(buffer);
    }
}