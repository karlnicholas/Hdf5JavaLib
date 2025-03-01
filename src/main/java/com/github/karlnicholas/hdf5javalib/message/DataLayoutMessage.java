package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

import static com.github.karlnicholas.hdf5javalib.utils.HdfUtils.writeFixedPointToBuffer;

@Getter
public class DataLayoutMessage extends HdfMessage {
    private final int version;
    private final int layoutClass;
    @Setter
    private HdfFixedPoint dataAddress;
    private final HdfFixedPoint[] dimensionSizes;
    private final int compactDataSize;
    private final byte[] compactData;
    private final HdfFixedPoint datasetElementSize;

    // Constructor to initialize all fields
    public DataLayoutMessage(
            int version,
            int layoutClass,
            HdfFixedPoint dataAddress,
            HdfFixedPoint[] dimensionSizes,
            int compactDataSize,
            byte[] compactData,
            HdfFixedPoint datasetElementSize
    ) {
        super(MessageType.DataLayoutMessage, ()->{
            short size = (short) 8;
            switch (layoutClass) {
                case 0: // Compact Storage
                    break;

                case 1: // Contiguous Storage
                    size += 16;
                    break;

                case 2: // Chunked Storage
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported layout class: " + layoutClass);
            }
            return size;
        }, (byte)0);
        this.version = version;
        this.layoutClass = layoutClass;
        this.dataAddress = dataAddress;
        this.dimensionSizes = dimensionSizes;
        this.compactDataSize = compactDataSize;
        this.compactData = compactData;
        this.datasetElementSize = datasetElementSize;
    }

    /**
     * Parses the header message and returns a constructed instance.
     *
     * @param flags      Flags associated with the message (not used here).
     * @param data       Byte array containing the header message data.
     * @param offsetSize Size of offsets in bytes.
     * @param lengthSize Size of lengths in bytes (not used here).
     * @return A fully constructed `DataLayoutMessage` instance.
     */
    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Read version (1 byte)
        int version = Byte.toUnsignedInt(buffer.get());
        if (version < 1 || version > 3) {
            throw new IllegalArgumentException("Unsupported Data Layout Message version: " + version);
        }

        // Read layout class (1 byte)
        int layoutClass = Byte.toUnsignedInt(buffer.get());

        // Initialize fields
        HdfFixedPoint dataAddress = null;
        HdfFixedPoint[] dimensionSizes = null;
        int compactDataSize = 0;
        byte[] compactData = null;
        HdfFixedPoint datasetElementSize = null;
        BitSet emptyBitSet = new BitSet();

        // Parse based on layout class
        switch (layoutClass) {
            case 0: // Compact Storage
                compactDataSize = Short.toUnsignedInt(buffer.getShort()); // Compact data size (2 bytes)
                compactData = new byte[compactDataSize];
                buffer.get(compactData); // Read compact data
                break;

            case 1: // Contiguous Storage
                dataAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short)0, (short)(offsetSize*8)); // Data address
                dimensionSizes = new HdfFixedPoint[1];
                dimensionSizes[0] = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short)0, (short)(offsetSize*8)); // Dimension size
                break;

            case 2: // Chunked Storage
                dataAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short)0, (short)(offsetSize*8)); // Data address
                int numDimensions = Byte.toUnsignedInt(buffer.get()); // Number of dimensions (1 byte)
                dimensionSizes = new HdfFixedPoint[numDimensions];
                for (int i = 0; i < numDimensions; i++) {
                    dimensionSizes[i] = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short)0, (short)(offsetSize*8)); // Dimension sizes
                }
                datasetElementSize = HdfFixedPoint.readFromByteBuffer(buffer, (short)4, emptyBitSet, (short)0, (short)(4*8)); // Dataset element size (4 bytes)
                break;

            default:
                throw new IllegalArgumentException("Unsupported layout class: " + layoutClass);
        }

        // Return a constructed instance of DataLayoutMessage
        return new DataLayoutMessage(version, layoutClass, dataAddress, dimensionSizes, compactDataSize, compactData, datasetElementSize);
    }

    @Override
    public String toString() {
        return "DataLayoutMessage{" +
                "version=" + version +
                ", layoutClass=" + layoutClass +
                ", dataAddress=" + (layoutClass == 1 || layoutClass == 2 ? dataAddress : "N/A") +
                ", dimensionSizes=" + Arrays.toString(dimensionSizes) +
                ", compactDataSize=" + (layoutClass == 0 ? compactDataSize : "N/A") +
                ", compactData=" + (layoutClass == 0 ? Arrays.toString(compactData) : "N/A") +
                ", datasetElementSize=" + (layoutClass == 2 ? datasetElementSize : "N/A") +
                '}';
    }

    @Override
    public void writeToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        // Read version (1 byte)
        buffer.put((byte) version);
        // Read layout class (1 byte)
        buffer.put((byte) layoutClass);

        switch (layoutClass) {
            case 0: // Compact Storage
                buffer.putShort((short) compactDataSize); // Compact data size (2 bytes)
                buffer.put(compactData); // Read compact data
                break;

            case 1: // Contiguous Storage
                writeFixedPointToBuffer(buffer, dataAddress);
                writeFixedPointToBuffer(buffer, dimensionSizes[0]);
                break;

            case 2: // Chunked Storage
                writeFixedPointToBuffer(buffer, dataAddress);
                buffer.get(dimensionSizes.length); // Number of dimensions (1 byte)
                for (HdfFixedPoint dimensionSize: dimensionSizes) {
                    writeFixedPointToBuffer(buffer, dimensionSize);
                }
                writeFixedPointToBuffer(buffer, datasetElementSize);
                break;

            default:
                throw new IllegalArgumentException("Unsupported layout class: " + layoutClass);
        }

    }

    public static class ChunkedStorage {
        private final int version;
        private final int rank;
        private final long[] chunkSizes;
        private final HdfFixedPoint address;

        public ChunkedStorage(int version, int rank, long[] chunkSizes, HdfFixedPoint address) {
            this.version = version;
            this.rank = rank;
            this.chunkSizes = chunkSizes;
            this.address = address;
        }

        public static ChunkedStorage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int version = Byte.toUnsignedInt(buffer.get());
            int rank = Byte.toUnsignedInt(buffer.get());
            long[] chunkSizes = new long[rank];
            for (int i = 0; i < rank; i++) {
                chunkSizes[i] = Integer.toUnsignedLong(buffer.getInt());
            }
            HdfFixedPoint address = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, new BitSet(), (short)0, (short)(offsetSize*8));
            return new ChunkedStorage(version, rank, chunkSizes, address);
        }

        @Override
        public String toString() {
            return "ChunkedStorage{" +
                    "version=" + version +
                    ", rank=" + rank +
                    ", chunkSizes=" + Arrays.toString(chunkSizes) +
                    ", address=" + (address != null ? address.toBigInteger() : "null") +
                    '}';
        }

    }

    public static class ContiguousStorage {
        private final int version;
        private final HdfFixedPoint address;
        private final HdfFixedPoint size;

        public ContiguousStorage(int version, HdfFixedPoint address, HdfFixedPoint size) {
            this.version = version;
            this.address = address;
            this.size = size;
        }

        public static ContiguousStorage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int version = Byte.toUnsignedInt(buffer.get());
            BitSet emptyBitSet = new BitSet(0);
            HdfFixedPoint address = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short)0, (short)(offsetSize*8));
            HdfFixedPoint size = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short)0, (short)(lengthSize*8));
            return new ContiguousStorage(version, address, size);
        }

        @Override
        public String toString() {
            return "ContiguousStorage{" +
                    "version=" + version +
                    ", address=" + (address != null ? address.toBigInteger() : "null") +
                    ", size=" + (size != null ? size.toBigInteger() : "null") +
                    '}';
        }

    }
}
