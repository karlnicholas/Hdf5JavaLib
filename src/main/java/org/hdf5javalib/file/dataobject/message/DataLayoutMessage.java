package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import lombok.Setter;
import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

@Getter
public class DataLayoutMessage extends HdfMessage {
    private final int version;
    private final int layoutClass;
    @Setter
    private HdfFixedPoint<BigInteger> dataAddress;
    private final HdfFixedPoint<BigInteger>[] dimensionSizes;
    private final int compactDataSize;
    private final byte[] compactData;
    private final HdfFixedPoint<BigInteger> datasetElementSize;

    // Constructor to initialize all fields
    public DataLayoutMessage(
            int version,
            int layoutClass,
            HdfFixedPoint<BigInteger> dataAddress,
            HdfFixedPoint<BigInteger>[] dimensionSizes,
            int compactDataSize,
            byte[] compactData,
            HdfFixedPoint<BigInteger> datasetElementSize
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
        HdfFixedPoint<BigInteger> dataAddress = null;
        HdfFixedPoint<BigInteger>[] dimensionSizes = null;
        int compactDataSize = 0;
        byte[] compactData = null;
        HdfFixedPoint<BigInteger> datasetElementSize = null;
        BitSet emptyBitSet = new BitSet();

        // Parse based on layout class
        switch (layoutClass) {
            case 0: // Compact Storage
                compactDataSize = Short.toUnsignedInt(buffer.getShort()); // Compact data size (2 bytes)
                compactData = new byte[compactDataSize];
                buffer.get(compactData); // Read compact data
                break;

            case 1: // Contiguous Storage
                dataAddress = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, offsetSize, emptyBitSet, (short)0, (short)(offsetSize*8)); // Data address
                dimensionSizes = new HdfFixedPoint[1];
                dimensionSizes[0] = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, offsetSize, emptyBitSet, (short)0, (short)(offsetSize*8)); // Dimension size
                break;

            case 2: // Chunked Storage
                dataAddress = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, offsetSize, emptyBitSet, (short)0, (short)(offsetSize*8)); // Data address
                int numDimensions = Byte.toUnsignedInt(buffer.get()); // Number of dimensions (1 byte)
                dimensionSizes = new HdfFixedPoint[numDimensions];
                for (int i = 0; i < numDimensions; i++) {
                    dimensionSizes[i] = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, offsetSize, emptyBitSet, (short)0, (short)(offsetSize*8)); // Dimension sizes
                }
                datasetElementSize = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, (short)4, emptyBitSet, (short)0, (short)(4*8)); // Dataset element size (4 bytes)
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
        private final HdfFixedPoint<BigInteger> address;

        public ChunkedStorage(int version, int rank, long[] chunkSizes, HdfFixedPoint<BigInteger> address) {
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
            HdfFixedPoint<BigInteger> address = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, offsetSize, new BitSet(), (short)0, (short)(offsetSize*8));
            return new ChunkedStorage(version, rank, chunkSizes, address);
        }

        @Override
        public String toString() {
            return "ChunkedStorage{" +
                    "version=" + version +
                    ", rank=" + rank +
                    ", chunkSizes=" + Arrays.toString(chunkSizes) +
                    ", address=" + (address != null ? address.getInstance() : "null") +
                    '}';
        }

    }

    public static class ContiguousStorage {
        private final int version;
        private final HdfFixedPoint<BigInteger> address;
        private final HdfFixedPoint<BigInteger> size;

        public ContiguousStorage(int version, HdfFixedPoint<BigInteger> address, HdfFixedPoint<BigInteger> size) {
            this.version = version;
            this.address = address;
            this.size = size;
        }

        public static ContiguousStorage parseHeaderMessage(byte flags, byte[] data, short offsetSize, short lengthSize) {
            ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
            int version = Byte.toUnsignedInt(buffer.get());
            BitSet emptyBitSet = new BitSet(0);
            HdfFixedPoint<BigInteger> address = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, offsetSize, emptyBitSet, (short)0, (short)(offsetSize*8));
            HdfFixedPoint<BigInteger> size = HdfFixedPoint.readFromByteBuffer(BigInteger.class, buffer, lengthSize, emptyBitSet, (short)0, (short)(lengthSize*8));
            return new ContiguousStorage(version, address, size);
        }

        @Override
        public String toString() {
            return "ContiguousStorage{" +
                    "version=" + version +
                    ", address=" + (address != null ? address.getDatatype() : "null") +
                    ", size=" + (size != null ? size.getDatatype() : "null") +
                    '}';
        }

    }
}
