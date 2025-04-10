package org.hdf5javalib.file.dataobject.message;

import lombok.Getter;
import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents a Dataspace Message in the HDF5 file format.
 */
@Getter
public class DataspaceMessage extends HdfMessage {
    private final int version;
    private final int dimensionality;
    private final BitSet flags;
    private final HdfFixedPoint[] dimensions;
    private final HdfFixedPoint[] maxDimensions;
    private final boolean hasMaxDimensions;

    public DataspaceMessage(
            int version,
            int dimensionality,
            BitSet flags,
            HdfFixedPoint[] dimensions,
            HdfFixedPoint[] maxDimensions,
            boolean hasMaxDimensions,
            byte rawFlags,
            short sizeMessageData
    ) {
        super(MessageType.DataspaceMessage, sizeMessageData, rawFlags);
        this.version = version;
        this.dimensionality = dimensionality;
        this.flags = flags;
        this.dimensions = dimensions;
        this.maxDimensions = maxDimensions;
        this.hasMaxDimensions = hasMaxDimensions;
    }

    /**
     * Parses the header message and returns a constructed instance.
     */
    public static HdfMessage parseHeaderMessage(byte flags, byte[] data, short ignoredOffsetSize, short lengthSize, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        int version = Byte.toUnsignedInt(buffer.get());
        int dimensionality = Byte.toUnsignedInt(buffer.get());
        int flagByte = Byte.toUnsignedInt(buffer.get());
        BitSet flagSet = BitSet.valueOf(new byte[]{(byte) flagByte});

        buffer.position(buffer.position() + 5);

        BitSet emptyBitSet = new BitSet();
        HdfFixedPoint[] dimensions = new HdfFixedPoint[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensions[i] = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short) 0, (short) (lengthSize * 8));
        }

        boolean hasMaxDimensions = flagSet.get(DataspaceFlag.MAX_DIMENSIONS_PRESENT.getBitIndex());
        HdfFixedPoint[] maxDimensions = null;
        if (hasMaxDimensions) {
            maxDimensions = new HdfFixedPoint[dimensionality];
            for (int i = 0; i < dimensionality; i++) {
                maxDimensions[i] = HdfFixedPoint.readFromByteBuffer(buffer, lengthSize, emptyBitSet, (short) 0, (short) (lengthSize * 8));
            }
        }

        return new DataspaceMessage(version, dimensionality, flagSet, dimensions, maxDimensions, hasMaxDimensions, flags, (short) data.length);
    }

    @Override
    public String toString() {
        return "DataspaceMessage(" + (getSizeMessageData() + 8) + "){" +
                "version=" + version +
                ", dimensionality=" + dimensionality +
                ", flags=" + flags +
                ", dimensions=" + (dimensions != null ? Arrays.toString(dimensions) : "Not Present") +
                ", maxDimensions=" + (maxDimensions != null ? Arrays.toString(maxDimensions) : "Not Present") +
                ", hasMaxDimensions=" + hasMaxDimensions +
                '}';
    }

    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
        writeInfoToByteBuffer(buffer);
    }

    public void writeInfoToByteBuffer(ByteBuffer buffer) {
        buffer.put((byte) version);
        buffer.put((byte) dimensionality);

        // Encode BitSet as single byte
        byte encoded = flags.isEmpty() ? 0 : flags.toByteArray()[0];
        buffer.put(encoded);

        buffer.put(new byte[5]);

        if (dimensions != null) {
            for (HdfFixedPoint dimension : dimensions) {
                writeFixedPointToBuffer(buffer, dimension);
            }
        }

        if (maxDimensions != null) {
            for (HdfFixedPoint maxDimension : maxDimensions) {
                writeFixedPointToBuffer(buffer, maxDimension);
            }
        }
    }

    public boolean hasFlag(DataspaceFlag flag) {
        return flags.get(flag.getBitIndex());
    }

    public static BitSet buildFlagSet(boolean maxDimensionsPresent, boolean permutationIndicesPresent) {
        BitSet bits = new BitSet();
        if (maxDimensionsPresent) {
            bits.set(DataspaceFlag.MAX_DIMENSIONS_PRESENT.getBitIndex());
        }
        if (permutationIndicesPresent) {
            bits.set(DataspaceFlag.PERMUTATION_INDICES_PRESENT.getBitIndex());
        }
        return bits;
    }

    public enum DataspaceFlag {
        MAX_DIMENSIONS_PRESENT(0),
        PERMUTATION_INDICES_PRESENT(1);

        private final int bitIndex;

        DataspaceFlag(int bitIndex) {
            this.bitIndex = bitIndex;
        }

        public int getBitIndex() {
            return bitIndex;
        }

        public boolean isSet(BitSet flags) {
            return flags.get(bitIndex);
        }
    }
}
