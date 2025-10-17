package org.hdf5javalib.dataclass.reference;

import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfDataObject;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

import static org.hdf5javalib.dataclass.reference.HdfDataspaceSelectionInstance.HdfSelectionType.*;

public abstract class HdfDataspaceSelectionInstance {
    public abstract HdfDataHolder getData(HdfDataObject hdfDataObject, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException;

    public enum HdfSelectionType {
        H5S_SEL_NONE(0, "Nothing selected"),
        H5S_SEL_POINTS(1, "Sequence of points selected"),
        H5S_SEL_HYPERSLABS(2, "Hyperslab selected"),
        H5S_SEL_ALL(3, "Entire extent selected");

        private final int value;
        private final String description;

        HdfSelectionType(int value, String description) {
            this.value = value;
            this.description = description;
        }

        public int getValue() {
            return value;
        }

        public String getDescription() {
            return description;
        }

        // Optional: Method to get enum by value
        public static HdfSelectionType fromValue(int value) {
            for (HdfSelectionType type : HdfSelectionType.values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Invalid selection value: " + value);
        }
    }

    /**
     * Parses the selection information from the remaining data buffer.
     *
     * <p>Delegates parsing logic based on the selection type (NONE, ALL, POINTS, HYPERSLABS).
     *
     * @param remaingData The {@link ByteBuffer} containing the selection details.
     * @return An instance of {@code HdfDataspaceSelectionInstance} corresponding to the parsed selection type and version.
     * @throws IllegalArgumentException if the selection type or version is invalid.
     */
    public static HdfDataspaceSelectionInstance parseSelectionInfo(ByteBuffer remaingData) {
        HdfSelectionType selectionType = fromValue(remaingData.getInt());
        int version = remaingData.getInt();

        return switch (selectionType) {
            case H5S_SEL_NONE -> new HdfSelectionNone(version);
            case H5S_SEL_ALL -> new HdfSelectionAll(version);
            case H5S_SEL_POINTS -> parsePointsSelection(version, remaingData);
            case H5S_SEL_HYPERSLABS -> parseHyperslabsSelection(version, remaingData);
            default -> throw new IllegalArgumentException("Invalid selection type: " + selectionType);
        };
    }

    //
    // Helper Methods to Reduce Complexity
    //

    private static HdfDataspaceSelectionInstance parsePointsSelection(int version, ByteBuffer remaingData) {
        return switch (version) {
            case 1 -> parsePointsV1(version, remaingData);
            case 2 -> parsePointsV2(version, remaingData);
            default -> throw new IllegalArgumentException("Invalid selection version for POINTS: " + version);
        };
    }

    private static HdfDataspaceSelectionInstance parsePointsV1(int version, ByteBuffer remaingData) {
        remaingData.getInt(); // Skip/read padding
        int length = remaingData.getInt();
        int rank = remaingData.getInt();
        int numPoints = remaingData.getInt();
        int[][] values = new int[numPoints][rank];
        for (int pnum = 0; pnum < numPoints; pnum++) {
            for (int rnum = 0; rnum < rank; rnum++) {
                values[pnum][rnum] = remaingData.getInt();
            }
        }
        return new HdfSelectPointsV1(version, length, rank, numPoints, values);
    }

    private static HdfDataspaceSelectionInstance parsePointsV2(int version, ByteBuffer remaingData) {
        int encodingSize = remaingData.getInt();
        int rank = remaingData.getInt();
        long numPoints = getSizeEncodedValue(encodingSize, remaingData);

        long[][] values = new long[Math.toIntExact(numPoints)][rank];
        for (int pnum = 0; pnum < numPoints; pnum++) {
            for (int rnum = 0; rnum < rank; rnum++) {
                values[pnum][rnum] = getSizeEncodedValue(encodingSize, remaingData);
            }
        }
        return new HdfSelectPointsV2(version, encodingSize, rank, numPoints, values);
    }

    private static HdfDataspaceSelectionInstance parseHyperslabsSelection(int version, ByteBuffer remaingData) {
        return switch (version) {
            case 1 -> parseHyperslabsV1(version, remaingData);
            case 2 -> parseHyperslabsV2(version, remaingData);
            case 3 -> parseHyperslabsV3(version, remaingData);
            default -> throw new IllegalArgumentException("Invalid selection version for HYPERSLABS: " + version);
        };
    }

    private static HdfDataspaceSelectionInstance parseHyperslabsV1(int version, ByteBuffer remaingData) {
        remaingData.getInt(); // Skip/read padding
        int length = remaingData.getInt();
        int rank = remaingData.getInt();
        int numBlocks = remaingData.getInt();
        int[][] startOffsets = new int[numBlocks][rank];
        int[][] endOffsets = new int[numBlocks][rank];
        for (int bnum = 0; bnum < numBlocks; bnum++) {
            for (int rnum = 0; rnum < rank; rnum++) {
                startOffsets[bnum][rnum] = remaingData.getInt();
            }
            for (int rnum = 0; rnum < rank; rnum++) {
                endOffsets[bnum][rnum] = remaingData.getInt();
            }
        }
        return new HdfSelectionHyperSlabV1(version, length, rank, numBlocks, startOffsets, endOffsets);
    }

    private static HdfDataspaceSelectionInstance parseHyperslabsV2(int version, ByteBuffer remaingData) {
        int flags = remaingData.get();
        int length = remaingData.getInt();
        int rank = remaingData.getInt();
        long[] start = new long[rank];
        long[] stride = new long[rank];
        long[] count = new long[rank];
        long[] block = new long[rank];
        for (int s = 0; s < rank; s++) {
            start[s] = remaingData.getLong();
            stride[s] = remaingData.getLong();
            count[s] = remaingData.getLong();
            block[s] = remaingData.getLong();
        }
        return new HdfSelectionHyperSlabV2(version, flags, length, rank, start, stride, count, block);
    }

    private static HdfDataspaceSelectionInstance parseHyperslabsV3(int version, ByteBuffer remaingData) {
        int flags = remaingData.get();
        int encodeSize = remaingData.get();
        int rank = remaingData.getInt();

        return switch (flags) {
            case 0 -> parseHyperslabsV3Regular(version, flags, encodeSize, rank, remaingData);
            case 1 -> parseHyperslabsV3Irregular(version, flags, encodeSize, rank, remaingData);
            default -> throw new IllegalArgumentException("Invalid flags for HYPERSLABS V3: " + flags);
        };
    }

    private static HdfDataspaceSelectionInstance parseHyperslabsV3Regular(int version, int flags, int encodeSize, int rank, ByteBuffer remaingData) {
        long[] start = new long[rank];
        long[] stride = new long[rank];
        long[] count = new long[rank];
        long[] block = new long[rank];
        for (int s = 0; s < rank; s++) {
            start[s] = getSizeEncodedValue(encodeSize, remaingData);
            stride[s] = getSizeEncodedValue(encodeSize, remaingData);
            count[s] = getSizeEncodedValue(encodeSize, remaingData);
            block[s] = getSizeEncodedValue(encodeSize, remaingData);
        }
        return new HdfSelectionHyperSlabV3Regular(version, flags, encodeSize, rank, start, stride, count, block);
    }

    private static HdfDataspaceSelectionInstance parseHyperslabsV3Irregular(int version, int flags, int encodeSize, int rank, ByteBuffer remaingData) {
        long numBlocks = getSizeEncodedValue(encodeSize, remaingData);
        long[][] startOffsets = new long[Math.toIntExact(numBlocks)][rank];
        long[][] endOffsets = new long[Math.toIntExact(numBlocks)][rank];
        for (int bnum = 0; bnum < numBlocks; bnum++) {
            for (int rnum = 0; rnum < rank; rnum++) {
                startOffsets[bnum][rnum] = getSizeEncodedValue(encodeSize, remaingData);
            }
            for (int rnum = 0; rnum < rank; rnum++) {
                endOffsets[bnum][rnum] = getSizeEncodedValue(encodeSize, remaingData);
            }
        }
        return new HdfSelectionHyperSlabV3Irregular(version, flags, encodeSize, rank, numBlocks, startOffsets, endOffsets);
    }

    private static long getSizeEncodedValue(int encodingSize, ByteBuffer remaingData) {
        return switch (encodingSize) {
            case 2 -> remaingData.getShort();
            case 4 -> remaingData.getInt();
            case 8 -> remaingData.getLong();
            default -> throw new IllegalArgumentException("Invalid encoding size " + encodingSize);
        };
    }
}