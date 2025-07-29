package org.hdf5javalib.dataclass.reference;

import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfDataObject;
import org.hdf5javalib.utils.HdfDataHolder;

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

    public static HdfDataspaceSelectionInstance parseSelectionInfo(ByteBuffer remaingData) {
        HdfSelectionType selectionType = fromValue(remaingData.getInt());
        if (selectionType == H5S_SEL_NONE) {
            int version = remaingData.getInt();
            return new HdfSelectionNone(version);
        }
        if (selectionType == H5S_SEL_POINTS) {
            int version = remaingData.getInt();
            if (version == 1) {
                remaingData.getInt();
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
            } else if (version == 2) {
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
            } else {
                throw new IllegalArgumentException("Invalid selection type: " + selectionType);
            }
        } else if (selectionType == H5S_SEL_HYPERSLABS) {
            int version = remaingData.getInt();
            if (version == 1) {
                remaingData.getInt();
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
            } else if (version == 2) {
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
            } else if (version == 3) {
                int flags = remaingData.get();
                int encodeSize = remaingData.get();
                int rank = remaingData.getInt();
                if (flags == 0) {
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
                } else if (flags == 1) {
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
                } else {
                    throw new IllegalArgumentException("Invalid selection type: " + selectionType);
                }
            } else {
                throw new IllegalArgumentException("Invalid selection type: " + selectionType);
            }
        } else if (selectionType == H5S_SEL_ALL) {
            int version = remaingData.getInt();
            return new HdfSelectionAll(version);
        } else {
            throw new IllegalArgumentException("Invalid selection type: " + selectionType);
        }
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
