package org.hdf5javalib.redo.reference;

import java.nio.ByteBuffer;

public abstract class HdfDataspaceSelectionInstance {
    public static HdfDataspaceSelectionInstance parseSelectionInfo(ByteBuffer remaingData) {
        int selectionType = remaingData.getInt();
        if ( selectionType == 0 ) {
            int version = remaingData.getInt();
            return new HdfSelectionNone(version);
        }
        if ( selectionType == 1 ) {
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
                HdfSelectPoints selectionPoints = new HdfSelectPoints(version, length, rank, numPoints, values);
                return selectionPoints;
            } else if (version == 2) {
                return null;
            } else if (version == 3) {
                return null;
            } else {
                throw new IllegalArgumentException("Invalid selection type: " + selectionType);
            }
        } else if ( selectionType == 2 ) {
            int version = remaingData.getInt();
            if ( version == 1 ) {
                remaingData.getInt();
                int length = remaingData.getInt();
                int rank = remaingData.getInt();
                int numBlocks = remaingData.getInt();
                int[][] startOffsets = new int[rank][numBlocks];
                int[][] endOffsets = new int[rank][numBlocks];
                for( int bnum = 0; bnum < numBlocks; bnum++ ) {
                    for (int rnum = 0; rnum < rank; rnum++) {
                        startOffsets[rnum][bnum] = remaingData.getInt();
                    }
                    for (int rnum = 0; rnum < rank; rnum++) {
                        endOffsets[rnum][bnum] = remaingData.getInt();
                    }
                }
                HdfSelectionHyperSlab selectionHyperSlab = new HdfSelectionHyperSlab(version, length, rank, numBlocks, startOffsets, endOffsets);
                return selectionHyperSlab;
            } else if ( version == 2 ) {
                return null;
            } else {
                throw new IllegalArgumentException("Invalid selection type: " + selectionType);
            }
        } else if (  selectionType == 3 ) {
            int version = remaingData.getInt();
            return new HdfSelectionAll(version);
        } else {
            throw new IllegalArgumentException("Invalid selection type: " + selectionType);
        }
    }

}
