package org.hdf5javalib.redo.reference;

import java.nio.ByteBuffer;

public abstract class HdfDataspaceSelectionInstance {
    /**
     * Enum representing SectionType for HDF5 strings.
     */
    public enum SectionType {
        NONE(0, "NONE", "No Selection"),
        POINTS(1, "POINTS", "Points Selection"),
        /** Reserved cSectionType for future use. */
        HYPERSLAB(2, "HYPERSLAB", "Hyperslab Selection"),
        /** Reserved SectionType for future use. */
        ATTRIBUTE(3, "ATTRIBUTE", "Attribute Selection");
        private final int value;
        private final String name;
        private final String description;

        SectionType(int value, String name, String description) {
            this.value = value;
            this.name = name;
            this.description = description;
        }

        /**
         * Retrieves the CharacterSet corresponding to the given value.
         *
         * @param value the numeric value of the character set
         * @return the corresponding CharacterSet
         * @throws IllegalArgumentException if the value does not match any known character set
         */
        public static SectionType fromValue(int value) {
            for (SectionType set : values()) {
                if (set.value == value) return set;
            }
            throw new IllegalArgumentException("Invalid character set value: " + value);
        }

//        public static HdfDataspaceSelectionInstance createClass(int type) {
//            return switch (type) {
//                case 0 -> new HdfSelectionNone();
//                case 1 -> new HdfSelectPoints();
//                case 2 -> new HdfSelectionHyperSlab();
//                case 3 -> new HdfSelectionAttribute();
//                default -> throw new IllegalStateException("Unexpected value: " + type);
//            };
//        }
    }

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
