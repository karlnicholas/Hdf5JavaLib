package org.hdf5javalib.redo.reference;

public class HdfSelectionHyperSlab extends HdfDataspaceSelectionInstance {
    private final int version;
    private final int length;
    private final int rank;
    private final int numBlocks;
    private final int[][] startOffsets;
    private final int[][] endOffsets;

    public HdfSelectionHyperSlab(int version, int length, int rank, int numBlocks, int[][] startOffsets, int[][] endOffsets) {
        this.version = version;
        this.length = length;
        this.rank = rank;
        this.numBlocks = numBlocks;
        this.startOffsets = startOffsets;
        this.endOffsets = endOffsets;
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HdfSelectionHyperSlab{")
                .append("version=").append(version)
                .append(", length=").append(length)
                .append(", rank=").append(rank)
                .append(", numBlocks=").append(numBlocks)
                .append(", startOffsets=[");

        // Format startOffsets
        for (int i = 0; i < startOffsets.length; i++) {
            sb.append("[");
            for (int j = 0; j < startOffsets[i].length; j++) {
                sb.append(startOffsets[i][j]);
                if (j < startOffsets[i].length - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
            if (i < startOffsets.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("], endOffsets=[");

        // Format endOffsets
        for (int i = 0; i < endOffsets.length; i++) {
            sb.append("[");
            for (int j = 0; j < endOffsets[i].length; j++) {
                sb.append(endOffsets[i][j]);
                if (j < endOffsets[i].length - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
            if (i < endOffsets.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("]}");
        return sb.toString();
    }
}
