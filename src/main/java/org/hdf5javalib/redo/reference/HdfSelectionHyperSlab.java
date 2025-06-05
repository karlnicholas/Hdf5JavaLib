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
        sb.append("HdfSelectionHyperSlab{v=").append(version)
                .append(",l=").append(length)
                .append(",r=").append(rank)
                .append(",n=").append(numBlocks)
                .append(",b=[");

        if (numBlocks <= 0 || startOffsets == null || endOffsets == null) {
            sb.append("]");
        } else {
            for (int u = 0; u < numBlocks; u++) {
                if (u > 0) sb.append(",");
                sb.append("B").append(u + 1).append(":(");
                for (int n = 0; n < rank; n++) {
                    sb.append(n > 0 ? "," : "").append(startOffsets[n][u]);
                }
                sb.append(")(");
                for (int n = 0; n < rank; n++) {
                    sb.append(n > 0 ? "," : "").append(endOffsets[n][u]);
                }
                sb.append(")");
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }
}
