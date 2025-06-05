package org.hdf5javalib.redo.reference;

public class HdfSelectionHyperSlabV1 extends HdfDataspaceSelectionInstance {
    private final int version;
    private final int length;
    private final int rank;
    private final int numBlocks;
    private final int[][] startOffsets;
    private final int[][] endOffsets;

    public HdfSelectionHyperSlabV1(int version, int length, int rank, int numBlocks, int[][] startOffsets, int[][] endOffsets) {
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
        sb.append("HdfSelectionHyperSlabV1{v=").append(version)
                .append(",l=").append(length)
                .append(",r=").append(rank)
                .append(",n=").append(numBlocks)
                .append(",b=[");

        if (numBlocks <= 0 || startOffsets == null || endOffsets == null) {
            sb.append("]");
        } else {
            for (int b = 0; b < numBlocks; b++) {
                if (b > 0) sb.append(",");
                sb.append("B").append(b + 1).append(":(");
                for (int r = 0; r < rank; r++) {
                    sb.append(r > 0 ? "," : "").append(startOffsets[b][r]);
                }
                sb.append(")(");
                for (int r = 0; r < rank; r++) {
                    sb.append(r > 0 ? "," : "").append(endOffsets[b][r]);
                }
                sb.append(")");
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }
}
