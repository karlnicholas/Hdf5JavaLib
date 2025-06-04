package org.hdf5javalib.redo.reference;

public class HdfSelectPoints extends HdfDataspaceSelectionInstance {
    private final int version;
    private final int length;
    private final int rank;
    private final int numPoints;
    private final int[][] values;

    public HdfSelectPoints(int version, int length, int rank, int numPoints, int[][] values) {
        this.version = version;
        this.length = length;
        this.rank = rank;
        this.numPoints = numPoints;
        this.values = values;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HdfSelectPoints{")
                .append("version=").append(version)
                .append(", length=").append(length)
                .append(", rank=").append(rank)
                .append(", numPoints=").append(numPoints)
                .append(", values=[");

        for (int i = 0; i < values.length; i++) {
            sb.append("[");
            for (int j = 0; j < values[i].length; j++) {
                sb.append(values[i][j]);
                if (j < values[i].length - 1) {
                    sb.append(", ");
                }
            }
            sb.append("]");
            if (i < values.length - 1) {
                sb.append(", ");
            }
        }
        sb.append("]}");
        return sb.toString();
    }
}
