package org.hdf5javalib.redo.dataclass.reference;

public class HdfSelectPointsV2 extends HdfDataspaceSelectionInstance {
    private final int version;
    private final int encodedSize;
    private final int rank;
    private final long numPoints;
    private final long[][] values;

    public HdfSelectPointsV2(int version, int encodedSize, int rank, long numPoints, long[][] values) {
        this.version = version;
        this.encodedSize = encodedSize;
        this.rank = rank;
        this.numPoints = numPoints;
        this.values = values;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HdfSelectPointsV1{v=").append(version)
                .append(",r=").append(rank)
                .append(",n=").append(numPoints)
                .append(",p=[");

        if (numPoints <= 0 || values == null) {
            sb.append("]");
        } else {
            for (int i = 0; i < numPoints; i++) {
                if (i > 0) sb.append(",");
                sb.append("P").append(i + 1).append(":(");
                for (int j = 0; j < rank; j++) {
                    sb.append(j > 0 ? "," : "").append(values[i][j]);
                }
                sb.append(")");
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }
}
