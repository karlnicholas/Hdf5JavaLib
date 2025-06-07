package org.hdf5javalib.redo.dataclass.reference;

public class HdfSelectionHyperSlabV3Irregular extends HdfDataspaceSelectionInstance {
    private final int version;
    private final int flags;
    private final int encodeSize;
    private final int rank;
    private final long numBlocks;
    private final long[][] startOffsets;
    private final long[][] endOffsets;

    public HdfSelectionHyperSlabV3Irregular(int version, int flags, int encodeSize, int rank, long numBlocks, long[][] startOffsets, long[][] endOffsets) {
        this.version = version;
        this.flags = flags;
        this.encodeSize = encodeSize;
        this.rank = rank;
        this.numBlocks = numBlocks;
        this.startOffsets = startOffsets;
        this.endOffsets = endOffsets;
    }
}
