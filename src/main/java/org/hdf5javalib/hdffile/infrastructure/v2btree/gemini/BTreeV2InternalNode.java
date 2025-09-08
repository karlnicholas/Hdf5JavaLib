package org.hdf5javalib.hdffile.infrastructure.v2btree.gemini;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class BTreeV2InternalNode extends BTreeV2Node {
    public final List<ChildPointer> childPointers;
    public final int checksum;

    static class ChildPointer {
        final long address;
        final long numberOfRecords; // n_recs in child
        final long totalNumberOfRecords; // total n_recs in child and descendants (optional)

        ChildPointer(long address, long numberOfRecords, long totalNumberOfRecords) {
            this.address = address;
            this.numberOfRecords = numberOfRecords;
            this.totalNumberOfRecords = totalNumberOfRecords;
        }
    }

    private BTreeV2InternalNode(ByteBuffer bb, BTreeV2Header header, int recordsInThisNode, int currentDepth,
                                int sizeOfOffsets, int nRecsSize, int totalNRecsSize) {
        super("BTIN", bb, header, recordsInThisNode);

        int numPointers = recordsInThisNode + 1;
        this.childPointers = new ArrayList<>(numPointers);
        for (int i = 0; i < numPointers; i++) {
            long addr = Hdf5Utils.readOffset(bb, sizeOfOffsets);
            long nRecs = Hdf5Utils.readVariableSizeUnsigned(bb, nRecsSize);
            long totalNRecs = -1; // -1 indicates not present
            if (currentDepth > 1) { // totalNumberOfRecords is not present in "twig" nodes
                totalNRecs = Hdf5Utils.readVariableSizeUnsigned(bb, totalNRecsSize);
            }
            childPointers.add(new ChildPointer(addr, nRecs, totalNRecs));
        }
        this.checksum = Hdf5Utils.readChecksum(bb);
    }

    public static BTreeV2InternalNode read(ByteBuffer bb, BTreeV2Header header, int recordsInThisNode,
                                           int currentDepth, int sizeOfOffsets,
                                           int nRecsSize, int totalNRecsSize) {
        return new BTreeV2InternalNode(bb, header, recordsInThisNode, currentDepth, sizeOfOffsets,
                nRecsSize, totalNRecsSize);
    }
}

