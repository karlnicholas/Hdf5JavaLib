package org.hdf5javalib.hdffile.infrastructure.v2btree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BTreeV2Reader {

    private final SeekableByteChannel channel;
    private final int sizeOfOffsets;
    private final int sizeOfLengths;
    private final BTreeV2Header header;

    // --- Pre-calculated sizes for variable-length integers in internal nodes ---
    private final int[] nRecsSizeByDepth;
    private final int[] totalNRecsSizeByDepth;

    public BTreeV2Reader(SeekableByteChannel channel, int sizeOfOffsets, int sizeOfLengths) throws IOException {
        this.channel = channel;
        this.sizeOfOffsets = sizeOfOffsets;
        this.sizeOfLengths = sizeOfLengths;
        this.header = BTreeV2Header.read(channel, sizeOfOffsets, sizeOfLengths);

        // Pre-calculate the variable sizes as described in the spec
        this.nRecsSizeByDepth = new int[header.depth + 1];
        this.totalNRecsSizeByDepth = new int[header.depth + 1];
        calculateNodeInfoSizes();
    }

    /**
     * Traverses the B-tree and returns all records in order.
     */
    public List<BTreeV2Record> getAllRecords() throws IOException {
        if (header.rootNodeAddress == Hdf5Utils.UNDEFINED_ADDRESS) {
            return Collections.emptyList();
        }

        List<BTreeV2Record> allRecords = new ArrayList<>((int)header.totalNumberOfRecordsInBTree);
        traverse(header.rootNodeAddress, header.depth, header.numberOfRecordsInRootNode, allRecords);
        return allRecords;
    }

    private void traverse(long nodeAddress, int depth, int recordsInNode, List<BTreeV2Record> results) throws IOException {
        channel.position(nodeAddress);
        ByteBuffer nodeBuffer = ByteBuffer.allocate(header.nodeSize).order(ByteOrder.LITTLE_ENDIAN);
        Hdf5Utils.readBytes(channel, nodeBuffer);

        String signature = Hdf5Utils.readSignature(nodeBuffer);
        nodeBuffer.position(0); // Rewind after peeking

        if (depth == 0) { // Leaf Node
            Hdf5Utils.V2_verifySignature(nodeBuffer, "BTLF");
            BTreeV2LeafNode leaf = BTreeV2LeafNode.read(nodeBuffer, header, recordsInNode);
            results.addAll(leaf.records);
        } else { // Internal Node
            Hdf5Utils.V2_verifySignature(nodeBuffer, "BTIN");
            int nRecsSize = nRecsSizeByDepth[depth - 1];
            int totalNRecsSize = totalNRecsSizeByDepth[depth - 1];
            BTreeV2InternalNode internal = BTreeV2InternalNode.read(nodeBuffer, header, recordsInNode, depth, sizeOfOffsets, nRecsSize, totalNRecsSize);

            // Recurse on the first child
            traverse(internal.childPointers.get(0).address, depth - 1, (int) internal.childPointers.get(0).numberOfRecords, results);

            // Interleave records with children
            for (int i = 0; i < internal.records.size(); i++) {
                results.add(internal.records.get(i));
                BTreeV2InternalNode.ChildPointer child = internal.childPointers.get(i + 1);
                traverse(child.address, depth - 1, (int) child.numberOfRecords, results);
            }
        }
    }

    /**
     * This method implements the complex logic for determining the size of the
     * "Number of Records" and "Total Number of Records" fields in internal nodes.
     * These sizes depend on the maximum possible records that can be stored in child nodes.
     */
    private void calculateNodeInfoSizes() {
        if (header.depth == 0) return; // No internal nodes

        long maxRecords[] = new long[header.depth + 1];

        // Max records in a leaf node (depth 0)
        final int leafOverhead = 4 + 1 + 1 + 4; // sig, ver, type, checksum
        maxRecords[0] = (header.nodeSize - leafOverhead) / header.recordSize;
        nRecsSizeByDepth[0] = Hdf5Utils.bytesNeededFor(maxRecords[0]);
        totalNRecsSizeByDepth[0] = nRecsSizeByDepth[0];

        // Max records for internal nodes (iteratively)
        for (int d = 1; d <= header.depth; d++) {
            // Size of a child pointer at this level (pointing to level d-1)
            int childPointerSize = sizeOfOffsets + nRecsSizeByDepth[d-1];
            if (d > 1) { // Twig nodes (d=1) don't have totalNRecs
                childPointerSize += totalNRecsSizeByDepth[d-1];
            }

            final int internalOverhead = 4 + 1 + 1 + 4; // sig, ver, type, checksum
            long maxRecs = (header.nodeSize - internalOverhead - childPointerSize) / (header.recordSize + childPointerSize);
            maxRecords[d] = maxRecs;

            // The size of nRecs for a node at depth D depends on maxRecords at D-1
            nRecsSizeByDepth[d] = Hdf5Utils.bytesNeededFor(maxRecords[d-1]);
            // The size of totalNRecs for a node at D depends on maxRecords at D times total at D-1
            totalNRecsSizeByDepth[d] = Hdf5Utils.bytesNeededFor(maxRecords[d] * totalNRecsSizeByDepth[d-1]);
        }
    }

    public BTreeV2Header getHeader() {
        return header;
    }
}