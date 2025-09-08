package org.hdf5javalib.hdffile.infrastructure.v2btree.grok;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.List;

public class V2BTreeReader {

    private final SeekableByteChannel channel;
    private final int sizeOfOffsets;
    private final int sizeOfLengths;

    // Struct for B-tree header
    public static class BTreeHeader {
        public int type;
        public int nodeSize;
        public int recordSize;
        public int depth;
        public int splitPercent;
        public int mergePercent;
        public long rootNodeAddress;
        public int numberOfRecordsInRootNode;
        public long totalNumberOfRecords;
        public int checksum;
    }

    // Struct for internal node record (generic)
    public static class InternalNode {
        public byte[] records; // Raw bytes, to be parsed based on type
        public long[] childNodeAddresses;
        public int[] numberOfRecordsInChild;
        public long[] totalNumberOfRecordsInChild; // Only for depth > 1
        public int checksum;
    }

    // Struct for leaf node record (generic)
    public static class LeafNode {
        public byte[] records; // Raw bytes, to be parsed based on type
        public int checksum;
    }

    // Struct for Type 5 record (Link Name for Indexed Group)
    public static class Type5Record {
        public int hashOfName;
        public byte[] heapId; // 7 bytes
    }

    // Struct for Type 6 record (Creation Order for Indexed Group)
    public static class Type6Record {
        public long creationOrder;
        public byte[] heapId; // 7 bytes
    }

    public V2BTreeReader(SeekableByteChannel channel, int sizeOfOffsets, int sizeOfLengths) {
        this.channel = channel;
        this.sizeOfOffsets = sizeOfOffsets;
        this.sizeOfLengths = sizeOfLengths;
    }

    public BTreeHeader readHeader() throws IOException {
        BTreeHeader header = new BTreeHeader();

        // Signature
        String signature = readString(4);
        if (!"BTHD".equals(signature)) {
            throw new IOException("Invalid v2 B-tree header signature: " + signature);
        }

        // Version
        byte version = readByte();
        if (version != 0) {
            throw new IOException("Unsupported v2 B-tree version: " + version);
        }

        // Type
        header.type = readByte() & 0xFF;

        // Node Size
        header.nodeSize = readUInt32();

        // Record Size
        header.recordSize = readUInt16();

        // Depth
        header.depth = readUInt16();

        // Split Percent
        header.splitPercent = readByte() & 0xFF;

        // Merge Percent
        header.mergePercent = readByte() & 0xFF;

        // Root Node Address
        header.rootNodeAddress = readUnsigned(sizeOfOffsets);

        // Number of Records in Root Node
        header.numberOfRecordsInRootNode = readUInt16();

        // Total Number of Records in B-tree
        header.totalNumberOfRecords = readUnsigned(sizeOfLengths);

        // Checksum
        header.checksum = readUInt32();

        return header;
    }

    // Read all heap IDs from the B-tree (for types 5 or 6)
    public List<byte[]> getAllHeapIds(long btreeAddress, int btreeType) throws IOException {
        channel.position(btreeAddress);
        BTreeHeader header = readHeader();
        if (header.type != btreeType) {
            throw new IOException("Expected B-tree type " + btreeType + ", but found " + header.type);
        }
        if (header.type != 5 && header.type != 6) {
            throw new UnsupportedOperationException("Only B-tree types 5 and 6 are supported in this implementation");
        }

        List<byte[]> heapIds = new ArrayList<>();
        if (header.totalNumberOfRecords == 0) {
            return heapIds;
        }

        // Compute maximum records per node for sizing fields
        int maxRecordsInLeaf = (header.nodeSize - 8) / header.recordSize; // signature + version + type + checksum
        int maxRecordsInInternal = (header.nodeSize - 8 - sizeOfOffsets - computeRecordCountSize(maxRecordsInLeaf)) /
                (header.recordSize + sizeOfOffsets + computeRecordCountSize(maxRecordsInLeaf));
        if (header.depth > 1) {
            maxRecordsInInternal = (header.nodeSize - 8 - sizeOfOffsets - computeRecordCountSize(maxRecordsInLeaf) -
                    computeTotalRecordCountSize(maxRecordsInLeaf, header.depth)) /
                    (header.recordSize + sizeOfOffsets + computeRecordCountSize(maxRecordsInLeaf) +
                            computeTotalRecordCountSize(maxRecordsInLeaf, header.depth));
        }

        // Start traversal from root node
        collectHeapIdsFromNode(header.rootNodeAddress, header.depth, header.type, header.recordSize, maxRecordsInLeaf,
                maxRecordsInInternal, heapIds);
        return heapIds;
    }

    private void collectHeapIdsFromNode(long address, int depth, int type, int recordSize, int maxRecordsInLeaf,
                                        int maxRecordsInInternal, List<byte[]> heapIds) throws IOException {
        if (address == -1) { // Undefined address
            return;
        }

        channel.position(address);
        String signature = readString(4);
        byte version = readByte();
        if (version != 0) {
            throw new IOException("Unsupported node version: " + version);
        }

        if ("BTLF".equals(signature)) {
            // Leaf node
            if (depth != 0) {
                throw new IOException("Expected leaf node at depth 0, but depth is " + depth);
            }
            LeafNode leaf = readLeafNode(type, recordSize, maxRecordsInLeaf);
            parseLeafNodeRecords(leaf, type, heapIds);
        } else if ("BTIN".equals(signature)) {
            // Internal node
            if (depth == 0) {
                throw new IOException("Expected internal node at depth > 0, but depth is 0");
            }
            InternalNode internal = readInternalNode(type, recordSize, maxRecordsInInternal, depth);
            parseInternalNodeRecords(internal, type, heapIds);
            int recordCountSize = computeRecordCountSize(maxRecordsInLeaf);
            int totalRecordCountSize = depth > 1 ? computeTotalRecordCountSize(maxRecordsInLeaf, depth - 1) : 0;
            for (int i = 0; i < internal.childNodeAddresses.length; i++) {
                collectHeapIdsFromNode(internal.childNodeAddresses[i], depth - 1, type, recordSize, maxRecordsInLeaf,
                        maxRecordsInInternal, heapIds);
            }
        } else {
            throw new IOException("Invalid node signature: " + signature);
        }
    }

    private LeafNode readLeafNode(int type, int recordSize, int maxRecordsInLeaf) throws IOException {
        LeafNode node = new LeafNode();
        byte typeByte = readByte();
        if (typeByte != type) {
            throw new IOException("Leaf node type " + typeByte + " does not match header type " + type);
        }

        // Read records
        int numRecords = maxRecordsInLeaf; // Read maximum possible, adjust based on actual data
        node.records = new byte[numRecords * recordSize];
        ByteBuffer recordBuf = ByteBuffer.wrap(node.records);
        int bytesRead = channel.read(recordBuf);
        if (bytesRead < recordSize) {
            throw new IOException("Failed to read leaf node records");
        }
        node.records = new byte[bytesRead];
        System.arraycopy(recordBuf.array(), 0, node.records, 0, bytesRead);

        // Checksum
        node.checksum = readUInt32();
        // TODO: Verify checksum

        return node;
    }

    private InternalNode readInternalNode(int type, int recordSize, int maxRecordsInInternal, int depth) throws IOException {
        InternalNode node = new InternalNode();
        byte typeByte = readByte();
        if (typeByte != type) {
            throw new IOException("Internal node type " + typeByte + " does not match header type " + type);
        }

        // Read records
        int numRecords = maxRecordsInInternal; // Read maximum possible, adjust based on actual data
        node.records = new byte[numRecords * recordSize];
        ByteBuffer recordBuf = ByteBuffer.wrap(node.records);
        int bytesRead = channel.read(recordBuf);
        if (bytesRead < recordSize) {
            throw new IOException("Failed to read internal node records");
        }
        node.records = new byte[bytesRead];
        System.arraycopy(recordBuf.array(), 0, node.records, 0, bytesRead);

        // Read child pointers
        int recordCountSize = computeRecordCountSize(maxRecordsInInternal);
        int totalRecordCountSize = depth > 1 ? computeTotalRecordCountSize(maxRecordsInInternal, depth - 1) : 0;
        node.childNodeAddresses = new long[numRecords + 1];
        node.numberOfRecordsInChild = new int[numRecords + 1];
        node.totalNumberOfRecordsInChild = depth > 1 ? new long[numRecords + 1] : null;

        for (int i = 0; i <= numRecords; i++) {
            node.childNodeAddresses[i] = readUnsigned(sizeOfOffsets);
            node.numberOfRecordsInChild[i] = (int) readUnsigned(recordCountSize);
            if (depth > 1) {
                node.totalNumberOfRecordsInChild[i] = readUnsigned(totalRecordCountSize);
            }
        }

        // Checksum
        node.checksum = readUInt32();
        // TODO: Verify checksum

        return node;
    }

    private void parseLeafNodeRecords(LeafNode node, int type, List<byte[]> heapIds) {
        ByteBuffer buf = ByteBuffer.wrap(node.records).order(ByteOrder.LITTLE_ENDIAN);
        int recordSize = node.records.length / Math.max(1, node.records.length / (type == 5 ? 11 : 15)); // Adjust based on type
        while (buf.hasRemaining()) {
            if (type == 5) {
                Type5Record record = new Type5Record();
                record.hashOfName = buf.getInt();
                record.heapId = new byte[7];
                buf.get(record.heapId);
                heapIds.add(record.heapId);
            } else if (type == 6) {
                Type6Record record = new Type6Record();
                record.creationOrder = buf.getLong();
                record.heapId = new byte[7];
                buf.get(record.heapId);
                heapIds.add(record.heapId);
            }
        }
    }

    private void parseInternalNodeRecords(InternalNode node, int type, List<byte[]> heapIds) {
        ByteBuffer buf = ByteBuffer.wrap(node.records).order(ByteOrder.LITTLE_ENDIAN);
        int recordSize = node.records.length / Math.max(1, node.records.length / (type == 5 ? 11 : 15));
        while (buf.hasRemaining()) {
            if (type == 5) {
                Type5Record record = new Type5Record();
                record.hashOfName = buf.getInt();
                record.heapId = new byte[7];
                buf.get(record.heapId);
                heapIds.add(record.heapId);
            } else if (type == 6) {
                Type6Record record = new Type6Record();
                record.creationOrder = buf.getLong();
                record.heapId = new byte[7];
                buf.get(record.heapId);
                heapIds.add(record.heapId);
            }
        }
    }

    private int computeRecordCountSize(int maxRecordsInLeaf) {
        return (int) Math.ceil(Math.log(maxRecordsInLeaf + 1) / Math.log(256));
    }

    private int computeTotalRecordCountSize(int maxRecordsInLeaf, int depth) {
        long maxTotalRecords = maxRecordsInLeaf;
        for (int i = 0; i < depth; i++) {
            maxTotalRecords *= maxRecordsInLeaf;
        }
        return (int) Math.ceil(Math.log(maxTotalRecords + 1) / Math.log(256));
    }

    // Helper: Read fixed-length string (ASCII)
    private String readString(int size) throws IOException {
        byte[] bytes = new byte[size];
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        if (channel.read(buf) != size) {
            throw new IOException("Failed to read string of size " + size);
        }
        return new String(bytes, "US-ASCII");
    }

    // Helper: Read single byte
    private byte readByte() throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1);
        if (channel.read(buf) != 1) {
            throw new IOException("Failed to read byte");
        }
        buf.flip();
        return buf.get();
    }

    // Helper: Read unsigned int16 (2 bytes)
    private int readUInt16() throws IOException {
        return (int) readUnsigned(2);
    }

    // Helper: Read unsigned int32 (4 bytes)
    private int readUInt32() throws IOException {
        return (int) readUnsigned(4);
    }

    // Helper: Read unsigned integer of variable byte size (little-endian)
    private long readUnsigned(int size) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN);
        if (channel.read(buf) != size) {
            throw new IOException("Failed to read unsigned integer of size " + size);
        }
        buf.flip();
        long value = 0;
        for (int i = 0; i < size; i++) {
            value |= (buf.get() & 0xFFL) << (i * 8);
        }
        return value;
    }

//    public static void main(String[] args) throws IOException {
//        // Example usage:
//        // SeekableByteChannel channel = Files.newByteChannel(Paths.get("file.h5"), StandardOpenOption.READ);
//        // channel.position(btreeAddress);
//        V2BTreeReader reader = new V2BTreeReader(/* channel */, 8, 8);
//        BTreeHeader header = reader.readHeader();
//        System.out.println("B-tree Type: " + header.type);
//        System.out.println("Root Node Address: " + header.rootNodeAddress);
//
//        // Get heap IDs from Name Index (type 5)
//        List<byte[]> nameIndexHeapIds = reader.getAllHeapIds(/* nameIndexAddress */, 5);
//        System.out.println("Name Index Heap IDs: " + nameIndexHeapIds.size());
//
//        // Get heap IDs from Creation Order Index (type 6)
//        List<byte[]> creationOrderHeapIds = reader.getAllHeapIds(/* creationOrderAddress */, 6);
//        System.out.println("Creation Order Heap IDs: " + creationOrderHeapIds.size());
//
//        // Use heap IDs with FractalHeapReader
//        // FractalHeapReader heapReader = new FractalHeapReader(channel, 8, 8);
//        // heapReader.readHeader();
//        // for (byte[] heapId : nameIndexHeapIds) {
//        //     byte[] objectData = heapReader.getObjectData(heapId);
//        //     System.out.println("Object Data Length: " + objectData.length);
//        // }
//    }
}