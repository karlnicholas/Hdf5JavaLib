package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfFileAllocation; // Ensure this import exists

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer; // Assuming this exists


@Getter
@Slf4j
public class HdfBTreeV1 {
    private final String signature;
    private final int nodeType;
    private final int nodeLevel;
    private int entriesUsed;
    private final HdfFixedPoint leftSiblingAddress;
    private final HdfFixedPoint rightSiblingAddress;
    private final HdfFixedPoint keyZero; // Kept as requested
    private final List<HdfBTreeEntry> entries; // Entries now contain SNODs or child B-trees

    // Keeping original constructors - though only the first one makes sense with recursive loading
    // read from file constructor
    public HdfBTreeV1(
            String signature,
            int nodeType,
            int nodeLevel,
            int entriesUsed,
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress,
            HdfFixedPoint keyZero,
            List<HdfBTreeEntry> entries // This list is now populated recursively
    ) {
        this.signature = signature;
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.entriesUsed = entriesUsed;
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.keyZero = keyZero;
        this.entries = entries;
    }

    // This constructor might be less useful now, as entries are built recursively
    // build for writing constructor
    public HdfBTreeV1(
            String signature,
            int nodeType,
            int nodeLevel,
            // entriesUsed will be managed internally by addDataset now
            HdfFixedPoint leftSiblingAddress,
            HdfFixedPoint rightSiblingAddress
    ) {
        this.signature = signature;
        this.nodeType = nodeType;
        this.nodeLevel = nodeLevel;
        this.entriesUsed = 0; // Start with 0 entries, managed by addDataset
        this.leftSiblingAddress = leftSiblingAddress;
        this.rightSiblingAddress = rightSiblingAddress;
        this.keyZero = HdfFixedPoint.of(0); // Assume keyZero is always 0 for newly created B-Trees
        this.entries = new ArrayList<>(); // Initialize empty list
    }


    // --- Entry point for recursive reading ---
    public static HdfBTreeV1 readFromFileChannel(FileChannel fileChannel, short offsetSize, short lengthSize) throws IOException {
        long initialAddress = fileChannel.position();
        // Use a helper to handle recursion and prevent cycles
        return readFromFileChannelRecursive(fileChannel, initialAddress, offsetSize, lengthSize, new HashMap<>());
    }

    // --- Recursive Helper Method --- (Unchanged) ---
    private static HdfBTreeV1 readFromFileChannelRecursive(FileChannel fileChannel,
                                                           long nodeAddress,
                                                           short offsetSize,
                                                           short lengthSize,
                                                           Map<Long, HdfBTreeV1> visitedNodes // Cycle detection/cache
    ) throws IOException { // Throws standard exceptions

        // --- Cycle Detection ---
        if (visitedNodes.containsKey(nodeAddress)) {
            throw new IllegalStateException("Cycle detected or node re-visited: BTree node address "
                    + nodeAddress + " encountered again during recursive read.");
        }

        // --- Position and Read Header ---
        fileChannel.position(nodeAddress);
        long startPos = nodeAddress;

        int headerSize = 8 + offsetSize + offsetSize;
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        int headerBytesRead = fileChannel.read(headerBuffer);
        if (headerBytesRead != headerSize) {
            throw new IOException("Could not read complete BTree header (" + headerSize + " bytes) at position " + startPos + ". Read only " + headerBytesRead + " bytes.");
        }
        headerBuffer.flip();

        byte[] signatureBytes = new byte[4];
        headerBuffer.get(signatureBytes);
        String signature = new String(signatureBytes);
        if (!"TREE".equals(signature)) {
            throw new IOException("Invalid B-tree node signature: '" + signature + "' at position " + startPos);
        }

        int nodeType = Byte.toUnsignedInt(headerBuffer.get());
        int nodeLevel = Byte.toUnsignedInt(headerBuffer.get());
        int entriesUsed = Short.toUnsignedInt(headerBuffer.getShort());

        BitSet emptyBitset = new BitSet();
        HdfFixedPoint leftSiblingAddress = HdfFixedPoint.checkUndefined(headerBuffer, offsetSize) ? HdfFixedPoint.undefined(headerBuffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(headerBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
        HdfFixedPoint rightSiblingAddress = HdfFixedPoint.checkUndefined(headerBuffer, offsetSize) ? HdfFixedPoint.undefined(headerBuffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(headerBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));

        // --- Read Key/Pointer Block ---
        int entriesDataSize = lengthSize + (entriesUsed * (offsetSize + lengthSize));
        if (entriesUsed < 0 || entriesDataSize < lengthSize) {
            throw new IOException("Invalid BTree node parameters at position " + startPos + ": entriesUsed=" + entriesUsed);
        }
        long currentPosBeforeEntries = fileChannel.position();
        long fileSize = fileChannel.size();
        if (currentPosBeforeEntries + entriesDataSize > fileSize) {
            throw new IOException("Calculated BTree entriesDataSize (" + entriesDataSize + ") exceeds available file size at position " + currentPosBeforeEntries + " (Node: " + startPos + ")");
        }

        ByteBuffer entriesBuffer = ByteBuffer.allocate(entriesDataSize).order(java.nio.ByteOrder.LITTLE_ENDIAN);
        if (entriesDataSize > 0) {
            int entryBytesRead = fileChannel.read(entriesBuffer);
            if (entryBytesRead != entriesDataSize) {
                throw new IOException("Could not read complete BTree entries data block (" + entriesDataSize + " bytes) at position " + currentPosBeforeEntries + ". Read only " + entryBytesRead + " bytes.");
            }
            entriesBuffer.flip();
        } else if (entriesUsed > 0) {
            throw new IOException("BTree node at " + startPos + " has entriesUsed=" + entriesUsed + " but entriesDataSize is 0.");
        }


        HdfFixedPoint keyZero = HdfFixedPoint.readFromByteBuffer(entriesBuffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));

        List<HdfBTreeEntry> entries = new ArrayList<>(entriesUsed);
        long filePosAfterEntriesBlock = fileChannel.position();

        // --- Prepare Node and Mark Visited ---
        HdfBTreeV1 currentNode = new HdfBTreeV1(signature, nodeType, nodeLevel, entriesUsed, leftSiblingAddress, rightSiblingAddress, keyZero, entries);
        visitedNodes.put(nodeAddress, currentNode); // Mark before recursion

        // --- Process Entries ---
        for (int i = 0; i < entriesUsed; i++) {
            HdfFixedPoint childPointer = HdfFixedPoint.readFromByteBuffer(entriesBuffer, offsetSize, emptyBitset, (short) 0, (short)(offsetSize*8));
            HdfFixedPoint key = HdfFixedPoint.readFromByteBuffer(entriesBuffer, lengthSize, emptyBitset, (short) 0, (short)(lengthSize*8));
            long childAddress = childPointer.getInstance(Long.class);
            HdfBTreeEntry entry = null;

            if (childAddress < -1L || (childAddress != -1L && childAddress >= fileSize)) {
                throw new IOException("Invalid child address " + childAddress + " in BTree entry " + i
                        + " at node " + startPos + " (Level " + nodeLevel + "). File size is " + fileSize);
            }

            if (nodeLevel == 0) { // --- LEAF level node ---
                if (childAddress != -1L) {
                    fileChannel.position(childAddress);
                    HdfGroupSymbolTableNode snod = HdfGroupSymbolTableNode.readFromFileChannel(fileChannel, offsetSize);
                    int snodEntriesToRead = snod.getNumberOfSymbols();
                    for (int e = 0; e < snodEntriesToRead; ++e) {
                        HdfSymbolTableEntry snodEntry = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);
                        snod.getSymbolTableEntries().add(snodEntry);
                    }
                    entry = new HdfBTreeEntry(key, childPointer, snod);
                    fileChannel.position(filePosAfterEntriesBlock); // Restore position
                } else {
                    entry = new HdfBTreeEntry(key, childPointer, (HdfGroupSymbolTableNode) null);
                }
            } else { // --- INTERNAL level node (nodeLevel > 0) ---
                if (childAddress != -1L) {
                    HdfBTreeV1 childNode = readFromFileChannelRecursive(fileChannel, childAddress, offsetSize, lengthSize, visitedNodes);
                    entry = new HdfBTreeEntry(key, childPointer, childNode);
                    fileChannel.position(filePosAfterEntriesBlock); // Restore position
                } else {
                    entry = new HdfBTreeEntry(key, childPointer, (HdfBTreeV1) null);
                }
            }
            entries.add(entry);
        }
        return currentNode;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HdfBTreeV1{");
        sb.append("signature='").append(signature).append('\'');
        sb.append(", nodeType=").append(nodeType);
        sb.append(", nodeLevel=").append(nodeLevel);
        sb.append(", entriesUsed=").append(entriesUsed);
        sb.append(", leftSiblingAddress=").append(leftSiblingAddress);
        sb.append(", rightSiblingAddress=").append(rightSiblingAddress);
        sb.append(", keyZero=").append(keyZero);
        sb.append(", entries=["); // Start listing entries

        if (entries != null && !entries.isEmpty()) {
            boolean first = true;
            for (HdfBTreeEntry entry : entries) {
                if (!first) {
                    sb.append(", "); // Separator between entries
                }
                sb.append(entry); // Implicitly calls entry.toString()
                first = false;
            }
        } else if (entries != null) {
            sb.append("<empty>");
        } else {
            sb.append("<null>");
        }

        sb.append("]"); // End listing entries
        sb.append('}');
        return sb.toString();
    }

    // Helper methods (Optional but Recommended)
    public boolean isLeafLevelNode() { return this.nodeLevel == 0; }
    public boolean isInternalLevelNode() { return this.nodeLevel > 0; }

    /**
     * Adds a dataset entry to the Symbol Table Node managed by the first B-Tree entry.
     * Creates the first B-Tree entry and its associated SNOD if they don't exist.
     * Limited to 8 dataset entries within the single SNOD.
     *
     * @param linkNameOffset           Offset in the Local Heap where the dataset's link name is stored.
     * @param datasetObjectHeaderAddress Address of the dataset's Object Header.
     * @throws IllegalStateException if called on a non-leaf node, if the SNOD limit (8) is reached,
     *                               or if the internal state is inconsistent.
     */
    public void addDataset(long linkNameOffset, long datasetObjectHeaderAddress) { // Renamed method and parameters
        // Ensure this method only makes sense for leaf nodes holding group info
        if (!isLeafLevelNode()) {
            throw new IllegalStateException("addDataset can only be called on leaf B-tree nodes (nodeLevel 0).");
        }

        HdfFileAllocation fileAllocation = HdfFileAllocation.getInstance();
        HdfGroupSymbolTableNode targetSnod;

        if (entries.isEmpty()) {
            // First dataset being added to this B-tree node. Create the first B-tree entry and its SNOD.
            if (entriesUsed != 0) {
                // Inconsistent state check
                throw new IllegalStateException("B-tree entries list is empty but entriesUsed is " + entriesUsed);
            }

            // 1. Allocate space for the new Symbol Table Node (SNOD)
            final int MAX_SNOD_ENTRIES = 8;
            long snodOffset = fileAllocation.allocateNextSnodStorage(); // Allocate SNOD space

            // 2. Create the new SNOD object
            targetSnod = new HdfGroupSymbolTableNode("SNOD", 1, 0, new ArrayList<>(MAX_SNOD_ENTRIES)); // Initial capacity 8

            // 3. Create the new B-tree entry pointing to the SNOD
            HdfFixedPoint key = HdfFixedPoint.of(linkNameOffset); // First entry's key often relates to first link name? Or should it be 0? Check HDF5 spec. Using linkNameOffset for now.
            HdfFixedPoint childPointer = HdfFixedPoint.of(snodOffset);
            HdfBTreeEntry newEntry = new HdfBTreeEntry(key, childPointer, targetSnod);

            // 4. Add the entry to the B-tree
            entries.add(newEntry);
            entriesUsed++; // Increment B-tree entry count

        } else {
            // B-tree already has an entry (we assume only one for now)
            if (entriesUsed != 1) {
                // For this simplified version, only handle the case where exactly one entry exists
                throw new IllegalStateException("addDataset currently only supports adding to the SNOD within the first B-tree entry. entriesUsed=" + entriesUsed);
            }
            HdfBTreeEntry firstEntry = entries.get(0);
            targetSnod = firstEntry.getSymbolTableNode();
            if (targetSnod == null) {
                // This indicates an internal node or an invalid state for this simplified method
                throw new IllegalStateException("The first B-tree entry does not contain a Symbol Table Node.");
            }
        }

        // Now, add the symbol table entry to the target SNOD

        // Check if the SNOD is full (max 8 entries)
        if (targetSnod.getNumberOfSymbols() >= 8) { // Check against the actual limit
            throw new IllegalStateException("Cannot add more than 8 datasets to this Symbol Table Node.");
        }

        // Create the new Symbol Table Entry for the dataset
        HdfSymbolTableEntry ste = new HdfSymbolTableEntry(
                HdfFixedPoint.of(linkNameOffset),           // Cache Type 0: Link name offset
                HdfFixedPoint.of(datasetObjectHeaderAddress) // Object Header Address
                // Assuming default cache type and other fields for STE
        );

        // Add the entry to the SNOD
        targetSnod.addEntry(ste);
        // Note: We do NOT increment this.entriesUsed here, as that tracks B-tree entries, not SNOD entries.
        // The SNOD itself tracks its number of symbols internally via getNumberOfSymbols().
    }


    public void writeToByteBuffer(ByteBuffer buffer) {
        buffer.put(signature.getBytes());
        buffer.put((byte) nodeType);
        buffer.put((byte) nodeLevel);
        buffer.putShort((short) entriesUsed);
        writeFixedPointToBuffer(buffer, leftSiblingAddress);
        writeFixedPointToBuffer(buffer, rightSiblingAddress);
        writeFixedPointToBuffer(buffer, keyZero);

        for (HdfBTreeEntry entry : entries) {
            // Write B-tree entry Key (Child Pointer is written first?) - Check format!
            // Assuming format: ChildPtr, Key
            writeFixedPointToBuffer(buffer, entry.getChildPointer());
            if (entry.getKey() != null) {
                writeFixedPointToBuffer(buffer, entry.getKey());
            } else {
                // Handle null key? Write undefined address? Depends on spec.
                // For now, assume key is never null for valid entries being written.
                throw new NullPointerException("BTree Entry key cannot be null during write");
            }

            // --- Writing pointed-to data needs separate logic ---
            // The B-Tree node itself only contains the pointers/keys.
            // Writing the SNODs or child B-Trees happens elsewhere, triggered by the caller.
            // The commented-out logic below was incorrect as it tried to write SNOD data
            // *within* the B-Tree node's buffer space.

            /* // Incorrect logic removed:
            if (entry.getSymbolTableNode() != null) {
                // This is wrong - writing SNOD data here overwrites B-Tree structure
                // buffer.position(entry.getChildPointer().getInstance(Long.class).intValue()); // Don't change position
                // entry.getSymbolTableNode().writeToBuffer(buffer); // Don't write SNOD here
            }
            */
        }
        // Pad buffer? B-Tree nodes often have fixed sizes based on k value. Not handled here.
    }
}