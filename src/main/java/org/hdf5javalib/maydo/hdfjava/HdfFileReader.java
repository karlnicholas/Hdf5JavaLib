package org.hdf5javalib.maydo.hdfjava;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.datatype.FixedPointDatatype;
import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefix;
import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefixV2;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.HdfMessage;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.ObjectHeaderContinuationMessage;
import org.hdf5javalib.maydo.hdffile.infrastructure.*;
import org.hdf5javalib.maydo.hdffile.metadata.HdfSuperblock;
import org.hdf5javalib.maydo.utils.HdfReadUtils;
import org.hdf5javalib.maydo.utils.HdfWriteUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

import static org.hdf5javalib.maydo.datatype.FixedPointDatatype.BIT_MULTIPLIER;
import static org.hdf5javalib.maydo.hdffile.dataobjects.HdfObjectHeaderPrefixV1.*;
import static org.hdf5javalib.maydo.hdffile.infrastructure.HdfGroupSymbolTableNode.GROUP_SYMBOL_TABLE_NODE_SIGNATURE;
import static org.hdf5javalib.maydo.hdffile.infrastructure.HdfLocalHeap.*;
import static org.hdf5javalib.maydo.hdffile.infrastructure.HdfSymbolTableEntry.RESERVED_FIELD_1_SIZE;
import static org.hdf5javalib.maydo.hdffile.infrastructure.HdfSymbolTableEntryCacheNoScratch.SYMBOL_TABLE_ENTRY_SCRATCH_SIZE;
import static org.hdf5javalib.maydo.hdffile.metadata.HdfSuperblock.*;

/**
 * Reads and parses HDF5 file structures.
 * <p>
 * The {@code HdfFileReader} class implements {@link org.hdf5javalib.HdfDataFile} to provide functionality
 * for reading an HDF5 file from a {@link SeekableByteChannel}. It initializes the superblock,
 * root group, global heap, and file allocation, and constructs a hierarchy of groups and
 * datasets by parsing the file's metadata and data structures.
 * </p>
 */
public class HdfFileReader implements HdfDataFile {
//    /** The superblock containing metadata about the HDF5 file. */
//    private HdfSuperblock superblock;
    public static final byte[] BTREE_SIGNATURE = {'T', 'R', 'E', 'E'};
    public static final int BTREE_HEADER_INITIAL_SIZE = 8;
    private static final int MAX_SNOD_ENTRIES = 8;

    /**
     * The seekable byte channel for reading the HDF5 file.
     */
    private final SeekableByteChannel fileChannel;

    /**
     * The global heap for storing variable-length data.
     */
    private final HdfGlobalHeap globalHeap;

    /**
     * The file allocation manager for tracking storage blocks.
     */
    private HdfFileAllocation fileAllocation;
    private HdfSuperblock superblock;
    private HdfBTree bTree;

    /**
     * Constructs an HdfFileReader for reading an HDF5 file.
     *
     * @param fileChannel the seekable byte channel for accessing the HDF5 file
     */
    public HdfFileReader(SeekableByteChannel fileChannel) {
        this.fileChannel = fileChannel;
//        this.fileAllocation = null;
        this.globalHeap = new HdfGlobalHeap(this::initializeGlobalHeap, this);
    }

    /**
     * Reads and parses the HDF5 file structure.
     * <p>
     * Initializes the superblock, root group, B-tree, local heap, and datasets by
     * reading from the file channel. Constructs the group and dataset hierarchy and
     * returns this reader instance for further operations.
     * </p>
     *
     * @return this HdfFileReader instance
     * @throws IOException if an I/O error occurs during reading
     */
    public HdfFileReader readFile() throws Exception {
        long superblockOffset = findSuperblockOffset(fileChannel);
        superblock = readSuperblockFromSeekableByteChannel(fileChannel, superblockOffset, this);
        HdfSymbolTableEntry rootGroupSymbolTableEntry = readSteFromSeekableByteChannel(fileChannel, this);
        // determine version of data object headers
        long objectHeaderAddress = rootGroupSymbolTableEntry.getObjectHeaderAddress().getInstance(Long.class);
        // The first part of the header is 6 bytes: Signature (4) + Version (1) + Flags (1)
        ByteBuffer headerStartBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(headerStartBuffer);
        headerStartBuffer.flip();
        int version = Byte.toUnsignedInt(headerStartBuffer.get());
        if( version > 1 ) {
            throw new UnsupportedOperationException("V2 architecture not supported. Header Object version: " + version);
        }
        HdfObjectHeaderPrefix objectHeader = readObjectHeader(fileChannel, objectHeaderAddress, this);
        long heapOffset = ((HdfSymbolTableEntryCacheWithScratch)rootGroupSymbolTableEntry.getCache()).getLocalHeapAddress().getInstance(Long.class);
        long bTreeAddress = ((HdfSymbolTableEntryCacheWithScratch)rootGroupSymbolTableEntry.getCache()).getbTreeAddress().getInstance(Long.class);
        HdfLocalHeap localHeap = readLocalHeapFromSeekableByteChannel(fileChannel, heapOffset, this);
        HdfBTreeV1 groupBTree = readBTreeFromSeekableByteChannel(fileChannel, bTreeAddress, this);
        String groupName = localHeap.stringAtOffset(rootGroupSymbolTableEntry.getLinkNameOffset());
        // set BTree
        HdfGroup groupObject = new HdfGroup(groupName, objectHeader, null);
        bTree = new HdfBTree(groupObject);
        // recurse through infrastructure
        readInfrastructure(groupObject, localHeap, groupBTree);

        return this;
    }

    private static long findSuperblockOffset(SeekableByteChannel fileChannel) throws IOException {
        long size = fileChannel.size();
        long offset = 0;
        while ( (offset + SIGNATURE_SIZE + VERSION_SIZE) < size ) {
            fileChannel.position(offset);
            // Step 1: Allocate the minimum buffer size to determine the version
            ByteBuffer buffer = ByteBuffer.allocate(SIGNATURE_SIZE + VERSION_SIZE); // File signature (8 bytes) + version (1 byte)
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            // Read the initial bytes to determine the version
            fileChannel.read(buffer);
            buffer.flip();

            // Verify file signature
            byte[] signature = new byte[FILE_SIGNATURE.length];
            buffer.get(signature);
            if (!java.util.Arrays.equals(signature, FILE_SIGNATURE)) {
                offset += 500;
                continue;
            }

            // Read version
            int version = Byte.toUnsignedInt(buffer.get());
            if ( version > 1) {
                throw new UnsupportedOperationException("V2 architecture not supported. Superblock version: " + version);
            }
            return offset;
        }
        throw new IllegalArgumentException("HDF file signature not found");
    }

    private void readInfrastructure(HdfGroup parentGroup, HdfLocalHeap localHeap, HdfBTreeV1 groupBTree) throws Exception {
        for( HdfBTreeEntryBase entry: groupBTree.getEntries()) {
            HdfGroupSymbolTableNode groupSymbolTableNode = ((HdfGroupBTreeEntry)entry).getGroupSymbolTableNode();
            for( HdfSymbolTableEntry symbolTableEntry: groupSymbolTableNode.getSymbolTableEntries() ) {
                String objectName = localHeap.stringAtOffset(symbolTableEntry.getLinkNameOffset());
                HdfObjectHeaderPrefixV1 objectHeader = readObjectHeader(fileChannel, symbolTableEntry.getObjectHeaderAddress().getInstance(Long.class), this);
                switch ( symbolTableEntry.getCache().getCacheType()) {
                case 0:
                    HdfDataset datasetObject = new HdfDataset(objectName, objectHeader, parentGroup);
                    parentGroup.addChild(datasetObject);
                    break;
                case 1:
                    long newHeapOffset = ((HdfSymbolTableEntryCacheWithScratch)symbolTableEntry.getCache()).getLocalHeapAddress().getInstance(Long.class);
                    long newBTreeAddress = ((HdfSymbolTableEntryCacheWithScratch)symbolTableEntry.getCache()).getbTreeAddress().getInstance(Long.class);
                    HdfLocalHeap newLocalHeap = readLocalHeapFromSeekableByteChannel(fileChannel, newHeapOffset, this);
                    HdfBTreeV1 newGroupBTree = readBTreeFromSeekableByteChannel(fileChannel, newBTreeAddress, this);
                    HdfGroup groupObject = new HdfGroup(objectName, objectHeader, parentGroup);
                    parentGroup.addChild(groupObject);
                    readInfrastructure(groupObject, newLocalHeap, newGroupBTree);
                    break;
                }
            }
        }
//        HdfObjectHeaderPrefixV1 objectHeader = readObjectHeader(fileChannel, this);
    }

    /**
     * Retrieves all datasets in the group.
     *
     * @return a collection of
     */
    public Iterator<HdfDataset> datasetIterator() {
//        return bTree.getEntries().stream()
//                .filter(bte -> bte.getGroupSymbolTableNode().isPresent())
//                .flatMap(bte -> bte.getGroupSymbolTableNode().get().getSymbolTableEntries().stream())
//                .filter(ste -> ste.getCache() instanceof HdfSymbolTableEntryCacheNoScratch)
//                .map(ste -> ((HdfSymbolTableEntryCacheNoScratch) ste.getCache()).getDataSet())
//                .toList();
        return bTree.datasetIterator();

    }
    public List<HdfDataset> getDatasets() {
        // Convert Iterator to List using a for loop
        List<HdfDataset> resultList = new ArrayList<>();
        for (Iterator<HdfDataset> iterator = datasetIterator(); iterator.hasNext(); ) {
            resultList.add(iterator.next());
        }
        return resultList;
    }

//    private Optional<HdfDataObject> findTypeInBTree(HdfBTree bTree, String[] components, int index, String currentComponent) {
//        if (bTree == null || components == null || index >= components.length || currentComponent == null) {
//            return Optional.empty();
//        }
////        Optional<HdfDataObject> result = bTree.findObjectByName(currentComponent, this);
////        if (result.isPresent()) {
////            return findTypeByPath(components, index + 1, result.get());
////        }
//        return Optional.empty();
//    }
    public Optional<HdfDataset> getDataset(String path) {
        return getHdfDataObject(path, HdfDataset.class);
    }
    public Optional<HdfGroup> getGroup(String path) {
        return getHdfDataObject(path, HdfGroup.class);
    }

    public <T extends HdfDataObject> Optional<T> getHdfDataObject(String path, Class<T> clazz) {
        if (path == null || path.isEmpty()) {
            return Optional.empty();
        }
        String cleanedPath = path.startsWith("/") ? path.substring(1) : path;
        if (cleanedPath.isEmpty()) {
            return Optional.empty();
        }
        String[] components = cleanedPath.split("/");
        if (components.length == 0) {
            return Optional.empty();
        }
        HdfGroup currentGroup = bTree.getRoot();
        int compIndex = 0;
        while (currentGroup != null) {
            Optional<HdfBTreeNode> child = currentGroup.findChildByName(components[compIndex++]);
            if ( child.isEmpty() ) {
                return Optional.empty();
            } else if (compIndex == components.length && clazz.isAssignableFrom(child.get().getDataObject().getClass())) {
                return child.map(clazz::cast);
            } else if ( compIndex < components.length - 1 && HdfGroup.class.isAssignableFrom(child.get().getDataObject().getClass()) ) {
                currentGroup = (HdfGroup) child.get();
            } else {
                currentGroup = null;
            }
        }
        return Optional.empty();
    }

    /**
     * Reads a Superblock from a file channel.
     * <p>
     * Parses the superblock metadata, including the file signature, version, offset and length sizes,
     * B-tree settings, and address fields, from the specified file channel. Validates the signature
     * and version, and constructs a Superblock instance with the parsed data.
     * </p>
     *
     * @param fileChannel the seekable byte channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfSuperblock instance
     * @throws IOException              if an I/O error occurs
     * @throws IllegalArgumentException if the file signature is invalid or the version is unsupported
     */
    public static HdfSuperblock readSuperblockFromSeekableByteChannel(SeekableByteChannel fileChannel, long superblockOffset, HdfDataFile hdfDataFile) throws Exception {
        fileChannel.position(superblockOffset);
        // Step 1: Allocate the minimum buffer size to determine the version
        ByteBuffer buffer = ByteBuffer.allocate(SIGNATURE_SIZE + VERSION_SIZE); // File signature (8 bytes) + version (1 byte)
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read the initial bytes to determine the version
        fileChannel.read(buffer);
        buffer.flip();

        // Verify file signature
        byte[] signature = new byte[FILE_SIGNATURE.length];
        buffer.get(signature);
        if (!java.util.Arrays.equals(signature, FILE_SIGNATURE)) {
            throw new IllegalArgumentException("Invalid file signature");
        }

        // Read version
        int version = Byte.toUnsignedInt(buffer.get());

        // Step 2: Determine the size of the superblock based on the version
        int superblockSize;
        if (version == 0) {
            superblockSize = SUPERBLOCK_SIZE_V1; // Version 0 superblock size
        } else if (version == 1) {
            superblockSize = SUPERBLOCK_SIZE_V2; // Version 1 superblock size (example value, adjust per spec)
        } else {
            throw new IllegalArgumentException("Unsupported HDF5 superblock version: " + version);
        }

        // Step 3: Allocate a new buffer for the full superblock
        buffer = ByteBuffer.allocate(superblockSize).order(ByteOrder.LITTLE_ENDIAN);

        // Reset file channel position to re-read from the beginning
        fileChannel.position(0);

        // Read the full superblock
        fileChannel.read(buffer);
        buffer.flip();

        // Step 4: Parse the remaining superblock fields
        buffer.position(SIGNATURE_SIZE + VERSION_SIZE); // Skip the file signature
        int freeSpaceVersion = Byte.toUnsignedInt(buffer.get());
        int rootGroupVersion = Byte.toUnsignedInt(buffer.get());
        buffer.get(); // Skip reserved
        int sharedHeaderVersion = Byte.toUnsignedInt(buffer.get());
        int offsetSize = Byte.toUnsignedInt(buffer.get());
        int lengthSize = Byte.toUnsignedInt(buffer.get());
        buffer.get(); // Skip reserved

        FixedPointDatatype fixedPointDatatypeForOffset = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                offsetSize, (short) 0, (short) (BIT_MULTIPLIER * offsetSize), hdfDataFile);
        FixedPointDatatype fixedPointDatatypeForLength = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                lengthSize, (short) 0, (short) (BIT_MULTIPLIER * lengthSize), hdfDataFile);

        int groupLeafNodeK = Short.toUnsignedInt(buffer.getShort());
        int groupInternalNodeK = Short.toUnsignedInt(buffer.getShort());
        buffer.getInt(); // Skip consistency flags

        // Parse addresses using HdfFixedPoint
        HdfFixedPoint baseAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
        HdfFixedPoint freeSpaceAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
        HdfFixedPoint endOfFileAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
        HdfFixedPoint driverInformationAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
        HdfFixedPoint hdfOffset = HdfWriteUtils.hdfFixedPointFromValue(superblockOffset, fixedPointDatatypeForOffset);

        HdfSuperblock superblock = new HdfSuperblock(
                version,
                freeSpaceVersion,
                rootGroupVersion,
                sharedHeaderVersion,
                offsetSize,
                lengthSize,
                groupLeafNodeK,
                groupInternalNodeK,
                baseAddress,
                freeSpaceAddress,
                endOfFileAddress,
                driverInformationAddress,
                hdfDataFile,
                "Superblock",
                hdfOffset,
                fixedPointDatatypeForOffset,
                fixedPointDatatypeForLength
        );
        return superblock;
    }

    /**
     * Reads an HdfBTree from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfBTree instance
     * @throws IOException if an I/O error occurs or the B-Tree data is invalid
     */
    public static HdfBTreeV1 readBTreeFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            long btreeAddress,
            HdfDataFile hdfDataFile
    ) throws Exception {
        return readFromSeekableByteChannelRecursive(fileChannel, btreeAddress, hdfDataFile, new LinkedHashMap<>());
    }

    /**
     * Recursively reads an HdfBTree from a file channel, handling cycles.
     *
     * @param fileChannel  the file channel to read from
     * @param nodeAddress  the address of the current node
     * @param visitedNodes a map of visited node addresses to detect cycles
     * @param hdfDataFile  the HDF5 file context
     * @return the constructed HdfBTree instance
     * @throws IOException if an I/O error occurs or the B-Tree data is invalid
     */
    private static HdfBTreeV1 readFromSeekableByteChannelRecursive(SeekableByteChannel fileChannel,
                                                                 long nodeAddress,
                                                                 HdfDataFile hdfDataFile,
                                                                 Map<Long, HdfBTreeV1> visitedNodes
    ) throws Exception {
        if (visitedNodes.containsKey(nodeAddress)) {
            throw new IllegalStateException("Cycle detected or node re-visited: BTree node address "
                    + nodeAddress + " encountered again during recursive read.");
        }

        fileChannel.position(nodeAddress);
        FixedPointDatatype hdfOffset = hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset();
        final int offsetSize = hdfOffset.getSize();
        FixedPointDatatype hdfLength = hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset();
        final int lengthSize = hdfLength.getSize();

        int headerSize = BTREE_HEADER_INITIAL_SIZE + offsetSize + offsetSize;
        ByteBuffer headerBuffer = ByteBuffer.allocate(headerSize).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(headerBuffer);
        headerBuffer.flip();

        byte[] signatureBytes = new byte[BTREE_SIGNATURE.length];
        headerBuffer.get(signatureBytes);
        if (Arrays.compare(signatureBytes, BTREE_SIGNATURE) != 0) {
            throw new IOException("Invalid B-tree node signature: '" + Arrays.toString(signatureBytes) + "' at position " + nodeAddress);
        }

        int nodeType = Byte.toUnsignedInt(headerBuffer.get());
        int nodeLevel = Byte.toUnsignedInt(headerBuffer.get());
        int entriesUsed = Short.toUnsignedInt(headerBuffer.getShort());

        HdfFixedPoint leftSiblingAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfOffset, headerBuffer);
        HdfFixedPoint rightSiblingAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfOffset, headerBuffer);

        int entriesDataSize = lengthSize + (entriesUsed * (offsetSize + lengthSize));
        ByteBuffer entriesBuffer = ByteBuffer.allocate(entriesDataSize).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(entriesBuffer);
        entriesBuffer.flip();

        HdfFixedPoint keyZero = HdfReadUtils.readHdfFixedPointFromBuffer(hdfLength, entriesBuffer);

        List<HdfBTreeEntryBase> entries = new ArrayList<>(entriesUsed);

        HdfBTreeV1 currentNode = new HdfBTreeV1(nodeType, nodeLevel, entriesUsed, leftSiblingAddress, rightSiblingAddress, keyZero, entries, hdfDataFile,
                HdfWriteUtils.hdfFixedPointFromValue(nodeAddress, hdfOffset));
        visitedNodes.put(nodeAddress, currentNode);

        for (int i = 0; i < entriesUsed; i++) {
            HdfFixedPoint childPointer = HdfReadUtils.readHdfFixedPointFromBuffer(hdfOffset, entriesBuffer);
            HdfFixedPoint key = HdfReadUtils.readHdfFixedPointFromBuffer(hdfLength, entriesBuffer);
            long filePosAfterEntriesBlock = fileChannel.position();
            long childAddress = childPointer.getInstance(Long.class);
            fileChannel.position(childAddress);
            HdfGroupSymbolTableNode snod = readSnodFromSeekableByteChannel(fileChannel, hdfDataFile);
            HdfGroupBTreeEntry entry = new HdfGroupBTreeEntry(key, childPointer, null, snod);

            fileChannel.position(filePosAfterEntriesBlock);
            entries.add(entry);
        }
        return currentNode;
    }

    /**
     * Reads an HdfLocalHeap from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfLocalHeap
     * @throws IOException              if an I/O error occurs
     * @throws IllegalArgumentException if the heap signature or reserved bytes are invalid
     */
    public static HdfLocalHeap readLocalHeapFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            long localHeapOffset,
            HdfDataFile hdfDataFile
    ) throws IOException {

        fileChannel.position(localHeapOffset);
        ByteBuffer buffer = ByteBuffer.allocate(LOCAL_HEAP_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);

        fileChannel.read(buffer);
        buffer.flip();

        byte[] signatureBytes = new byte[LOCAL_HEAP_SIGNATURE.length];
        buffer.get(signatureBytes);
//        String signature = new String(signatureBytes);
        if (Arrays.compare(LOCAL_HEAP_SIGNATURE, signatureBytes) != 0) {
            throw new IllegalArgumentException("Invalid heap signature: " + signatureBytes);
        }

        int version = Byte.toUnsignedInt(buffer.get());

        buffer.position(buffer.position() + LOCAL_HEAP_HEADER_RESERVED_SIZE);

        HdfFixedPoint dataSegmentSize = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), buffer);
        HdfFixedPoint freeListOffset = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
        HdfFixedPoint dataSegmentAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);

        HdfLocalHeapData hdfLocalHeapData = readLocalHeapDataFromSeekableByteChannel(
                fileChannel, dataSegmentSize, freeListOffset, dataSegmentAddress, hdfDataFile);

        return new HdfLocalHeap(version,hdfDataFile, hdfLocalHeapData, localHeapOffset);
    }
    /**
     * Reads an HdfGroupSymbolTableNode from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfGroupSymbolTableNode
     * @throws IOException              if an I/O error occurs or the SNOD signature is invalid
     * @throws IllegalArgumentException if the SNOD signature is invalid
     */
    public static HdfGroupSymbolTableNode readSnodFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile
    ) throws Exception {
        long offset = fileChannel.position();
        ByteBuffer buffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(buffer);
        buffer.flip();
        int version;
        int numberOfSymbols;

        // Read Signature (4 bytes)
        byte[] signatureBytes = new byte[GROUP_SYMBOL_TABLE_NODE_SIGNATURE.length];
        buffer.get(signatureBytes);
        if (Arrays.compare(GROUP_SYMBOL_TABLE_NODE_SIGNATURE, signatureBytes) != 0) {
            throw new IllegalArgumentException("Invalid SNOD signature: " + Arrays.toString(signatureBytes));
        }

        // Read Version (1 byte)
        version = Byte.toUnsignedInt(buffer.get());

        // Skip Reserved Bytes (1 byte)
        buffer.get();

        // Read Number of Symbols (2 bytes, little-endian)
        numberOfSymbols = Short.toUnsignedInt(buffer.getShort());

        // Read Symbol Table Entries
        List<HdfSymbolTableEntry> symbolTableEntries = new ArrayList<>(numberOfSymbols);
        for (int i = 0; i < numberOfSymbols; i++) {
            HdfSymbolTableEntry entry = readSteFromSeekableByteChannel(fileChannel, hdfDataFile);
            symbolTableEntries.add(entry);
        }

        return new HdfGroupSymbolTableNode(
                version,
                symbolTableEntries,
                hdfDataFile,
                HdfWriteUtils.hdfFixedPointFromValue(offset, hdfDataFile.getSuperblock().getFixedPointDatatypeForLength()));
    }

    public static HdfLocalHeapData readLocalHeapDataFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfFixedPoint dataSegmentSize,
            HdfFixedPoint freeListOffset,
            HdfFixedPoint dataSegmentAddress,
            HdfDataFile hdfDataFile
    ) throws IOException {

        Map<HdfFixedPoint, HdfLocalHeapDataValue> data = new LinkedHashMap<>();
        fileChannel.position(dataSegmentAddress.getInstance(Long.class));
        // Allocate buffer and read heap data from the file channel
        byte[] heapData = new byte[dataSegmentSize.getInstance(Long.class).intValue()];
        ByteBuffer buffer = ByteBuffer.wrap(heapData);
        fileChannel.read(buffer);
        buffer.flip();
        long iFreeListOffset = freeListOffset.getInstance(Long.class);
        if (iFreeListOffset == 1) {
            iFreeListOffset = dataSegmentSize.getInstance(Long.class);
        }
        long iOffset = 0;
        while (buffer.position() < iFreeListOffset) {
            long iStart = iOffset;
            // Find the null terminator
            while (iOffset < heapData.length && heapData[Math.toIntExact(iOffset)] != 0) {
                iOffset++;
            }
            String dataValue = new String(heapData, (int) iStart, (int) (iOffset - iStart), StandardCharsets.US_ASCII);
            HdfFixedPoint hdfOffset = HdfWriteUtils.hdfFixedPointFromValue(iStart, freeListOffset.getDatatype());
            HdfLocalHeapDataValue value = new HdfLocalHeapDataValue(dataValue, hdfOffset);
            data.put(hdfOffset, value);
            // 8 to add 1 for the 0 terminator.
            iOffset = (iOffset + 8) & ~7;
            buffer.position((int) iOffset);
        }

        return new HdfLocalHeapData(dataSegmentAddress, dataSegmentSize, freeListOffset, data);
    }

    public static HdfSymbolTableEntryCache readCacheWithScratchFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile
    ) throws Exception {
        // reading for group.
        HdfFixedPoint bTreeAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);
        HdfFixedPoint localHeapAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);
//        long savedPosition = fileChannel.position();
//
//        fileChannel.position(localHeapAddress.getInstance(Long.class));
//        HdfLocalHeap localHeap = readLocalHeapFromSeekableByteChannel(fileChannel, hdfDataFile, objectName);
//
//        fileChannel.position(bTreeAddress.getInstance(Long.class));
//        HdfBTree bTreeV1 = readBTreeFromSeekableByteChannel(fileChannel, hdfDataFile, localHeap, objectName);
//        fileChannel.position(savedPosition);
//        return new HdfSymbolTableEntryCacheWithScratch(objectName, objectHeader, bTreeV1, localHeap, hdfDataFile);
        return new HdfSymbolTableEntryCacheWithScratch(bTreeAddress, localHeapAddress);
    }

    /**
     * Reads an HdfSymbolTableEntry from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HdfDataFile offset fields
     * @return the constructed HdfSymbolTableEntry
     * @throws IOException if an I/O error occurs
     */
    public static HdfSymbolTableEntry readSteFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile
    ) throws Exception {
        // Read the fixed-point values for linkNameOffset and objectHeaderAddress
        HdfFixedPoint linkNameOffset = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);
        HdfFixedPoint objectHeaderAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), fileChannel);

        // Read cache type and skip reserved field
        int cacheType = HdfReadUtils.readIntFromFileChannel(fileChannel);
        HdfReadUtils.skipBytes(fileChannel, RESERVED_FIELD_1_SIZE); // Skip reserved field

//        long savedPosition = fileChannel.position();
//        fileChannel.position(objectHeaderAddress.getInstance(Long.class));
//        String objectName = localHeap == null ? "" : localHeap.stringAtOffset(linkNameOffset);
//        HdfObjectHeaderPrefix objectHeader = readObjectHeaderPrefixFromSeekableByteChannel(
//                fileChannel,
//                hdfDataFile,
//                objectName
//        );
//        fileChannel.position(savedPosition);
        HdfSymbolTableEntryCache cache;
        if (cacheType == 0) {
            cache = readCacheNoScratchFromSeekableByteChannel(fileChannel, hdfDataFile);
        } else if (cacheType == 1) {
            cache = readCacheWithScratchFromSeekableByteChannel(fileChannel, hdfDataFile);
        } else {
            throw new IllegalStateException("Unsupported cache type: " + cacheType);
        }

        return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress, cache);
    }

    public static HdfSymbolTableEntryCacheNoScratch readCacheNoScratchFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile
    ) throws IOException {
        HdfReadUtils.skipBytes(fileChannel, SYMBOL_TABLE_ENTRY_SCRATCH_SIZE); // Skip 16 bytes for scratch-pad
//        return new HdfSymbolTableEntryCacheNoScratch(hdfDataFile, objectHeader, objectName);
        return new HdfSymbolTableEntryCacheNoScratch();
    }
    /**
     * Reads an HdfObjectHeaderPrefixV1 from a file channel.
     * <p>
     * Parses the fixed-size header (version, reference count, header size) and header messages,
     * including any continuation messages, from the specified file channel.
     * </p>
     *
     * @param fileChannel the seekable byte channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfObjectHeaderPrefixV1 instance
     * @throws IOException              if an I/O error occurs
     * @throws IllegalArgumentException if reserved fields are non-zero
     */

    public static HdfObjectHeaderPrefix readObjectHeaderPrefixFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfDataFile hdfDataFile,
            String objectName
    ) throws IOException {
        long offset = fileChannel.position();
        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN); // Buffer for the fixed-size header
        fileChannel.read(buffer);
        buffer.flip();

        // Parse Version (1 byte)
        int version = Byte.toUnsignedInt(buffer.get());
        if ( version == 1 ) {
            return readObjectHeader(fileChannel, offset, hdfDataFile);
        } else {
            buffer.rewind();
            byte[] signature = new byte[HdfObjectHeaderPrefixV2.OBJECT_HEADER_MESSAGE_SIGNATURE.length];
            buffer.get(signature);
            if (Arrays.compare(signature, HdfObjectHeaderPrefixV2.OBJECT_HEADER_MESSAGE_SIGNATURE) != 0) {
                throw new IllegalStateException("Object header signature mismatch");
            }
            return readObjectHeader(fileChannel, offset, hdfDataFile);
        }
    }

    protected static HdfObjectHeaderPrefixV1 readObjectHeader(
            SeekableByteChannel fileChannel,
            long objectHeaderOffset,
            HdfDataFile hdfDataFile
    ) throws IOException {
        fileChannel.position(objectHeaderOffset);
        ByteBuffer buffer = ByteBuffer.allocate(OBJECT_HEADER_PREFIX_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN); // Buffer for the fixed-size header
        fileChannel.read(buffer);
        buffer.flip();

        // Parse Version (1 byte)
        int version = Byte.toUnsignedInt(buffer.get());

        // Reserved (1 byte, should be zero)
        buffer.position(buffer.position() + OBJECT_HEADER_PREFIX_RESERVED_SIZE_1);

        // Total Number of Header Messages (2 bytes, little-endian)
        int totalHeaderMessages = Short.toUnsignedInt(buffer.getShort());

        // Object Reference Count (4 bytes, little-endian)
        long objectReferenceCount = Integer.toUnsignedLong(buffer.getInt());

        // Object Header Size (4 bytes, little-endian)
        long objectHeaderSize = Integer.toUnsignedLong(buffer.getInt());

        // Reserved (4 bytes, should be zero)
        buffer.position(buffer.position() + OBJECT_HEADER_PREFIX_RESERVED_SIZE_2);

        List<HdfMessage> dataObjectHeaderMessages = new ArrayList<>(
                HdfMessage.readMessagesFromByteBuffer(fileChannel, objectHeaderSize, hdfDataFile, HdfMessage.V1_OBJECT_HEADER_READ_PREFIX)
        );

        for (HdfMessage hdfMessage : dataObjectHeaderMessages) {
            if (hdfMessage instanceof ObjectHeaderContinuationMessage objectHeaderContinuationMessage) {
                dataObjectHeaderMessages.addAll(HdfMessage.parseContinuationMessage(fileChannel, objectHeaderContinuationMessage, hdfDataFile, HdfMessage.V1_OBJECT_HEADER_READ_PREFIX));

                break;
            }
        }

        // Create the instance
        return new HdfObjectHeaderPrefixV1(version, objectReferenceCount, objectHeaderSize, dataObjectHeaderMessages,
                hdfDataFile,
                HdfWriteUtils.hdfFixedPointFromValue(objectHeaderOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset()));
    }

    static HdfObjectHeaderPrefixV2 readV2ObjectHeader(SeekableByteChannel fileChannel, long objectHeaderAddress, HdfDataFile hdfDataFile) throws IOException {
        fileChannel.position(objectHeaderAddress);

        // --- 1. Read Signature, Version, and Flags ---
        // The first part of the header is 6 bytes: Signature (4) + Version (1) + Flags (1)
        ByteBuffer headerStartBuffer = ByteBuffer.allocate(6).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(headerStartBuffer);
        headerStartBuffer.flip();

        // Verify Signature ("OHDR")
        byte[] signatureBytes = new byte[4];
        headerStartBuffer.get(signatureBytes);
        String signature = new String(signatureBytes, "ASCII");
        if (!"OHDR".equals(signature)) {
            throw new IOException("Invalid HDF5 Object Header V2 signature. Expected 'OHDR', found '" + signature + "' at offset " + objectHeaderAddress);
        }

        // Parse Version (must be 2)
        int version = Byte.toUnsignedInt(headerStartBuffer.get());
        if (version != 2) {
            throw new IOException("Unsupported Object Header version. Expected 2, found " + version);
        }

        // Parse Flags (1 byte)
        byte flags = headerStartBuffer.get();
        boolean timesPresent = (flags & 0b00100000) != 0;
        boolean attrPhaseChangePresent = (flags & 0b00010000) != 0;

        // --- 2. Read Optional Fields based on Flags ---
        Instant accessTime = null, modificationTime = null, changeTime = null, birthTime = null;
        if (timesPresent) {
            ByteBuffer timeBuffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN); // 4 * 4-byte timestamps
            fileChannel.read(timeBuffer);
            timeBuffer.flip();
            accessTime = Instant.ofEpochSecond(Integer.toUnsignedLong(timeBuffer.getInt()));
            modificationTime = Instant.ofEpochSecond(Integer.toUnsignedLong(timeBuffer.getInt()));
            changeTime = Instant.ofEpochSecond(Integer.toUnsignedLong(timeBuffer.getInt()));
            birthTime = Instant.ofEpochSecond(Integer.toUnsignedLong(timeBuffer.getInt()));
        }

        Integer maxCompactAttributes = null, minDenseAttributes = null;
        if (attrPhaseChangePresent) {
            ByteBuffer attrPhaseBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN); // 2 * 2-byte values
            fileChannel.read(attrPhaseBuffer);
            attrPhaseBuffer.flip();
            maxCompactAttributes = Short.toUnsignedInt(attrPhaseBuffer.getShort());
            minDenseAttributes = Short.toUnsignedInt(attrPhaseBuffer.getShort());
        }

        // --- 3. Read Size of Chunk #0 (Variable Size) --- // JAVA 17 OPTIMIZED
        int chunkSizeLength = 1 << (flags & 0b00000011); // 1, 2, 4, or 8 bytes
        ByteBuffer chunkSizeBytesBuffer = ByteBuffer.allocate(chunkSizeLength).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(chunkSizeBytesBuffer);
        chunkSizeBytesBuffer.flip();

        long sizeOfChunk0 = switch (chunkSizeLength) {
            case 1 -> Byte.toUnsignedInt(chunkSizeBytesBuffer.get());
            case 2 -> Short.toUnsignedInt(chunkSizeBytesBuffer.getShort());
            case 4 -> Integer.toUnsignedLong(chunkSizeBytesBuffer.getInt());
            case 8 -> chunkSizeBytesBuffer.getLong();
            default -> throw new IOException("Invalid chunk size length: " + chunkSizeLength);
        };


        // --- 4. Read Header Messages ---
        // The messages are located in a block of 'sizeOfChunk0' bytes, followed by a 4-byte checksum.
        long prefixSize = fileChannel.position() - objectHeaderAddress;
        long messagesEndPosition = fileChannel.position() + sizeOfChunk0;

        List<HdfMessage> dataObjectHeaderMessages = new ArrayList<>(
                HdfMessage.readMessagesFromByteBuffer(fileChannel, sizeOfChunk0, hdfDataFile,
                        (flags & 0b00000100) > 0 ? HdfMessage.V2OBJECT_HEADER_READ_PREFIX_WITHORDER : HdfMessage.V2_OBJECT_HEADER_READ_PREFIX
                )
        );


        // After reading messages, the channel position might be before messagesEndPosition if a gap exists.
        // We must skip the gap to read the checksum.
        fileChannel.position(messagesEndPosition);

        // --- 5. Read Checksum ---
        ByteBuffer checksumBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.read(checksumBuffer);
        checksumBuffer.flip();
        int checksum = checksumBuffer.getInt();
        // Here you would typically verify the checksum against the header chunk data.

        // --- 6. Handle Continuation Messages ---
        // This logic is similar to V1, but you need to check the newly read messages.
        for (HdfMessage hdfMessage : dataObjectHeaderMessages) {
            if (hdfMessage instanceof ObjectHeaderContinuationMessage objectHeaderContinuationMessage) {
                // The continuation message points to the next chunk. You need a method to parse these.
                // A continuation chunk is NOT a full V2 header, it's just more messages.
                dataObjectHeaderMessages.addAll(HdfMessage.parseContinuationMessage(fileChannel, objectHeaderContinuationMessage, hdfDataFile,
                                (flags & 0b00000100) == 1 ? HdfMessage.V2OBJECT_HEADER_READ_PREFIX_WITHORDER : HdfMessage.V2_OBJECT_HEADER_READ_PREFIX
                        )
                );
                break; // Typically only one continuation message per chunk
            }
        }

        // --- 7. Create the V2 Header Prefix Instance ---
        return new HdfObjectHeaderPrefixV2(flags, sizeOfChunk0, checksum,
                accessTime, modificationTime, changeTime, birthTime,
                maxCompactAttributes, minDenseAttributes,
                dataObjectHeaderMessages, hdfDataFile, objectHeaderAddress, prefixSize);
    }



    /**
     * Initializes the global heap at the specified offset.
     * <p>
     * Reads the global heap data from the file channel starting at the given offset
     * and configures the global heap instance.
     * </p>
     *
     * @param offset the file offset where the global heap data begins
     */
    private void initializeGlobalHeap(HdfFixedPoint offset) {
        try {
            fileChannel.position(offset.getInstance(Long.class));
            globalHeap.initializeFromSeekableByteChannel(fileChannel, this);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//    /**
//     * Collects a map of dataset names to their information from the B-tree and local heap.
//     *
//     * @param fileChannel the seekable byte channel for reading the file
//     * @param bTree       the B-tree containing symbol table entries
//     * @param localHeap   the local heap storing link names
//     * @return a map of dataset names to their {@link HdfGroup.DataSetInfo}
//     * @throws IOException if an I/O error occurs
//     */
//    private Map<String, HdfGroup.DataSetInfo> collectDatasetsMap(SeekableByteChannel fileChannel, HdfBTree bTree, HdfLocalHeap localHeap) throws IOException {
//        Map<String, HdfGroup.DataSetInfo> dataSets = new LinkedHashMap<>();
//        collectDatasetsRecursive(bTree, dataSets, localHeap, fileChannel);
//        return dataSets;
//    }
//
//    /**
//     * Recursively collects dataset information from the B-tree.
//     * <p>
//     * Traverses the B-tree, processing leaf nodes to extract dataset metadata and
//     * recursively handling internal nodes to collect all datasets.
//     * </p>
//     *
//     * @param currentNode the current B-tree node
//     * @param dataSets    the map to store dataset information
//     * @param localHeap   the local heap for link names
//     * @param fileChannel the seekable byte channel for reading
//     * @throws IOException if an I/O error occurs
//     */
//    private void collectDatasetsRecursive(HdfBTree currentNode,
//                                          Map<String, HdfGroup.DataSetInfo> dataSets,
//                                          HdfLocalHeap localHeap,
//                                          SeekableByteChannel fileChannel) throws IOException {
//        for (HdfGroupBTreeEntry entry : currentNode.getEntries()) {
//            if (entry.isLeafEntry()) {
//                HdfGroupSymbolTableNode snod = entry.getSymbolTableNode();
//                for (HdfSymbolTableEntry ste : snod.getSymbolTableEntries()) {
//                    HdfString linkName = localHeap.parseStringAtOffset(ste.getLinkNameOffset());
//                    long dataObjectHeaderAddress = ste.getObjectHeaderOffset().getInstance(Long.class);
//                    long linkNameOffset = ste.getLinkNameOffset().getInstance(Long.class);
//                    fileChannel.position(dataObjectHeaderAddress);
//                    HdfObjectHeaderPrefixV1 header = HdfObjectHeaderPrefixV1.readFromSeekableByteChannel(fileChannel, this);
//                    DatatypeMessage dataType = header.findMessageByType(DatatypeMessage.class).orElseThrow();
//                    HdfDataset dataset = new HdfDataset(this, linkName.toString(), dataType.getHdfDatatype(), header);
//                    HdfGroup.DataSetInfo dataSetInfo = new HdfGroup.DataSetInfo(dataset,
//                            HdfWriteUtils.hdfFixedPointFromValue(0, superblock.getFixedPointDatatypeForOffset()),
//                            linkNameOffset);
//                    dataSets.put(linkName.toString(), dataSetInfo);
//                }
//            } else if (entry.isInternalEntry()) {
//                HdfBTree childBTree = entry.getChildBTree();
//                collectDatasetsRecursive(childBTree, dataSets, localHeap, fileChannel);
//            }
//        }
//    }

    /**
     * Retrieves the global heap of the HDF5 file.
     *
     * @return the {@link HdfGlobalHeap} instance
     */
    @Override
    public HdfGlobalHeap getGlobalHeap() {
        return globalHeap;
    }

    /**
     * Retrieves the file allocation manager of the HDF5 file.
     *
     * @return the {@link HdfFileAllocation} instance
     */
//        @Override
//        public HdfFileAllocation getFileAllocation() {
//            return fileAllocation;
//        }

    /**
     * Retrieves the seekable byte channel for reading the HDF5 file.
     *
     * @return the {@link SeekableByteChannel} instance
     * @throws UnsupportedOperationException as this operation is not yet supported
     */
    @Override
    public SeekableByteChannel getSeekableByteChannel() {
        return fileChannel;
    }

    @Override
    public HdfSuperblock getSuperblock() {
        return superblock;
    }

    @Override
    public HdfBTree getBTree() {
        return bTree;
    }

    @Override
    public void setFileAllocation(HdfFileAllocation hdfFileAllocation) {
        this.fileAllocation = hdfFileAllocation;
    }

}