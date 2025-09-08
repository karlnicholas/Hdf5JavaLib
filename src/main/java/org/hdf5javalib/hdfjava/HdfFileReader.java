package org.hdf5javalib.hdfjava;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datatype.FixedPointDatatype;
import org.hdf5javalib.hdffile.dataobjects.HdfObjectHeaderPrefix;
import org.hdf5javalib.hdffile.dataobjects.HdfObjectHeaderPrefixV1;
import org.hdf5javalib.hdffile.dataobjects.HdfObjectHeaderPrefixV2;
import org.hdf5javalib.hdffile.dataobjects.messages.HdfMessage;
import org.hdf5javalib.hdffile.dataobjects.messages.LinkInfoMessage;
import org.hdf5javalib.hdffile.dataobjects.messages.ObjectHeaderContinuationMessage;
import org.hdf5javalib.hdffile.infrastructure.*;
import org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini.FractalHeap;
import org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini.FractalHeapReader;
import org.hdf5javalib.hdffile.infrastructure.v2btree.gemini.BTreeV2Header;
import org.hdf5javalib.hdffile.infrastructure.v2btree.gemini.BTreeV2Reader;
import org.hdf5javalib.hdffile.infrastructure.v2btree.gemini.BTreeV2Record;
import org.hdf5javalib.hdffile.infrastructure.v2btree.gemini.Type5Record;
import org.hdf5javalib.hdffile.infrastructure.v2btree.grok.V2BTreeReader;
import org.hdf5javalib.hdffile.metadata.HdfSuperblock;
import org.hdf5javalib.utils.HdfReadUtils;
import org.hdf5javalib.utils.HdfWriteUtils;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.hdf5javalib.datatype.FixedPointDatatype.BIT_MULTIPLIER;
import static org.hdf5javalib.hdffile.dataobjects.HdfObjectHeaderPrefixV1.*;
import static org.hdf5javalib.hdffile.infrastructure.HdfGroupSymbolTableNode.GROUP_SYMBOL_TABLE_NODE_SIGNATURE;
import static org.hdf5javalib.hdffile.infrastructure.HdfLocalHeap.*;
import static org.hdf5javalib.hdffile.infrastructure.HdfSymbolTableEntry.RESERVED_FIELD_1_SIZE;
import static org.hdf5javalib.hdffile.infrastructure.HdfSymbolTableEntryCacheNoScratch.SYMBOL_TABLE_ENTRY_SCRATCH_SIZE;
import static org.hdf5javalib.hdffile.metadata.HdfSuperblock.*;

/**
 * Reads and parses HDF5 file structures.
 * <p>
 * The {@code HdfFileReader} class implements {@link org.hdf5javalib.hdfjava.HdfDataFile} to provide functionality
 * for reading an HDF5 file from a {@link SeekableByteChannel}. It initializes the superblock,
 * root group, global heap, and file allocation, and constructs a hierarchy of groups and
 * datasets by parsing the file's metadata and data structures.
 * </p>
 */
public class HdfFileReader implements HdfDataFile {
//    /** The superblock containing metadata about the HDF5 file. */
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
    private HdfSuperblock superblock;
    private HdfTree bTree;

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
        long objectHeaderAddress;
        if ( superblock.getVersion() < 2 ) {
            HdfSymbolTableEntry rootGroupSymbolTableEntry = readSteFromSeekableByteChannel(fileChannel, this);
            // determine version of data object headers
            objectHeaderAddress = rootGroupSymbolTableEntry.getObjectHeaderAddress().getInstance(Long.class);

        } else {
            objectHeaderAddress = superblock.getRootGroupObjectHeaderAddresss().getInstance(Long.class);
        }
        if ( fileChannel.size() <= objectHeaderAddress ) {
            throw new UnsupportedOperationException("File only contains a superblock.");
        }
        // The first part of the header is 6 bytes: Signature (4) + Version (1) + Flags (1)
        ByteBuffer headerStartBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.position(objectHeaderAddress);
        fileChannel.read(headerStartBuffer);
        headerStartBuffer.flip();
        HdfObjectHeaderPrefix objectHeader;
        if (Arrays.equals(headerStartBuffer.array(), HdfObjectHeaderPrefixV2.OBJECT_HEADER_MESSAGE_SIGNATURE)) {
            objectHeader = readV2ObjectHeader(fileChannel, objectHeaderAddress, this);
            readV2Arch(fileChannel, this, objectHeader);
        } else {
            int version = Byte.toUnsignedInt(headerStartBuffer.get());
            if( version > 1 ) {
                throw new UnsupportedOperationException("V2 architecture not supported. Header Object version: " + version);
            }
            objectHeader = readObjectHeader(fileChannel, objectHeaderAddress, this);
        }
//        long heapOffset = ((HdfSymbolTableEntryCacheWithScratch)rootGroupSymbolTableEntry.getCache()).getLocalHeapAddress().getInstance(Long.class);
//        long bTreeAddress = ((HdfSymbolTableEntryCacheWithScratch)rootGroupSymbolTableEntry.getCache()).getbTreeAddress().getInstance(Long.class);
//        HdfLocalHeap localHeap = readLocalHeapFromSeekableByteChannel(fileChannel, heapOffset, this);
//        HdfBTreeV1 groupBTree = readBTreeFromSeekableByteChannel(fileChannel, bTreeAddress, this);
//        String groupName = localHeap.stringAtOffset(rootGroupSymbolTableEntry.getLinkNameOffset());
//        // set BTree
//        HdfGroup groupObject = new HdfGroup(groupName, objectHeader, null, null);
//        bTree = new HdfTree(groupObject);
//        // recurse through infrastructure
//        readInfrastructure(groupObject, localHeap, groupBTree);

        return this;
    }

    private static void readHeapHeader(SeekableByteChannel fileChannel, HdfFileReader hdfFileReader) {

    }

    private static void readV2Arch(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile, HdfObjectHeaderPrefix objectHeader) throws Exception {
//        HdfFixedPoint fractalHeapAddress = objectHeader.findMessageByType(LinkInfoMessage.class).orElseThrow().getFractalHeapAddress();
//        long fractalHeapOffset = fractalHeapAddress.getInstance(Long.class);
//        fileChannel.position(fractalHeapOffset);

        HdfFixedPoint v2BTreeNameIndexAddress = objectHeader.findMessageByType(LinkInfoMessage.class).orElseThrow().getV2BTreeNameIndexAddress();
        long v2BTreeNameIndexOffset = v2BTreeNameIndexAddress.getInstance(Long.class);
        fileChannel.position(v2BTreeNameIndexOffset);

        System.out.println("Starting B-tree v2 Read...");
        BTreeV2Reader bTreeV2Reader = new BTreeV2Reader(fileChannel, 8, 8);
        BTreeV2Header bTreeV2Header = bTreeV2Reader.getHeader();

        System.out.println("Header parsed successfully:");
        System.out.println("  Type: " + bTreeV2Header.type);
        System.out.println("  Depth: " + bTreeV2Header.depth);
        System.out.println("  Total Records: " + bTreeV2Header.totalNumberOfRecordsInBTree);
        System.out.println("  Root Node Address: " + bTreeV2Header.rootNodeAddress);

        List<BTreeV2Record> allRecords = bTreeV2Reader.getAllRecords();

        System.out.println("\nTraversal complete. Found " + allRecords.size() + " records:");

        HdfFixedPoint fractalHeapAddress = objectHeader.findMessageByType(LinkInfoMessage.class).orElseThrow().getFractalHeapAddress();
        long fractalHeapOffset = fractalHeapAddress.getInstance(Long.class);
        fileChannel.position(fractalHeapOffset);

        // 6. --- EXECUTE THE READER ---
        FractalHeapReader reader = new FractalHeapReader(fileChannel, 8, 8);
        FractalHeap heap = reader.read();

        // 7. --- RETRIEVE THE OBJECT AND VERIFY ---
        for (int i = 0; i < allRecords.size(); i++) {
            BTreeV2Record x = allRecords.get(i);

            byte[] heapId = ((Type5Record)x).heapId;

//            byte[] retrievedData = heap.getObject(heapId);
//            System.out.println("Row: " + Arrays.toString(Arrays.copyOfRange(retrievedData, 0, 10))
//            + " : " + retrievedData[10]
//            + " : " + new String(Arrays.copyOfRange(retrievedData, 11, 11+retrievedData[10]))
//            + " : " + Arrays.toString(Arrays.copyOfRange(retrievedData, 10+1+retrievedData[10], retrievedData.length))
//            );
            ByteBuffer retrievedData = ByteBuffer.wrap(heap.getObject(heapId)).order(ByteOrder.LITTLE_ENDIAN);
            byte[] rowHeader = new byte[10];
            retrievedData.get(rowHeader);
            int sLength = Byte.toUnsignedInt(retrievedData.get());
            byte[] stringBuffer = new byte[sLength];
            retrievedData.get(stringBuffer);
            String groupName = new String(stringBuffer);
            long objectHeaderOffset = retrievedData.getLong();
            System.out.println("Row: " + Arrays.toString(rowHeader)
            + " : " + sLength
            + " : " + groupName
            + " : " + objectHeaderOffset
            );

            //            System.out.println("Heap ID: " + Arrays.toString(heapId));
//            System.out.println("Retrieved Data: '" + new String(retrievedData, StandardCharsets.UTF_8) + "'");
            HdfObjectHeaderPrefix objectHeaderG1 = readObjectHeaderPrefixFromSeekableByteChannel(fileChannel, objectHeaderOffset, hdfDataFile, groupName);
            System.out.println(groupName + ":" + objectHeaderG1.getHeaderMessages());
        }
//        FractalHeapReader fractalHeapReader = new FractalHeapReader(fileChannel, 8, 8);
//        FractalHeap fractalHeap = fractalHeapReader.read();
//        System.out.println("fractalHeap: " + fractalHeap);
//        Object rb = fractalHeap.rootBlock;
//        System.out.println("fractalHeap: " + rb);

//        FractalHeapReader reader = new FractalHeapReader(fileChannel, 8, 8);
//        FractalHeapReader.FractalHeapHeader header = reader.readHeader();
//        System.out.println("Table Width: " + header.tableWidth);
//        System.out.println("Root Block Address: " + header.addressOfRootBlock);
//        BTreeV2Record x = allRecords.get(0);
//        // Example: Retrieve object data with a heap ID
////        byte[] heapId = {/* heap ID bytes */};
//        byte[] heapId = ((Type5Record)x).heapId;
//        byte[] objectData = reader.getObjectData(heapId);
//        System.out.println("Object Data Length: " + objectData.length);

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
            if (!Arrays.equals(signature, FILE_SIGNATURE)) {
                offset += 500;
                continue;
            }

            // Read version
//            int version = Byte.toUnsignedInt(buffer.get());
//            if ( version > 1) {
//                throw new UnsupportedOperationException("V2 architecture not supported. Superblock version: " + version);
//            }
            return offset;
        }
        throw new IllegalArgumentException("HDF file signature not found");
    }

    private void readInfrastructure(HdfGroup parentGroup, HdfLocalHeap localHeap, HdfBTreeV1 groupBTree) throws Exception {
        if (groupBTree.getNodeLevel() > 0) {
            // Internal node: recurse on child B-Trees
            for (HdfBTreeEntryBase entry : groupBTree.getEntries()) {
                HdfBTreeV1 childBTree = entry.getChildBTree();
                if (childBTree != null) {
                    readInfrastructure(parentGroup, localHeap, childBTree);
                }
            }
        } else {
            // Leaf node: process SNODs
            for (HdfBTreeEntryBase entry : groupBTree.getEntries()) {
                HdfGroupSymbolTableNode groupSymbolTableNode = ((HdfGroupBTreeEntry) entry).getGroupSymbolTableNode();
                if (groupSymbolTableNode != null) {
                    for (HdfSymbolTableEntry symbolTableEntry : groupSymbolTableNode.getSymbolTableEntries()) {
                        String objectName = localHeap.stringAtOffset(symbolTableEntry.getLinkNameOffset());
                        Long objectHeaderAddress = symbolTableEntry.getObjectHeaderAddress().getInstance(Long.class);
                        String hardLink = isHardLink(parentGroup, objectHeaderAddress);
                        HdfObjectHeaderPrefixV1 objectHeader = null;
                        if ( hardLink == null ) {
                            objectHeader = readObjectHeader(fileChannel, objectHeaderAddress, this);
                        }

                        switch (symbolTableEntry.getCache().getCacheType()) {
                            case 0:
                                HdfDataset datasetObject = new HdfDataset(objectName, objectHeader, parentGroup, hardLink);
                                parentGroup.addChild(datasetObject);
                                break;
                            case 1:
                                HdfGroup groupObject = new HdfGroup(objectName, objectHeader, parentGroup, hardLink);
                                parentGroup.addChild(groupObject);

                                long newHeapOffset = ((HdfSymbolTableEntryCacheWithScratch) symbolTableEntry.getCache()).getLocalHeapAddress().getInstance(Long.class);
                                long newBTreeAddress = ((HdfSymbolTableEntryCacheWithScratch) symbolTableEntry.getCache()).getbTreeAddress().getInstance(Long.class);
                                HdfLocalHeap newLocalHeap = readLocalHeapFromSeekableByteChannel(fileChannel, newHeapOffset, this);
                                HdfBTreeV1 newGroupBTree = readBTreeFromSeekableByteChannel(fileChannel, newBTreeAddress, this);
                                if ( hardLink == null ) {
                                    readInfrastructure(groupObject, newLocalHeap, newGroupBTree);
                                }
                                break;
                            default:
                                throw new UnsupportedOperationException("Unknown type: " + symbolTableEntry.getCache().getCacheType());
                        }
                    }
                }
            }
        }
    }

    private String isHardLink(HdfGroup parentGroup, Long objectHeaderAddress) {
        AtomicReference<String> result = new AtomicReference(null);
        HdfGroup rootGroup = (HdfGroup) parentGroup.getRoot();
        rootGroup.visitAllNodes(hdfBTreeNode -> {
            try {
                if ( hdfBTreeNode.getObjectHeader() != null ) {
                    Long testObjectHeaderOffset = hdfBTreeNode.getDataObject().getObjectHeader().getOffset().getInstance(Long.class);
                    if (Objects.equals(testObjectHeaderOffset, objectHeaderAddress)) {
                        result.set(hdfBTreeNode.getObjectPath());
                        return true;
                    }
                }
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException | IOException e) {
                throw new RuntimeException(e);
            }
            return false;
        });
        return result.get();
    }



    /**
     * Retrieves all datasets in the group.
     *
     * @return a collection of
     */
    public Iterator<HdfDataset> datasetIterator() {
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
            Optional<HdfTreeNode> child = currentGroup.findChildByName(components[compIndex++]);
            if ( child.isEmpty() ) {
                return Optional.empty();
            } else if (compIndex == components.length && clazz.isAssignableFrom(child.get().getDataObject().getClass())) {
                return child.map(clazz::cast);
            } else if ( compIndex < components.length && HdfGroup.class.isAssignableFrom(child.get().getDataObject().getClass()) ) {
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
        } else if (version == 2) {
            superblockSize = SUPERBLOCK_SIZE_V2; // Version 1 superblock size (example value, adjust per spec)
        } else if (version == 3) {
            superblockSize = SUPERBLOCK_SIZE_V3; // Version 1 superblock size (example value, adjust per spec)
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
        int freeSpaceVersion = -1;
        int rootGroupVersion = -1;
        int sharedHeaderVersion = -1;
        int offsetSize;
        int lengthSize;

        int groupLeafNodeK = -1;
        int groupInternalNodeK = -1;

        FixedPointDatatype fixedPointDatatypeForOffset;
        FixedPointDatatype fixedPointDatatypeForLength;

        // Parse addresses using HdfFixedPoint
        HdfFixedPoint baseAddress = null;
        HdfFixedPoint freeSpaceAddress = null;
        HdfFixedPoint extensionAddress = null;
        HdfFixedPoint endOfFileAddress = null;
        HdfFixedPoint driverInformationAddress = null;
        HdfFixedPoint rootObjectHeaderAddress = null;
        if ( version < 2) {
            // Step 4: Parse the remaining superblock fields
            buffer.position(SIGNATURE_SIZE + VERSION_SIZE); // Skip the file signature
            freeSpaceVersion = Byte.toUnsignedInt(buffer.get());
            rootGroupVersion = Byte.toUnsignedInt(buffer.get());
            buffer.get(); // Skip reserved
            sharedHeaderVersion = Byte.toUnsignedInt(buffer.get());
            offsetSize = Byte.toUnsignedInt(buffer.get());
            lengthSize = Byte.toUnsignedInt(buffer.get());
            buffer.get(); // Skip reserved

            fixedPointDatatypeForOffset = new FixedPointDatatype(
                    FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField(false, false, false, false),
                    offsetSize, (short) 0, (short) (BIT_MULTIPLIER * offsetSize), hdfDataFile);
            fixedPointDatatypeForLength = new FixedPointDatatype(
                    FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField(false, false, false, false),
                    lengthSize, (short) 0, (short) (BIT_MULTIPLIER * lengthSize), hdfDataFile);

            groupLeafNodeK = Short.toUnsignedInt(buffer.getShort());
            groupInternalNodeK = Short.toUnsignedInt(buffer.getShort());
            buffer.getInt(); // Skip consistency flags

            // Parse addresses using HdfFixedPoint
            baseAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
            freeSpaceAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
            endOfFileAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
            driverInformationAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
        } else {
            // Step 4: Parse the remaining superblock fields
            buffer.position(SIGNATURE_SIZE + VERSION_SIZE); // Skip the file signature
            offsetSize = Byte.toUnsignedInt(buffer.get());
            lengthSize = Byte.toUnsignedInt(buffer.get());
            int fileConsistencyFlags = Byte.toUnsignedInt(buffer.get()); // Skip reserved

            fixedPointDatatypeForOffset = new FixedPointDatatype(
                    FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField(false, false, false, false),
                    offsetSize, (short) 0, (short) (BIT_MULTIPLIER * offsetSize), hdfDataFile);
            fixedPointDatatypeForLength = new FixedPointDatatype(
                    FixedPointDatatype.createClassAndVersion(),
                    FixedPointDatatype.createClassBitField(false, false, false, false),
                    lengthSize, (short) 0, (short) (BIT_MULTIPLIER * lengthSize), hdfDataFile);

//            groupLeafNodeK = Short.toUnsignedInt(buffer.getShort());
//            groupInternalNodeK = Short.toUnsignedInt(buffer.getShort());
//            buffer.getInt(); // Skip consistency flags

            // Parse addresses using HdfFixedPoint
            baseAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
            extensionAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
            endOfFileAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
            rootObjectHeaderAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
            Long checkSum = Integer.toUnsignedLong(buffer.getInt());


        }

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
                extensionAddress,
                endOfFileAddress,
                driverInformationAddress,
                rootObjectHeaderAddress,
                hdfDataFile,
                fixedPointDatatypeForOffset,
                fixedPointDatatypeForLength
        );
        return superblock;
    }

    /**
     * Reads an HdfTree from a file channel.
     *
     * @param fileChannel the file channel to read from
     * @param hdfDataFile the HDF5 file context
     * @return the constructed HdfTree instance
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
     * Recursively reads an HdfTree from a file channel, handling cycles.
     *
     * @param fileChannel  the file channel to read from
     * @param nodeAddress  the address of the current node
     * @param visitedNodes a map of visited node addresses to detect cycles
     * @param hdfDataFile  the HDF5 file context
     * @return the constructed HdfTree instance
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

        HdfBTreeV1 currentNode = new HdfBTreeV1(nodeType, nodeLevel, entriesUsed, leftSiblingAddress, rightSiblingAddress, keyZero, entries, hdfDataFile);
        visitedNodes.put(nodeAddress, currentNode);

        for (int i = 0; i < entriesUsed; i++) {
            HdfFixedPoint childPointer = HdfReadUtils.readHdfFixedPointFromBuffer(hdfOffset, entriesBuffer);
            HdfFixedPoint key = HdfReadUtils.readHdfFixedPointFromBuffer(hdfLength, entriesBuffer);
            long filePosAfterEntriesBlock = fileChannel.position();
            long childAddress = childPointer.getInstance(Long.class);
            fileChannel.position(childAddress);

            HdfGroupBTreeEntry entry;
            if (nodeLevel == 1) {
                // It's a sub B-Tree
                HdfBTreeV1 child = readFromSeekableByteChannelRecursive(fileChannel, childAddress, hdfDataFile, visitedNodes);
                entry = new HdfGroupBTreeEntry(key, childPointer, child, null); // Assuming entry constructor accepts Object for last param
            } else {
                // It's a SNOD
                HdfGroupSymbolTableNode child = readSnodFromSeekableByteChannel(fileChannel, hdfDataFile);
                entry = new HdfGroupBTreeEntry(key, childPointer, null, child); // Assuming entry constructor accepts Object for last param
            }
            fileChannel.position(filePosAfterEntriesBlock);
            entries.add(entry);
        }
        return currentNode;
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
    ) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {

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

        return new HdfLocalHeap(version,hdfDataFile, hdfLocalHeapData);
    }

    public static HdfLocalHeapData readLocalHeapDataFromSeekableByteChannel(
            SeekableByteChannel fileChannel,
            HdfFixedPoint dataSegmentSize,
            HdfFixedPoint freeListOffset,
            HdfFixedPoint dataSegmentAddress,
            HdfDataFile hdfDataFile
    ) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {

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
            long objectHeaderAddress,
            HdfDataFile hdfDataFile,
            String objectName
    ) throws Exception {
//        long offset = fileChannel.position();
//        ByteBuffer buffer = ByteBuffer.allocate(16).order(ByteOrder.LITTLE_ENDIAN); // Buffer for the fixed-size header
//        fileChannel.read(buffer);
//        buffer.flip();

        // The first part of the header is 6 bytes: Signature (4) + Version (1) + Flags (1)
        ByteBuffer headerStartBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
        fileChannel.position(objectHeaderAddress);
        fileChannel.read(headerStartBuffer);
        headerStartBuffer.flip();
        HdfObjectHeaderPrefix objectHeader;
        if (Arrays.equals(headerStartBuffer.array(), HdfObjectHeaderPrefixV2.OBJECT_HEADER_MESSAGE_SIGNATURE)) {
            objectHeader = readV2ObjectHeader(fileChannel, objectHeaderAddress, hdfDataFile);
//            readV2Arch(fileChannel, hdfDataFile, objectHeader);
        } else {
            int version = Byte.toUnsignedInt(headerStartBuffer.get());
            if( version > 1 ) {
                throw new UnsupportedOperationException("V2 architecture not supported. Header Object version: " + version);
            }
            objectHeader = readObjectHeader(fileChannel, objectHeaderAddress, hdfDataFile);
        }

//        // Parse Version (1 byte)
//        int version = Byte.toUnsignedInt(buffer.get());
//        if ( version == 1 ) {
//            return readObjectHeader(fileChannel, offset, hdfDataFile);
//        } else {
//            buffer.rewind();
//            byte[] signature = new byte[HdfObjectHeaderPrefixV2.OBJECT_HEADER_MESSAGE_SIGNATURE.length];
//            buffer.get(signature);
//            if (Arrays.compare(signature, HdfObjectHeaderPrefixV2.OBJECT_HEADER_MESSAGE_SIGNATURE) != 0) {
//                throw new IllegalStateException("Object header signature mismatch");
//            }
//            return readObjectHeader(fileChannel, offset, hdfDataFile);
//        }
        return objectHeader;
    }

    protected static HdfObjectHeaderPrefixV1 readObjectHeader(
            SeekableByteChannel fileChannel,
            long objectHeaderOffset,
            HdfDataFile hdfDataFile
    ) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
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
        return new HdfObjectHeaderPrefixV1(
                version,
                objectReferenceCount,
                objectHeaderSize,
                dataObjectHeaderMessages,
                hdfDataFile,
                HdfWriteUtils.hdfFixedPointFromValue(objectHeaderOffset, hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset())
        );
    }

    static HdfObjectHeaderPrefixV2 readV2ObjectHeader(SeekableByteChannel fileChannel, long objectHeaderAddress, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
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
        parseContinuationMessages(fileChannel, flags, dataObjectHeaderMessages, hdfDataFile);
        // --- 7. Create the V2 Header Prefix Instance ---
        return new HdfObjectHeaderPrefixV2(flags, sizeOfChunk0, checksum,
                accessTime, modificationTime, changeTime, birthTime,
                maxCompactAttributes, minDenseAttributes,
                dataObjectHeaderMessages, hdfDataFile, objectHeaderAddress, prefixSize);
    }

    private static void parseContinuationMessages(SeekableByteChannel fileChannel, int flags, List<HdfMessage> currentMessages, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // This logic is similar to V1, but you need to check the newly read messages.
        for (HdfMessage hdfMessage : currentMessages) {
            if (hdfMessage instanceof ObjectHeaderContinuationMessage objectHeaderContinuationMessage) {
                // The continuation message points to the next chunk. You need a method to parse these.
                // A continuation chunk is NOT a full V2 header, it's just more messages.
                List<HdfMessage> newContinuationMessages = HdfMessage.parseContinuationMessage(fileChannel, objectHeaderContinuationMessage, hdfDataFile,
                        (flags & 0b00000100) > 0 ? HdfMessage.V2OBJECT_HEADER_READ_PREFIX_WITHORDER : HdfMessage.V2_OBJECT_HEADER_READ_PREFIX);
                parseContinuationMessages(fileChannel, flags, newContinuationMessages, hdfDataFile);
                currentMessages.addAll(newContinuationMessages);
                break; // Typically only one continuation message per chunk
            }
        }
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
    private void initializeGlobalHeap(HdfFixedPoint offset) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        fileChannel.position(offset.getInstance(Long.class));
        globalHeap.initializeFromSeekableByteChannel(fileChannel, this);
    }

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
    public HdfTree getBTree() {
        return bTree;
    }

}