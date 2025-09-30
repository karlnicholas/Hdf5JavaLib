package org.hdf5javalib.hdffile.metadata;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datatype.FixedPointDatatype;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfDisplayUtils;

import java.io.IOException;

/**
 * Represents the Superblock in the HDF5 file format.
 * <p>
 * The Superblock is the root metadata structure in an HDF5 file. It contains
 * essential information about the file layout, including pointers to key data
 * structures such as the root group, free space management, and driver
 * information. The Superblock is always located at a known file offset,
 * allowing quick access to file metadata.
 * </p>
 *
 * <h2>Structure</h2>
 * <p>The HDF5 Superblock consists of the following components:</p>
 * <ul>
 *   <li><b>Signature (8 bytes)</b>: Identifies the file as an HDF5 file
 *       (ASCII "˜HDF\r\n\032\n").</li>
 *   <li><b>Version (1 byte)</b>: Specifies the version of the superblock format.</li>
 *   <li><b>Offset Size (1 byte)</b>: Defines the size (in bytes) of file offsets.</li>
 *   <li><b>Length Size (1 byte)</b>: Defines the size (in bytes) of length fields.</li>
 *   <li><b>File Consistency Flags (1 byte, optional)</b>: Used in later versions
 *       for metadata consistency tracking.</li>
 *   <li><b>Base Address (offset)</b>: The file address of the base location
 *       for relative addressing.</li>
 *   <li><b>Free Space Address (offset)</b>: The file address of the free space
 *       manager, if present.</li>
 *   <li><b>End of File Address (offset)</b>: The address of the last allocated
 *       byte in the file.</li>
 *   <li><b>Driver Information Address (offset, optional)</b>: Location of the
 *       driver information block for specialized file drivers.</li>
 *   <li><b>Root Group Object Header Address (offset)</b>: The location of the
 *       root group's object header, which serves as the entry point to the file’s
 *       hierarchical structure.</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <p>The Superblock serves several critical functions:</p>
 * <ul>
 *   <li>Identifies an HDF5 file and its format version.</li>
 *   <li>Defines key parameters such as offset sizes and length sizes.</li>
 *   <li>Provides pointers to essential file structures, including the root group,
 *       free space manager, and driver information.</li>
 * </ul>
 *
 * <h2>Processing</h2>
 * <p>When reading an HDF5 file, the Superblock is the first structure examined to
 * determine the file format version, address size settings, and location of key
 * metadata structures. It is vital for properly interpreting the file’s contents.
 * </p>
 *
 * <p>This class provides methods to parse and interpret the HDF5 Superblock
 * based on the HDF5 file specification.</p>
 */
public class HdfSuperblock {
    public static final byte[] FILE_SIGNATURE = new byte[]{(byte) 0x89, 'H', 'D', 'F', '\r', '\n', 0x1A, '\n'};
    public static final int SIGNATURE_SIZE = 8;
    public static final int VERSION_SIZE = 1;
    public static final int SUPERBLOCK_SIZE_V1 = 56;
    public static final int SUPERBLOCK_SIZE_V2 = 96;
    public static final int SUPERBLOCK_SIZE_V3 = 48;

    private final int version;
    private final int freeSpaceVersion;
    private final int rootGroupVersion;
    private final int sharedHeaderVersion;
    private final int sizeOfOffsets;
    private final int sizeOfLengths;
    private final int groupLeafNodeK;
    private final int groupInternalNodeK;

    private final HdfFixedPoint baseAddress;
    private final HdfFixedPoint addressFileFreeSpaceInfo;
    private final HdfFixedPoint superblockExtensionAddress;
    private HdfFixedPoint endOfFileAddress;
    private final HdfFixedPoint rootGroupObjectHeaderAddress;
    private final HdfFixedPoint driverInformationAddress;

    // Working properties
    private final HdfDataFile hdfDataFile;
    private final FixedPointDatatype fixedPointDatatypeForOffset;
    private final FixedPointDatatype fixedPointDatatypeForLength;

    /**
     * Constructs a Superblock with the specified metadata.
     *
     * @param version                  the superblock version
     * @param freeSpaceVersion         the free space storage version
     * @param rootGroupVersion         the root group symbol table entry version
     * @param sharedHeaderVersion      the shared object header format version
     * @param sizeOfOffsets,           the number of bytes used to store addresses
     * @param sizeOfLengths,           he number of bytes used to store the size of an object.
     * @param groupLeafNodeK           the B-tree group leaf node K value
     * @param groupInternalNodeK       the B-tree group internal node K value
     * @param baseAddress              the base address for relative addressing
     * @param addressFileFreeSpaceInfo the address of the free space manager
     * @param endOfFileAddress         the end-of-file address
     * @param driverInformationAddress the address of the driver information block
     * @param hdfDataFile              the HDF5 file context
     */
    public HdfSuperblock(
            int version,
            int freeSpaceVersion,
            int rootGroupVersion,
            int sharedHeaderVersion,
            int sizeOfOffsets,
            int sizeOfLengths,
            int groupLeafNodeK,
            int groupInternalNodeK,
            HdfFixedPoint baseAddress,
            HdfFixedPoint addressFileFreeSpaceInfo,
            HdfFixedPoint superblockExtensionAddress,
            HdfFixedPoint endOfFileAddress,
            HdfFixedPoint driverInformationAddress,
            HdfFixedPoint rootGroupObjectHeaderAddress,
            HdfDataFile hdfDataFile,
            FixedPointDatatype fixedPointDatatypeForOffset,
            FixedPointDatatype fixedPointDatatypeForLength
    ) {
//        hdfDataFile.getFileAllocation().addAllocationBlock(this);
        this.version = version;
        this.freeSpaceVersion = freeSpaceVersion;
        this.rootGroupVersion = rootGroupVersion;
        this.sharedHeaderVersion = sharedHeaderVersion;
        this.sizeOfOffsets = sizeOfOffsets;
        this.sizeOfLengths = sizeOfLengths;
        this.groupLeafNodeK = groupLeafNodeK;
        this.groupInternalNodeK = groupInternalNodeK;
        this.baseAddress = baseAddress;
        this.addressFileFreeSpaceInfo = addressFileFreeSpaceInfo;
        this.superblockExtensionAddress = superblockExtensionAddress;
        this.endOfFileAddress = endOfFileAddress;
        this.rootGroupObjectHeaderAddress = rootGroupObjectHeaderAddress;
        this.driverInformationAddress = driverInformationAddress;
        this.hdfDataFile = hdfDataFile;
        this.fixedPointDatatypeForOffset = fixedPointDatatypeForOffset;
        this.fixedPointDatatypeForLength = fixedPointDatatypeForLength;
    }

    /**
     * Writes the Superblock to a file channel.
     * <p>
     * Serializes the superblock metadata, including the file signature, version, offset and length
     * sizes, B-tree settings, address fields, and root group symbol table entry, to the specified
     * file channel at the superblock's allocated offset.
     * </p>
     *
     * @param fileChannel the seekable byte channel to write to
     * @throws IOException if an I/O error occurs
     */
//    public void writeToFileChannel(SeekableByteChannel fileChannel) throws IOException {
//        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
//        ByteBuffer buffer = ByteBuffer.allocate((int) HdfFileAllocation.SUPERBLOCK_SIZE).order(ByteOrder.LITTLE_ENDIAN);
//
//        // Step 1: Write the HDF5 file signature (8 bytes)
//        buffer.put(FILE_SIGNATURE);
//
//        // Step 2: Superblock metadata (8 bytes)
//        buffer.put((byte) version);              // Superblock version (1 byte)
//        buffer.put((byte) freeSpaceVersion);     // Free space storage version (1 byte)
//        buffer.put((byte) rootGroupVersion);     // Root group symbol table entry version (1 byte)
//        buffer.put((byte) 0);                    // Reserved (must be 0) (1 byte)
//        buffer.put((byte) sharedHeaderVersion);  // Shared object header format version (1 byte)
//        buffer.put((byte) sizeOfOffsets);        // Size of offsets (1 byte)
//        buffer.put((byte) sizeOfLengths);        // Size of lengths (1 byte)
//        buffer.put((byte) 0);                    // Reserved (must be 0) (1 byte)
//
//        // Step 3: B-tree settings & consistency flags
//        buffer.putShort((short) groupLeafNodeK);      // B-tree group leaf node K (2 bytes)
//        buffer.putShort((short) groupInternalNodeK);  // B-tree group internal node K (2 bytes)
//        buffer.putInt(0);                             // File consistency flags (must be 0) (4 bytes)
//
//        // Step 4: Address fields (sizeOfOffsets bytes each) in little-endian
//        writeFixedPointToBuffer(buffer, baseAddress);         // Base Address
//        writeFixedPointToBuffer(buffer, addressFileFreeSpaceInfo);    // Free space address
//        writeFixedPointToBuffer(buffer, endOfFileAddress);    // End-of-file address
//        writeFixedPointToBuffer(buffer, driverInformationAddress); // Driver info block address
//
//        buffer.flip();
//
////        fileChannel.position(fileAllocation.getSuperblock().allocationRecord.getOffset().getInstance(Long.class));
////        while (buffer.hasRemaining()) {
////            fileChannel.write(buffer);
////        }
//    }

    /**
     * Returns a string representation of the Superblock.
     *
     * @return a string describing the superblock's metadata
     */
    @Override
    public String toString() {
        return "HdfSuperblock{" +
                "version=" + version +
                ", freeSpaceVersion=" + freeSpaceVersion +
                ", rootGroupVersion=" + rootGroupVersion +
                ", sharedHeaderVersion=" + sharedHeaderVersion +
                ", sizeOfOffsets=" + getFixedPointDatatypeForOffset().getSize() +
                ", sizeOfLengths=" + getFixedPointDatatypeForLength().getSize() +
                ", groupLeafNodeK=" + groupLeafNodeK +
                ", groupInternalNodeK=" + groupInternalNodeK +
                ", baseAddress=" + baseAddress +
                ", freeSpaceAddress=" + (addressFileFreeSpaceInfo.isUndefined() ? HdfDisplayUtils.UNDEFINED: addressFileFreeSpaceInfo) +
                ", endOfFileAddress=" + (endOfFileAddress.isUndefined() ?HdfDisplayUtils.UNDEFINED: endOfFileAddress) +
                ", driverInformationAddress=" + (driverInformationAddress.isUndefined() ?HdfDisplayUtils.UNDEFINED: driverInformationAddress) +
                '}';
    }

    public void setEndOfFileAddress(HdfFixedPoint endOfFileAddress) {
        this.endOfFileAddress = endOfFileAddress;
    }

    public FixedPointDatatype getFixedPointDatatypeForOffset() {
        return fixedPointDatatypeForOffset;
    }

    public FixedPointDatatype getFixedPointDatatypeForLength() {
        return fixedPointDatatypeForLength;
    }

    public int getGroupInternalNodeK() {
        return groupInternalNodeK;
    }

    public HdfFixedPoint getRootGroupObjectHeaderAddress() {
        return rootGroupObjectHeaderAddress;
    }

    public int getVersion() {
        return version;
    }
}