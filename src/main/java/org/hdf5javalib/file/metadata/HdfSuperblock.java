package org.hdf5javalib.file.metadata;

import org.hdf5javalib.HdfDataFile;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.HdfFileAllocation;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.infrastructure.HdfSymbolTableEntry;
import org.hdf5javalib.utils.HdfReadUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

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
    private static final byte[] FILE_SIGNATURE = new byte[]{(byte) 0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a};

    private final int version;
    private final int freeSpaceVersion;
    private final int rootGroupVersion;
    private final int sharedHeaderVersion;
    private final int groupLeafNodeK;
    private final int groupInternalNodeK;

    private final HdfFixedPoint baseAddress;
    private final HdfFixedPoint addressFileFreeSpaceInfo;
    private HdfFixedPoint endOfFileAddress;
    private final HdfFixedPoint driverInformationAddress;
    private final HdfSymbolTableEntry rootGroupSymbolTableEntry;

    // Working properties
    private final HdfDataFile hdfDataFile;
    private final FixedPointDatatype fixedPointDatatypeForOffset;
    private final FixedPointDatatype fixedPointDatatypeForLength;

    /**
     * Constructs a Superblock with the specified metadata.
     *
     * @param version                    the superblock version
     * @param freeSpaceVersion           the free space storage version
     * @param rootGroupVersion           the root group symbol table entry version
     * @param sharedHeaderVersion        the shared object header format version
     * @param groupLeafNodeK             the B-tree group leaf node K value
     * @param groupInternalNodeK         the B-tree group internal node K value
     * @param baseAddress                the base address for relative addressing
     * @param addressFileFreeSpaceInfo   the address of the free space manager
     * @param endOfFileAddress           the end-of-file address
     * @param driverInformationAddress   the address of the driver information block
     * @param rootGroupSymbolTableEntry  the root group symbol table entry
     * @param hdfDataFile                the HDF5 file context
     * @param fixedPointDatatypeForOffset the fixed-point datatype for offsets
     * @param fixedPointDatatypeForLength the fixed-point datatype for lengths
     */
    public HdfSuperblock(
            int version,
            int freeSpaceVersion,
            int rootGroupVersion,
            int sharedHeaderVersion,
            int groupLeafNodeK,
            int groupInternalNodeK,
            HdfFixedPoint baseAddress,
            HdfFixedPoint addressFileFreeSpaceInfo,
            HdfFixedPoint endOfFileAddress,
            HdfFixedPoint driverInformationAddress,
            HdfSymbolTableEntry rootGroupSymbolTableEntry,
            HdfDataFile hdfDataFile,
            FixedPointDatatype fixedPointDatatypeForOffset,
            FixedPointDatatype fixedPointDatatypeForLength
    ) {
        this.version = version;
        this.freeSpaceVersion = freeSpaceVersion;
        this.rootGroupVersion = rootGroupVersion;
        this.sharedHeaderVersion = sharedHeaderVersion;
        this.groupLeafNodeK = groupLeafNodeK;
        this.groupInternalNodeK = groupInternalNodeK;
        this.baseAddress = baseAddress;
        this.addressFileFreeSpaceInfo = addressFileFreeSpaceInfo;
        this.endOfFileAddress = endOfFileAddress;
        this.driverInformationAddress = driverInformationAddress;
        this.rootGroupSymbolTableEntry = rootGroupSymbolTableEntry;
        this.hdfDataFile = hdfDataFile;
        this.fixedPointDatatypeForOffset = fixedPointDatatypeForOffset;
        this.fixedPointDatatypeForLength = fixedPointDatatypeForLength;
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
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if the file signature is invalid or the version is unsupported
     */
    public static HdfSuperblock readFromFileChannel(SeekableByteChannel fileChannel, HdfDataFile hdfDataFile) throws IOException {
        // Step 1: Allocate the minimum buffer size to determine the version
        ByteBuffer buffer = ByteBuffer.allocate(8 + 1); // File signature (8 bytes) + version (1 byte)
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Read the initial bytes to determine the version
        fileChannel.read(buffer);
        buffer.flip();

        // Verify file signature
        byte[] signature = new byte[8];
        buffer.get(signature);
        if (!java.util.Arrays.equals(signature, FILE_SIGNATURE)) {
            throw new IllegalArgumentException("Invalid file signature");
        }

        // Read version
        int version = Byte.toUnsignedInt(buffer.get());

        // Step 2: Determine the size of the superblock based on the version
        int superblockSize;
        if (version == 0) {
            superblockSize = 56; // Version 0 superblock size
        } else if (version == 1) {
            superblockSize = 96; // Version 1 superblock size (example value, adjust per spec)
        } else {
            throw new IllegalArgumentException("Unsupported HDF5 superblock version: " + version);
        }

        // Step 3: Allocate a new buffer for the full superblock
        buffer = ByteBuffer.allocate(superblockSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);

        // Reset file channel position to re-read from the beginning
        fileChannel.position(0);

        // Read the full superblock
        fileChannel.read(buffer);
        buffer.flip();

        // Step 4: Parse the remaining superblock fields
        buffer.position(9); // Skip the file signature
        int freeSpaceVersion = Byte.toUnsignedInt(buffer.get());
        int rootGroupVersion = Byte.toUnsignedInt(buffer.get());
        buffer.get(); // Skip reserved
        int sharedHeaderVersion = Byte.toUnsignedInt(buffer.get());
        short offsetSize = (short) Byte.toUnsignedInt(buffer.get());
        short lengthSize = (short) Byte.toUnsignedInt(buffer.get());
        buffer.get(); // Skip reserved

        FixedPointDatatype fixedPointDatatypeForOffset = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                offsetSize, (short) 0, (short) (8*offsetSize));
        FixedPointDatatype fixedPointDatatypeForLength = new FixedPointDatatype(
                FixedPointDatatype.createClassAndVersion(),
                FixedPointDatatype.createClassBitField(false, false, false, false),
                lengthSize, (short) 0, (short) (8*lengthSize));

        int groupLeafNodeK = Short.toUnsignedInt(buffer.getShort());
        int groupInternalNodeK = Short.toUnsignedInt(buffer.getShort());
        buffer.getInt(); // Skip consistency flags

        HdfSymbolTableEntry rootGroupSymbolTableEntry = HdfSymbolTableEntry.readFromFileChannel(fileChannel, fixedPointDatatypeForOffset);

        // Parse addresses using HdfFixedPoint
        HdfFixedPoint baseAddress = HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
        HdfFixedPoint freeSpaceAddress = HdfReadUtils.checkUndefined(buffer, offsetSize) ? fixedPointDatatypeForOffset.undefined(buffer) : HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
        HdfFixedPoint endOfFileAddress = HdfReadUtils.checkUndefined(buffer, offsetSize) ? fixedPointDatatypeForOffset.undefined(buffer) : HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);
        HdfFixedPoint driverInformationAddress = HdfReadUtils.checkUndefined(buffer, offsetSize) ? fixedPointDatatypeForOffset.undefined(buffer) : HdfReadUtils.readHdfFixedPointFromBuffer(fixedPointDatatypeForOffset, buffer);

        return new HdfSuperblock(
                version,
                freeSpaceVersion,
                rootGroupVersion,
                sharedHeaderVersion,
                groupLeafNodeK,
                groupInternalNodeK,
                baseAddress,
                freeSpaceAddress,
                endOfFileAddress,
                driverInformationAddress,
                rootGroupSymbolTableEntry,
                hdfDataFile,
                fixedPointDatatypeForOffset,
                fixedPointDatatypeForLength
        );
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
    public void writeToFileChannel(SeekableByteChannel fileChannel) throws IOException {
        HdfFileAllocation fileAllocation = hdfDataFile.getFileAllocation();
        ByteBuffer buffer = ByteBuffer.allocate(Math.toIntExact(fileAllocation.getSuperblockSize())).order(ByteOrder.LITTLE_ENDIAN);

        // Step 1: Write the HDF5 file signature (8 bytes)
        buffer.put(new byte[]{(byte) 0x89, 0x48, 0x44, 0x46, 0x0D, 0x0A, 0x1A, 0x0A});

        // Step 2: Superblock metadata (8 bytes)
        buffer.put((byte) version);              // Superblock version (1 byte)
        buffer.put((byte) freeSpaceVersion);     // Free space storage version (1 byte)
        buffer.put((byte) rootGroupVersion);     // Root group symbol table entry version (1 byte)
        buffer.put((byte) 0);                    // Reserved (must be 0) (1 byte)
        buffer.put((byte) sharedHeaderVersion);  // Shared object header format version (1 byte)
        buffer.put((byte) getFixedPointDatatypeForOffset().getSize());        // Size of offsets (1 byte)
        buffer.put((byte) getFixedPointDatatypeForLength().getSize());        // Size of lengths (1 byte)
        buffer.put((byte) 0);                    // Reserved (must be 0) (1 byte)

        // Step 3: B-tree settings & consistency flags
        buffer.putShort((short) groupLeafNodeK);      // B-tree group leaf node K (2 bytes)
        buffer.putShort((short) groupInternalNodeK);  // B-tree group internal node K (2 bytes)
        buffer.putInt(0);                             // File consistency flags (must be 0) (4 bytes)

        // Step 4: Address fields (sizeOfOffsets bytes each) in little-endian
        writeFixedPointToBuffer(buffer, baseAddress);         // Base Address
        writeFixedPointToBuffer(buffer, addressFileFreeSpaceInfo);    // Free space address
        writeFixedPointToBuffer(buffer, endOfFileAddress);    // End-of-file address
        writeFixedPointToBuffer(buffer, driverInformationAddress); // Driver info block address

        rootGroupSymbolTableEntry.writeToBuffer(buffer);

        buffer.flip();

        fileChannel.position(fileAllocation.getSuperblockRecord().getOffset());
        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }
    }

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
                ", freeSpaceAddress=" + addressFileFreeSpaceInfo +
                ", endOfFileAddress=" + endOfFileAddress +
                ", driverInformationAddress=" + driverInformationAddress +
                "\r\nrootGroupSymbolTableEntry=" + rootGroupSymbolTableEntry +
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

    public HdfSymbolTableEntry getRootGroupSymbolTableEntry() {
        return rootGroupSymbolTableEntry;
    }
}