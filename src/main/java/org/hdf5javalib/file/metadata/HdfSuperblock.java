package org.hdf5javalib.file.metadata;

import lombok.Getter;
import lombok.Setter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.infrastructure.HdfSymbolTableEntry;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

@Getter
public class HdfSuperblock {
    private static final byte[] FILE_SIGNATURE = new byte[]{(byte) 0x89, 0x48, 0x44, 0x46, 0x0d, 0x0a, 0x1a, 0x0a};

    private final int version;
    private final int freeSpaceVersion;
    private final int rootGroupVersion;
    private final int sharedHeaderVersion;
    private final short offsetSize;
    private final short lengthSize;
    private final int groupLeafNodeK;
    private final int groupInternalNodeK;

    private final HdfFixedPoint baseAddress;
    private final HdfFixedPoint freeSpaceAddress;
    @Setter
    private HdfFixedPoint endOfFileAddress;
    private final HdfFixedPoint driverInformationAddress;
    private final HdfSymbolTableEntry rootGroupSymbolTableEntry;

    public HdfSuperblock(
            int version,
            int freeSpaceVersion,
            int rootGroupVersion,
            int sharedHeaderVersion,
            short offsetSize,
            short lengthSize,
            int groupLeafNodeK,
            int groupInternalNodeK,
            HdfFixedPoint baseAddress,
            HdfFixedPoint freeSpaceAddress,
            HdfFixedPoint endOfFileAddress,
            HdfFixedPoint driverInformationAddress,
            HdfSymbolTableEntry rootGroupSymbolTableEntry
    ) {
        this.version = version;
        this.freeSpaceVersion = freeSpaceVersion;
        this.rootGroupVersion = rootGroupVersion;
        this.sharedHeaderVersion = sharedHeaderVersion;
        this.offsetSize = offsetSize;
        this.lengthSize = lengthSize;
        this.groupLeafNodeK = groupLeafNodeK;
        this.groupInternalNodeK = groupInternalNodeK;
        this.baseAddress = baseAddress;
        this.freeSpaceAddress = freeSpaceAddress;
        this.endOfFileAddress = endOfFileAddress;
        this.driverInformationAddress = driverInformationAddress;
        this.rootGroupSymbolTableEntry = rootGroupSymbolTableEntry;
    }

    public static HdfSuperblock readFromFileChannel(FileChannel fileChannel) throws IOException {
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

        int groupLeafNodeK = Short.toUnsignedInt(buffer.getShort());
        int groupInternalNodeK = Short.toUnsignedInt(buffer.getShort());
        buffer.getInt(); // Skip consistency flags

        HdfSymbolTableEntry rootGroupSymbleTableEntry = HdfSymbolTableEntry.fromFileChannel(fileChannel, offsetSize);

        // Parse addresses using HdfFixedPoint
        BitSet emptyBitSet = new BitSet();
        HdfFixedPoint baseAddress = HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short) 0, (short)(offsetSize*8));
        HdfFixedPoint freeSpaceAddress = HdfFixedPoint.checkUndefined(buffer, offsetSize) ? HdfFixedPoint.undefined(buffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short) 0, (short)(offsetSize*8));
        HdfFixedPoint endOfFileAddress = HdfFixedPoint.checkUndefined(buffer, offsetSize) ? HdfFixedPoint.undefined(buffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short) 0, (short)(offsetSize*8));
        HdfFixedPoint driverInformationAddress = HdfFixedPoint.checkUndefined(buffer, offsetSize) ? HdfFixedPoint.undefined(buffer, offsetSize) : HdfFixedPoint.readFromByteBuffer(buffer, offsetSize, emptyBitSet, (short) 0, (short)(offsetSize*8));

        return new HdfSuperblock(
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
                rootGroupSymbleTableEntry
        );
    }

    public void writeToByteBuffer(ByteBuffer buffer) {
//        buffer.order(ByteOrder.LITTLE_ENDIAN); // Ensure little-endian ordering

        // Step 1: Write the HDF5 file signature (8 bytes)
        buffer.put(new byte[]{(byte) 0x89, 0x48, 0x44, 0x46, 0x0D, 0x0A, 0x1A, 0x0A});

        // Step 2: Superblock metadata (8 bytes)
        buffer.put((byte) version);              // Superblock version (1 byte)
        buffer.put((byte) freeSpaceVersion);     // Free space storage version (1 byte)
        buffer.put((byte) rootGroupVersion);     // Root group symbol table entry version (1 byte)
        buffer.put((byte) 0);                    // Reserved (must be 0) (1 byte)
        buffer.put((byte) sharedHeaderVersion);  // Shared object header format version (1 byte)
        buffer.put((byte) offsetSize);        // Size of offsets (1 byte)
        buffer.put((byte) lengthSize);        // Size of lengths (1 byte)
        buffer.put((byte) 0);                    // Reserved (must be 0) (1 byte)

        // Step 3: B-tree settings & consistency flags
        buffer.putShort((short) groupLeafNodeK);      // B-tree group leaf node K (2 bytes)
        buffer.putShort((short) groupInternalNodeK);  // B-tree group internal node K (2 bytes)
        buffer.putInt(0);                             // File consistency flags (must be 0) (4 bytes)

        // Step 4: Address fields (sizeOfOffsets bytes each) in little-endian
        writeFixedPointToBuffer(buffer, baseAddress);         // Base Address
        writeFixedPointToBuffer(buffer, freeSpaceAddress);    // Free space address
        writeFixedPointToBuffer(buffer, endOfFileAddress);    // End-of-file address
        writeFixedPointToBuffer(buffer, driverInformationAddress); // Driver info block address

        rootGroupSymbolTableEntry.writeToBuffer(buffer);
    }

    @Override
    public String toString() {
        return "HdfSuperblock{" +
                "version=" + version +
                ", freeSpaceVersion=" + freeSpaceVersion +
                ", rootGroupVersion=" + rootGroupVersion +
                ", sharedHeaderVersion=" + sharedHeaderVersion +
                ", sizeOfOffsets=" + offsetSize +
                ", sizeOfLengths=" + lengthSize +
                ", groupLeafNodeK=" + groupLeafNodeK +
                ", groupInternalNodeK=" + groupInternalNodeK +
                ", baseAddress=" + baseAddress +
                ", freeSpaceAddress=" + freeSpaceAddress +
                ", endOfFileAddress=" + endOfFileAddress +
                ", driverInformationAddress=" + driverInformationAddress +
                "\r\n\trootGroupSymbolTableEntry=" + rootGroupSymbolTableEntry +
                '}';
    }
}
