package org.hdf5javalib.file.infrastructure;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.utils.HdfReadUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.BitSet;

import static org.hdf5javalib.utils.HdfWriteUtils.writeFixedPointToBuffer;

@Getter
public class HdfSymbolTableEntry {
    private final HdfFixedPoint linkNameOffset;
    private final HdfFixedPoint objectHeaderOffset;
    private final int cacheType;
    private final HdfFixedPoint bTreeOffset;
    private final HdfFixedPoint localHeapOffset;

    public HdfSymbolTableEntry(HdfFixedPoint linkNameOffset, HdfFixedPoint objectHeaderOffset, HdfFixedPoint bTreeOffset, HdfFixedPoint localHeapOffset) {
        this.linkNameOffset = linkNameOffset;
        this.objectHeaderOffset = objectHeaderOffset;
        this.cacheType = 1;
        this.bTreeOffset = bTreeOffset;
        this.localHeapOffset = localHeapOffset;
    }

    public HdfSymbolTableEntry(HdfFixedPoint linkNameOffset, HdfFixedPoint objectHeaderOffset) {
        this.linkNameOffset = linkNameOffset;
        this.objectHeaderOffset = objectHeaderOffset;
        this.cacheType = 0;
        this.bTreeOffset = null;
        this.localHeapOffset = null;
    }

    public static HdfSymbolTableEntry readFromFileChannel(
            SeekableByteChannel fileChannel,
            FixedPointDatatype fixedPointDatatypeForOffset
            ) throws IOException {
        BitSet emptyBitSet = new BitSet();
        // Read the fixed-point values for linkNameOffset and objectHeaderAddress
        HdfFixedPoint linkNameOffset = HdfReadUtils.readHdfFixedPointFromFileChannel(fixedPointDatatypeForOffset, fileChannel);
        HdfFixedPoint objectHeaderAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(fixedPointDatatypeForOffset, fileChannel);

        // Read cache type and skip reserved field
        int cacheType = HdfReadUtils.readIntFromFileChannel(fileChannel);
        HdfReadUtils.skipBytes(fileChannel, 4); // Skip reserved field

        // Initialize addresses for cacheType 1
        if (cacheType == 1) {
            HdfFixedPoint bTreeAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(fixedPointDatatypeForOffset, fileChannel);
            HdfFixedPoint localHeapAddress = HdfReadUtils.readHdfFixedPointFromFileChannel(fixedPointDatatypeForOffset, fileChannel);
            return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress, bTreeAddress, localHeapAddress);
        } else {
            HdfReadUtils.skipBytes(fileChannel, 16); // Skip 16 bytes for scratch-pad
            return new HdfSymbolTableEntry(linkNameOffset, objectHeaderAddress);
        }
    }

    public void writeToBuffer(ByteBuffer buffer) {
        // Write Link Name Offset (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, linkNameOffset);

        // Write Object Header Address (sizeOfOffsets bytes, little-endian)
        writeFixedPointToBuffer(buffer, objectHeaderOffset);

        // Write Cache Type (4 bytes, little-endian)
        buffer.putInt(cacheType);

        // Write Reserved Field (4 bytes, must be 0)
        buffer.putInt(0);

        // If cacheType == 1, write B-tree Address and Local Heap Address
        if (cacheType == 1) {
            writeFixedPointToBuffer(buffer, bTreeOffset);
            writeFixedPointToBuffer(buffer, localHeapOffset);
        } else {
            // If cacheType != 1, write 16 bytes of reserved "scratch-pad" space
            buffer.put(new byte[16]);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("HdfSymbolTableEntry{");
        sb.append("linkNameOffset=").append(linkNameOffset)
                .append(", objectHeaderOffset=").append(objectHeaderOffset)
                .append(", cacheType=").append(cacheType);

        switch (cacheType) {
            case 0:
                break; // Base fields only
            case 1:
                sb.append(", bTreeOffset=").append(bTreeOffset)
                        .append(", localHeapOffset=").append(localHeapOffset);
                break;
            default:
                throw new IllegalStateException("Unknown cache type: " + cacheType);
        }

        sb.append("}");
        return sb.toString();
    }
}
