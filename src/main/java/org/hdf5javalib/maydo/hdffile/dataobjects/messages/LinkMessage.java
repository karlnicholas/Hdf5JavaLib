package org.hdf5javalib.maydo.hdffile.dataobjects.messages;

import org.hdf5javalib.maydo.dataclass.HdfFixedPoint;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.utils.HdfReadUtils;
import org.hdf5javalib.maydo.utils.HdfWriteUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.hdf5javalib.maydo.utils.HdfWriteUtils.writeFixedPointToBuffer;

/**
 * Represents a Link Info Message (Type 0x0002) in the HDF5 file format.
 * <p>
 * The {@code LinkInfoMessage} tracks information about the links within a "new style" group.
 * It contains flags and addresses that determine how links are stored and indexed, distinguishing
 * between compact storage and more complex structures like Fractal Heaps and v2 B-trees.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte):</b> The version number for this message format (currently 0).</li>
 *   <li><b>Flags (1 byte):</b> A bitfield indicating whether creation order is tracked and/or indexed.</li>
 *   <li><b>Maximum Creation Index (8 bytes, optional):</b> The maximum creation order index used. Present if creation order is tracked (flag bit 0 is set).</li>
 *   <li><b>Fractal Heap Address (variable size):</b> Address of the Fractal Heap used for storing links.</li>
 *   <li><b>v2 B-tree Address for Name Index (variable size):</b> Address of the B-tree for indexing links by name.</li>
 *   <li><b>v2 B-tree Address for Creation Order Index (variable size, optional):</b> Address of the B-tree for indexing links by creation order. Present if creation order is indexed (flag bit 1 is set).</li>
 * </ul>
 *
 * <h2>Purpose</h2>
 * <ul>
 *   <li>Manages metadata for link storage in modern HDF5 groups.</li>
 *   <li>Provides pointers to advanced data structures (Fractal Heap, v2 B-trees) for efficient link management.</li>
 *   <li>Controls whether link creation order is preserved and indexed.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 */
public class LinkMessage extends HdfMessage {
    private final int version;
    private final int flags;
    private final int linkType;
    private final HdfFixedPoint creationOrder;
    private final int linkNameCharacterSet;
    private final long lengthOfLinkName;
    private final String linkName;
    private final Object linkInformation;

    public LinkMessage(
            int version,
            int flags,
            int linkType,
            HdfFixedPoint creationOrder,
            int linkNameCharacterSet,
            long lengthOfLinkName,
            String linkName,
            Object linkInformation,
            int messageFlags,
            int sizeMessageData
    ) {
        super(MessageType.LinkMessage, sizeMessageData, flags);
        this.version = version;
        this.flags = flags;
        this.linkType = linkType;
        this.creationOrder = creationOrder;
        this.linkNameCharacterSet = linkNameCharacterSet;
        this.lengthOfLinkName = lengthOfLinkName;
        this.linkName = linkName;
        this.linkInformation = linkInformation;
    }

    public static HdfMessage parseHeaderMessage(int messageFlags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        int version = Byte.toUnsignedInt(buffer.get());
        int flags = Byte.toUnsignedInt(buffer.get());
        int linkType = -1;
        if ( (flags & 1<<3) != 0 ) {
            linkType = Byte.toUnsignedInt(buffer.get());
        }
        HdfFixedPoint creationOrder;
        if ( (flags & 1<<2) != 0 ) {
            creationOrder = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForLength(), buffer);
        } else {
            creationOrder = hdfDataFile.getSuperblock().getFixedPointDatatypeForLength().undefined();
        }
        // default to ASCII
        int linkNameCharacterSet = 0;
        if ( (flags & 1<<4) == 1 ) {
            linkNameCharacterSet = Byte.toUnsignedInt(buffer.get());
        }
        int nameBytes = flags & 0x3;
        long lengthOfLinkName = switch (nameBytes) {
            case 0 -> Byte.toUnsignedInt(buffer.get());
            case 1 -> Short.toUnsignedInt(buffer.getShort());
            case 2 -> Integer.toUnsignedLong(buffer.getInt());
            case 3 -> buffer.getLong();
            default -> throw new IllegalArgumentException("Unknown link name length: " + nameBytes);
        };

        byte[] linkNameBytes =  new byte[Math.toIntExact(lengthOfLinkName)];
        buffer.get(linkNameBytes);
        String linkName = new String(linkNameBytes, linkNameCharacterSet > 0 ? StandardCharsets.UTF_8 : StandardCharsets.US_ASCII);
        Object linkInformation = switch (linkType) {
            case 0 -> HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(), buffer);
            case 1, 64 -> {
                int softLinkSize = Short.toUnsignedInt(buffer.getShort());
                byte[] softLinkBytes = new byte[softLinkSize];
                buffer.get(softLinkBytes);
                yield new String(softLinkBytes);
            }
            default -> null;
        };

        return new LinkMessage(
                version,
                flags,
                linkType,
                creationOrder,
                linkNameCharacterSet,
                lengthOfLinkName,
                linkName,
                linkInformation,
                messageFlags, data.length);
    }

        @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {

    }
}