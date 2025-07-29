    package org.hdf5javalib.hdffile.dataobjects.messages;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datatype.FixedPointDatatype;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfDisplayUtils;
import org.hdf5javalib.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

public class ExternalDataFilesMessage extends HdfMessage {
    private static final int externalDataFilesMessageReserved1 = 3;

    private final byte version;
    private final int allocatedSlots;
    private final int usedSlots;
    private final HdfFixedPoint heapAddress;
    private final List<SlotDefinition> slotDefinitions;

    public ExternalDataFilesMessage(byte version, int allocatedSlots, int usedSlots,
                HdfFixedPoint heapAddress, List<SlotDefinition> slotDefinitions,
        int messageFlags, int sizeMessageData) {
            super(MessageType.LinkInfoMessage, sizeMessageData, messageFlags);
            this.version = version;
            this.allocatedSlots = allocatedSlots;
            this.usedSlots = usedSlots;
            this.heapAddress = heapAddress;
            this.slotDefinitions = slotDefinitions;
    }
    /**
     * Parses a LinkInfoMessage from the provided data and file context.
     *
     * @param messageFlags The general HDF message flags.
     * @param data         The byte array containing the message data.
     * @param hdfDataFile  The HDF5 file context for datatype resources.
     * @return A new LinkInfoMessage instance.
     */
    public static HdfMessage parseHeaderMessage(int messageFlags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        byte version = buffer.get();
        buffer.position(buffer.position() + externalDataFilesMessageReserved1);
        int allocatedSlots = Short.toUnsignedInt(buffer.getShort());
        int usedSlots = Short.toUnsignedInt(buffer.getShort());
        HdfFixedPoint heapAddress = HdfReadUtils.readHdfFixedPointFromBuffer(hdfDataFile.getSuperblock().getFixedPointDatatypeForOffset(),  buffer);

        FixedPointDatatype length = hdfDataFile.getSuperblock().getFixedPointDatatypeForLength();
        List<SlotDefinition> slotDefinitions = new ArrayList<>();
        for (int i = 0; i < allocatedSlots; i++) {
            HdfFixedPoint nameOffset = HdfReadUtils.readHdfFixedPointFromBuffer(length,  buffer);
            HdfFixedPoint fileOffset = HdfReadUtils.readHdfFixedPointFromBuffer(length,  buffer);
            HdfFixedPoint dataSize = HdfReadUtils.readHdfFixedPointFromBuffer(length,  buffer);
            if ( i < usedSlots) {
                slotDefinitions.add(new SlotDefinition(nameOffset, fileOffset, dataSize));
            }
        }
        return new ExternalDataFilesMessage(version, allocatedSlots, usedSlots, heapAddress, slotDefinitions,
                messageFlags, data.length);
    }

    /**
     * Writes the LinkInfoMessage data to the provided ByteBuffer.
     *
     * @param buffer The ByteBuffer to write the message data to.
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
    }

    public int getUsedSlots() {
        return usedSlots;
    }

    public List<SlotDefinition> getSlotDefinitions() {
        return slotDefinitions;
    }

    public record SlotDefinition(HdfFixedPoint nameOffset, HdfFixedPoint fileOffset, HdfFixedPoint dataSize) {
        @Override
        public String toString() {
            return "SlotDefinition{" +
                    "nameOffset=" + nameOffset +
                    ", fileOffset=" + fileOffset +
                    ", dataSize=" + (dataSize.isUndefined()? HdfDisplayUtils.UNDEFINED:dataSize) +
                    '}';
        }
    }
    @Override
    public String toString() {
        return "ExternalDataFilesMessage{" +
                "version=" + version +
                ", allocatedSlots=" + allocatedSlots +
                ", usedSlots=" + usedSlots +
                ", heapAddress=" + heapAddress +
                ", slotDefinitions=" + slotDefinitions +
                '}';
    }
}