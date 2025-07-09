package org.hdf5javalib.maydo.hdffile.dataobjects.messages;

import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfReadUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

/**
 * Represents a Fill Value Message in the HDF5 file format.
 * <p>
 * The {@code FillValueMessage} class defines the default value used to initialize
 * unallocated or uninitialized elements of a dataset. This ensures that datasets
 * have a consistent default value when accessed before explicit data is written.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Version (1 byte)</b>: The version of the fill value format.</li>
 *   <li><b>Space Allocation Time (1 byte, version-dependent)</b>: When space for
 *       fill values is allocated (Early, Late, or Incremental).</li>
 *   <li><b>Fill Value Write Time (1 byte, version-dependent)</b>: When the fill
 *       value is written (on creation or first write).</li>
 *   <li><b>Fill Value Defined Flag (1 byte)</b>: Indicates if a user-defined fill
 *       value is provided.</li>
 *   <li><b>Fill Value Size (4 bytes, optional)</b>: The size of the fill value in bytes.</li>
 *   <li><b>Fill Value Data (variable, optional)</b>: The actual fill value, matching
 *       the dataset's datatype.</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <ul>
 *   <li>Defining default values for missing or newly allocated data.</li>
 *   <li>Ensuring consistency when reading uninitialized portions of a dataset.</li>
 *   <li>Improving dataset integrity in applications requiring structured default data.</li>
 * </ul>
 *
 * @see HdfMessage
 * @see HdfDataFile
 */
public class FilterPipelineMessage extends HdfMessage {
    /**
     * The version of the fill value message format.
     */
    private final int version;
    /**
     * The time when space for fill values is allocated (Early, Late, Incremental).
     */
    private final int numberOfFilters;
    /**
     * The time when the fill value is written (on creation or first write).
     */
    private final List<FilterDescription> filterDescriptions;

    /**
     * Constructs a FillValueMessage with the specified components.
     *
     * @param version             the version of the fill value message format
     * @param numberOfFilters     the number of filters in the pipeline
     * @param filterDescriptions  the descriptions of the filters
     * @param flags               message flags
     * @param sizeMessageData     the size of the message data in bytes
     */
    public FilterPipelineMessage(
            int version,
            int numberOfFilters,
            List<FilterDescription> filterDescriptions,
            int flags,
            int sizeMessageData
    ) {
        super(MessageType.FillValueMessage, sizeMessageData, flags);
        this.version = version;
        this.numberOfFilters = numberOfFilters;
        this.filterDescriptions = filterDescriptions;
    }

    /**
     * Parses a FillValueMessage from the provided data and file context.
     *
     * @param flags       message flags
     * @param data        the byte array containing the message data
     * @param hdfDataFile the HDF5 file context for additional resources
     * @return a new FillValueMessage instance parsed from the data
     */
    public static HdfMessage parseHeaderMessage(int flags, byte[] data, HdfDataFile hdfDataFile) {
        ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);

        // Parse the first 4 bytes
        int version = Byte.toUnsignedInt(buffer.get());
        int numberOfFilters = Byte.toUnsignedInt(buffer.get());
        buffer.position(buffer.position() + 2);
        buffer.position(buffer.position() + 4);

        // Handle Version 2+ behavior and fillValueDefined flag
        if (version == 1) {
            List<FilterDescription> filterDescriptions = new ArrayList<>();
            for(int i = 0; i < numberOfFilters; i++) {
                filterDescriptions.add(FilterDescription.parseFilterDescription(buffer));
            }
            // Return a constructed instance of FillValueMessage
            return new FilterPipelineMessage(version, numberOfFilters, filterDescriptions, flags, data.length);
        }
        throw new UnsupportedOperationException("Version not supported: "  + version);
    }

    /**
     * Returns a string representation of this FillValueMessage.
     *
     * @return a string describing the message size, version, allocation times, and fill value details
     */
    @Override
    public String toString() {
        return "FilterPipelineMessage(" + (getSizeMessageData() + HDF_MESSAGE_PREAMBLE_SIZE) + "){" +
                "version=" + version +
                ", numberOfFilters=" + numberOfFilters +
                ", filterDescriptions=" + filterDescriptions +
                '}';
    }

    /**
     * Writes the fill value message data to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the message data to
     */
    @Override
    public void writeMessageToByteBuffer(ByteBuffer buffer) {
        writeMessageData(buffer);
    }

    static class FilterDescription {
        private final int filterIndentification;
        private final int nameLength;
        private final BitSet flags;
        private final int numberOfValuesForClientData;
        private final String name;
        private final int[] clientData;

        FilterDescription(int filterIndentification, int nameLength, BitSet flags, int numberOfValuesForClientData, String name, int[] clientData) {
            this.filterIndentification = filterIndentification;
            this.nameLength = nameLength;
            this.flags = flags;
            this.numberOfValuesForClientData = numberOfValuesForClientData;
            this.name = name;
            this.clientData = clientData;
        }

        @Override
        public String toString() {
            return "FilterDescription{" +
                    "filterIndentification=" + filterIndentification +
                    ", flags=" + flags +
                    ", name=" + name +
                    ", clientData=" + Arrays.toString(clientData) +
                    '}';
        }

        public static FilterDescription parseFilterDescription(ByteBuffer buffer) {
            int filterIndentification = Short.toUnsignedInt(buffer.getShort());
            int nameLength = Short.toUnsignedInt(buffer.getShort());
            byte[] flagBytes = new byte[2];
            buffer.get(flagBytes);
            BitSet flags = BitSet.valueOf(flagBytes);
            int numberOfValuesForClientData = Short.toUnsignedInt(buffer.getShort());
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            String name = new String(HdfReadUtils.trimZeroBytes(nameBytes));
            int[] clientData = new int[numberOfValuesForClientData];
            for(int i = 0; i < numberOfValuesForClientData; i++) {
                clientData[i] = buffer.getInt();
            }
            if ( numberOfValuesForClientData%2 != 0) {
                int[] padding = new int[1];
                padding[0] = buffer.getInt();
            }
            return new FilterDescription(filterIndentification, nameLength, flags, numberOfValuesForClientData, name, clientData);
        }
    }
}