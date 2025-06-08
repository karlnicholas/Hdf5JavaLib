package org.hdf5javalib.redo.datatype;

import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.hdffile.dataobjects.messages.DatatypeMessage;
import org.hdf5javalib.redo.hdffile.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * Represents a member of an HDF5 Compound Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code CompoundMemberDatatype} class models a single member within a compound datatype,
 * encapsulating its name, offset, dimensionality, and underlying datatype. It supports writing
 * to a {@link ByteBuffer} and delegates data conversion to the member's base datatype.
 * </p>
 *
 * @see HdfDatatype
 * @see DatatypeMessage
 */
public class CompoundMemberDatatype implements HdfDatatype {
    /**
     * The name of the compound member.
     */
    private final String name;
    /**
     * The byte offset of the member within the compound datatype.
     */
    private final int offset;
    /**
     * The number of dimensions for the member, if an array.
     */
    private final int dimensionality;
    /**
     * The dimension permutation index for the member.
     */
    private final int dimensionPermutation;
    /**
     * The sizes of each dimension, if an array.
     */
    private final int[] dimensionSizes;
    /**
     * The base datatype of the member.
     */
    private final HdfDatatype hdfDatatype;
    /**
     * The size of the message data for this member in bytes.
     */
    private final int sizeMessageData;
    private final HdfDataFile dataFile;

    /**
     * Constructs a CompoundMemberDatatype for an HDF5 compound datatype member.
     *
     * @param name                 the name of the member
     * @param offset               the byte offset within the compound datatype
     * @param dimensionality       the number of dimensions, if an array
     * @param dimensionPermutation the dimension permutation index
     * @param dimensionSizes       the sizes of each dimension
     * @param hdfDatatype                 the base datatype of the member
     * @throws IllegalStateException if the datatype class is not supported
     */
    public CompoundMemberDatatype(String name, int offset, int dimensionality, int dimensionPermutation, int[] dimensionSizes, HdfDatatype hdfDatatype, HdfDataFile dataFile) {
        this.name = name;
        this.offset = offset;
        this.dimensionality = dimensionality;
        this.dimensionPermutation = dimensionPermutation;
        this.dimensionSizes = dimensionSizes;
        this.hdfDatatype = hdfDatatype;
        this.dataFile = dataFile;
        sizeMessageData = switch (hdfDatatype.getDatatypeClass()) {
            case FIXED -> computeFixedMessageDataSize(name);
            case FLOAT -> computeFloatMessageDataSize(name);
            case TIME -> computeTimeMessageDataSize(name);
            case STRING -> computeStringMessageDataSize(name);
            case BITFIELD -> computeBitfieldMessageDataSize(name);
            case OPAQUE -> computeOpaqueMessageDataSize(name);
            case COMPOUND -> computeCompoundMessageDataSize(name);
            case REFERENCE -> computeReferenceMessageDataSize(name);
            case ENUM -> computeEnumMessageDataSize(name);
            case VLEN -> computeVariableLengthMessageDataSize(name);
            case ARRAY -> computeArrayMessageDataSize(name);
        };
//        log.debug("CompoundMemberDatatype {}", this);
    }

    private short computeFixedMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 - ((name.length() + 1) % 8)) % 8;
            return (short) (name.length() + 1 + padding + 40 + 4);
        } else {
            return 44;
        }
    }

    private short computeFloatMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 - ((name.length() + 1) % 8)) % 8;
            return (short) (name.length() + 1 + padding + 40 + 12);
        } else {
            return 52;
        }
    }

    private int computeTimeMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 - ((name.length() + 1) % 8)) % 8;
            return (short) (name.length() + 1 + padding + 40 + 2);
        } else {
            return 42;
        }
    }

    private short computeStringMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 - ((name.length() + 1) % 8)) % 8;
            return (short) (name.length() + 1 + padding + 40 + 0);
        } else {
            return 40;
        }
    }

    private int computeBitfieldMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 - ((name.length() + 1) % 8)) % 8;
            return (short) (name.length() + 1 + padding + 40 + 4);
        } else {
            return 44;
        }
    }

    private int computeOpaqueMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 - ((name.length() + 1) % 8)) % 8;
            return (short) (name.length() + 1 + padding + 40 + 0);
        } else {
            return 40;
        }
    }

    private int computeCompoundMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 - ((name.length() + 1) % 8)) % 8;
            return (short) (name.length() + 1 + padding + 40 + 0);
        } else {
            return 40;
        }
    }

    private int computeReferenceMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 - ((name.length() + 1) % 8)) % 8;
            return (short) (name.length() + 1 + padding + 40 + 0);
        } else {
            return 40;
        }
    }

    private int computeEnumMessageDataSize(String name) {
        // dymanic based on stuff
        if (!name.isEmpty()) {
            int padding = (8 - ((name.length() + 1) % 8)) % 8;
            return (short) (name.length() + 1 + padding + 40 + 12);
        } else {
            return 52;
        }
    }

    private short computeVariableLengthMessageDataSize(String name) {
        // parent type message
        if (!name.isEmpty()) {
            int padding = (8 - ((name.length() + 1) % 8)) % 8;
            return (short) (name.length() + 1 + padding + 40 + 12);
        } else {
            return 52;
        }
    }

    private int computeArrayMessageDataSize(String name) {
        // dynamic based on stuff
        if (!name.isEmpty()) {
            int padding = (8 - ((name.length() + 1) % 8)) % 8;
            return (short) (name.length() + 1 + padding + 40 + 0);
        } else {
            return 40;
        }
    }

    /**
     * Returns a string representation of this CompoundMemberDatatype.
     *
     * @return a string describing the member's name, offset, dimensionality, permutation, dimension sizes, type, and message data size
     */
    @Override
    public String toString() {
        return "Member{" +
                "name='" + name + '\'' +
                ", offset=" + offset +
                ", dimensionality=" + dimensionality +
                ", dimensionPermutation=" + dimensionPermutation +
                ", dimensionSizes=" + java.util.Arrays.toString(dimensionSizes) +
                ", type=" + hdfDatatype +
                ", sizeMessageData=" + sizeMessageData +
                '}';
    }

    /**
     * Writes the member's datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.put(name.getBytes(StandardCharsets.US_ASCII));
        buffer.put((byte) 0);
        int paddingSize = (8 - ((name.length() + 1) % 8)) % 8;
        buffer.put(new byte[paddingSize]);
        buffer.putInt(offset);
        buffer.put((byte) dimensionality);
        buffer.put(new byte[3]);
        buffer.putInt(dimensionPermutation);
        buffer.put(new byte[4]);
        for (int ds : dimensionSizes) {
            buffer.putInt(ds);
        }

        DatatypeMessage.writeDatatypeProperties(buffer, hdfDatatype);
    }

    /**
     * Returns the datatype class of the member's base datatype.
     *
     * @return the DatatypeClass of the base datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return hdfDatatype.getDatatypeClass();
    }

    /**
     * Returns the class and version byte of the member's base datatype.
     *
     * @return the class and version byte
     */
    @Override
    public int getClassAndVersion() {
        return hdfDatatype.getClassAndVersion();
    }

    /**
     * Returns the class bit field of the member's base datatype.
     *
     * @return the BitSet containing class-specific bit field information
     */
    @Override
    public BitSet getClassBitField() {
        return hdfDatatype.getClassBitField();
    }

    /**
     * Returns the size of the member's base datatype in bytes.
     *
     * @return the size in bytes
     */
    @Override
    public int getSize() {
        return hdfDatatype.getSize();
    }

    /**
     * Converts byte data to an instance of the specified class using the member's base datatype.
     *
     * @param <T>   the type of the instance to be created
     * @param clazz the Class object representing the target type
     * @param bytes the byte array containing the data
     * @return an instance of type T created from the byte array
     */
    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        return hdfDatatype.getInstance(clazz, bytes);
    }

    /**
     * Indicates whether a global heap is required for the member's base datatype.
     *
     * @param required true if the global heap is required, false otherwise
     * @return true if the base datatype requires a global heap, false otherwise
     */
    @Override
    public boolean requiresGlobalHeap(boolean required) {
        return hdfDatatype.requiresGlobalHeap(required);
    }

    /**
     * Sets the global heap for the member's base datatype.
     *
     * @param globalHeap the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        hdfDatatype.setGlobalHeap(globalHeap);
    }

    /**
     * Converts the byte array to a string representation using the member's base datatype.
     *
     * @param bytes the byte array to convert
     * @return a string representation of the data
     */
    @Override
    public String toString(byte[] bytes) {
        return hdfDatatype.toString(bytes);
    }

    @Override
    public HdfDataFile getDataFile() {
        return dataFile;
    }

    @Override
    public int getSizeMessageData() {
        return sizeMessageData;
    }

    public String getName() {
        return name;
    }

    public int getOffset() {
        return offset;
    }

    public HdfDatatype getHdfDatatype() {
        return hdfDatatype;
    }

    @Override
    public List<ReferenceDatatype> getReferenceInstances() {
        return hdfDatatype.getReferenceInstances();
    }
}