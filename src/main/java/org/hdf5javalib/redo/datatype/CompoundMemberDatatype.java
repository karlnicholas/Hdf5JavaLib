package org.hdf5javalib.redo.datatype;

import org.hdf5javalib.redo.hdffile.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.redo.hdffile.dataobjects.messages.DatatypeMessage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

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
    /** The name of the compound member. */
    private final String name;
    /** The byte offset of the member within the compound datatype. */
    private final int offset;
    /** The number of dimensions for the member, if an array. */
    private final int dimensionality;
    /** The dimension permutation index for the member. */
    private final int dimensionPermutation;
    /** The sizes of each dimension, if an array. */
    private final int[] dimensionSizes;
    /** The base datatype of the member. */
    private final HdfDatatype type;
    /** The size of the message data for this member in bytes. */
    private final short sizeMessageData;

    /**
     * Constructs a CompoundMemberDatatype for an HDF5 compound datatype member.
     *
     * @param name               the name of the member
     * @param offset             the byte offset within the compound datatype
     * @param dimensionality     the number of dimensions, if an array
     * @param dimensionPermutation the dimension permutation index
     * @param dimensionSizes     the sizes of each dimension
     * @param type               the base datatype of the member
     * @throws IllegalStateException if the datatype class is not supported
     */
    public CompoundMemberDatatype(String name, int offset, int dimensionality, int dimensionPermutation, int[] dimensionSizes, HdfDatatype type) {
        this.name = name;
        this.offset = offset;
        this.dimensionality = dimensionality;
        this.dimensionPermutation = dimensionPermutation;
        this.dimensionSizes = dimensionSizes;
        this.type = type;
        sizeMessageData = switch(type.getDatatypeClass()) {
            case FIXED -> computeFixedMessageDataSize(name);
            case FLOAT -> computeFloatMessageDataSize(name);
            case STRING -> computeStringMessageDataSize(name);
            case VLEN -> computeVariableLengthMessageDataSize(name);
            default -> throw new IllegalStateException("Unexpected datatype class: " + type.getDatatypeClass());
        };
//        log.debug("CompoundMemberDatatype {}", this);
    }

    private short computeFloatMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 -  ((name.length()+1)% 8)) % 8;


            return (short) (name.length()+1 + padding + 40 + 12);
        } else {
            return 52;
        }
    }

    private short computeFixedMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 -  ((name.length()+1)% 8)) % 8;
            return (short) (name.length()+1 + padding + 40 + 4);
        } else {
            return 44;
        }
    }

    private short computeStringMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 -  ((name.length()+1)% 8)) % 8;
            return (short) (name.length()+1 + padding + 40 + 0);
        } else {
            return 40;
        }
    }

    private short computeVariableLengthMessageDataSize(String name) {
        if (!name.isEmpty()) {
            int padding = (8 -  ((name.length()+1)% 8)) % 8;
            return (short) (name.length()+1 + padding + 40 + 12);
        } else {
            return 52;
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
                ", type=" + type +
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
        buffer.put((byte)0);
        int paddingSize = (8 -  ((name.length()+1)% 8)) % 8;
        buffer.put(new byte[paddingSize]);
        buffer.putInt(offset);
        buffer.put((byte)dimensionality);
        buffer.put(new byte[3]);
        buffer.putInt(dimensionPermutation);
        buffer.put(new byte[4]);
        for( int ds: dimensionSizes) {
            buffer.putInt(ds);
        }

        DatatypeMessage.writeDatatypeProperties(buffer, type);
    }

    /**
     * Returns the datatype class of the member's base datatype.
     *
     * @return the DatatypeClass of the base datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return type.getDatatypeClass();
    }

    /**
     * Returns the class and version byte of the member's base datatype.
     *
     * @return the class and version byte
     */
    @Override
    public int getClassAndVersion() {
        return type.getClassAndVersion();
    }

    /**
     * Returns the class bit field of the member's base datatype.
     *
     * @return the BitSet containing class-specific bit field information
     */
    @Override
    public BitSet getClassBitField() {
        return type.getClassBitField();
    }

    /**
     * Returns the size of the member's base datatype in bytes.
     *
     * @return the size in bytes
     */
    @Override
    public int getSize() {
        return type.getSize();
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
        return type.getInstance(clazz, bytes);
    }

    /**
     * Indicates whether a global heap is required for the member's base datatype.
     *
     * @param required true if the global heap is required, false otherwise
     * @return true if the base datatype requires a global heap, false otherwise
     */
    @Override
    public boolean requiresGlobalHeap(boolean required) {
        return type.requiresGlobalHeap(required);
    }

    /**
     * Sets the global heap for the member's base datatype.
     *
     * @param globalHeap the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        type.setGlobalHeap(globalHeap);
    }

    /**
     * Converts the byte array to a string representation using the member's base datatype.
     *
     * @param bytes the byte array to convert
     * @return a string representation of the data
     */
    @Override
    public String toString(byte[] bytes) {
        return type.toString(bytes);
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

    public HdfDatatype getType() {
        return type;
    }
}