package org.hdf5javalib.maydo.datatype;

import org.hdf5javalib.maydo.hdffile.HdfDataFile;
import org.hdf5javalib.maydo.hdffile.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

/**
 * Defines the interface for HDF5 datatypes, providing methods to manage datatype definitions,
 * serialization, and instance creation.
 */
public interface Datatype {

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    void writeDefinitionToByteBuffer(ByteBuffer buffer);

    /**
     * Retrieves the datatype class as an enum value.
     *
     * @return the DatatypeClass enum representing the datatype class
     */
    DatatypeClass getDatatypeClass();

    /**
     * Retrieves the class and version byte for the datatype.
     *
     * @return a byte representing the class and version of the datatype
     */
    int getClassAndVersion();

    /**
     * Retrieves the class bit field for the datatype.
     *
     * @return a BitSet containing class-specific bit field information
     */
    BitSet getClassBitField();

    /**
     * Retrieves the total size of the datatype in bytes.
     *
     * @return the size of the datatype in bytes
     */
    int getSize();

    /**
     * Retrieves the size of the message data for the datatype.
     *
     * @return the size of the message data as a short
     */
    int getSizeMessageData();

    /**
     * Creates an instance of the specified class from the given byte array.
     *
     * @param <T>   the type of the instance to be created
     * @param clazz the Class object representing the type to be created
     * @param bytes the byte array containing the data to convert
     * @return an instance of type T created from the byte array
     */
    <T> T getInstance(Class<T> clazz, byte[] bytes);

    /**
     * Indicates whether the datatype requires a global heap.
     *
     * @param required true if the global heap is required, false otherwise
     * @return true if the datatype requires a global heap, false otherwise
     */
    boolean requiresGlobalHeap(boolean required);

    /**
     * Sets the global heap for the datatype.
     *
     * @param globalHeap the HdfGlobalHeap to be set
     */
    void setGlobalHeap(HdfGlobalHeap globalHeap);

    /**
     * Converts the given byte array to a string representation based on the datatype.
     *
     * @param bytes the byte array to convert
     * @return a string representation of the byte array
     */
    String toString(byte[] bytes);

    HdfDataFile getDataFile();

    /**
     * Enum representing the possible HDF5 datatype classes.
     */
    enum DatatypeClass {
        /**
         * HDF5 integer types.
         */
        FIXED(0),

        /**
         * HDF5 floating-point types.
         */
        FLOAT(1),

        /**
         * HDF5 time types.
         */
        TIME(2),

        /**
         * HDF5 string types.
         */
        STRING(3),

        /**
         * HDF5 bitfield types.
         */
        BITFIELD(4),

        /**
         * HDF5 opaque types.
         */
        OPAQUE(5),

        /**
         * HDF5 compound types.
         */
        COMPOUND(6),

        /**
         * HDF5 reference types.
         */
        REFERENCE(7),

        /**
         * HDF5 enumerated types.
         */
        ENUM(8),

        /**
         * HDF5 variable-length types.
         */
        VLEN(9),

        /**
         * HDF5 array types.
         */
        ARRAY(10);

        /**
         * The numeric value associated with the datatype class.
         */
        private final int value;

        /**
         * Constructs a DatatypeClass with the specified value.
         *
         * @param value the numeric value of the datatype class
         */
        DatatypeClass(int value) {
            this.value = value;
        }

        /**
         * Retrieves the DatatypeClass corresponding to the given value.
         *
         * @param value the numeric value of the datatype class
         * @return the corresponding DatatypeClass
         * @throws IllegalArgumentException if the value does not match any known datatype class
         */
        public static DatatypeClass fromValue(int value) {
            for (DatatypeClass type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown datatype class value: " + value);
        }
    }

    public List<ReferenceDatatype> getReferenceInstances();
}