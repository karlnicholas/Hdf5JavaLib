package org.hdf5javalib.datatype;

import org.hdf5javalib.dataclass.HdfArray;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.hdffile.dataobjects.messages.DatatypeMessage;
import org.hdf5javalib.hdffile.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.hdfjava.HdfDataFile;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Represents an HDF5 Array Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code ArrayDatatype} class models a multi-dimensional array in HDF5, where each element has a defined base datatype.
 * It supports parsing from a {@link ByteBuffer}, validation of size and permutation indices, and conversion to Java
 * objects such as {@link HdfArray}, {@code byte[][]}, or {@code String} using registered converters.
 * </p>
 *
 * <h2>Structure</h2>
 * <ul>
 *   <li><b>Class and Version</b>: Identifies the datatype as class 10, version 2 (1 byte).</li>
 *   <li><b>Class Bit Field</b>: Reserved for future flags, currently unused.</li>
 *   <li><b>Size</b>: Total byte size of the entire array value.</li>
 *   <li><b>Dimensionality</b>: Number of dimensions in the array.</li>
 *   <li><b>Dimension Sizes</b>: Size of each dimension, in slowest-to-fastest order.</li>
 *   <li><b>Permutation Indices</b>: Canonical order (0, 1, ..., n-1) for memory layout.</li>
 *   <li><b>Base Type</b>: Datatype of individual array elements.</li>
 * </ul>
 *
 * @see Datatype
 * @see HdfGlobalHeap
 * @see DatatypeMessage
 */
public class ArrayDatatype implements Datatype {
    /**
     * The class and version information for the datatype (class 10, version 2).
     */
    private final int classAndVersion;
    /**
     * A BitSet containing class-specific bit field information (currently unused).
     */
    private final BitSet classBitField;
    /**
     * The total size of the array datatype in bytes.
     */
    private final int size;
    /**
     * The number of dimensions in the array.
     */
    private final int dimensionality;
    /**
     * The size of each dimension, in slowest-to-fastest order.
     */
    private final int[] dimensionSizes;
    /**
     * The permutation indices for dimensions (must be 0, 1, ..., n-1).
     */
    private final int[] permutationIndices;
    /**
     * The base datatype of the array elements.
     */
    private final Datatype datatype;
    private final HdfDataFile dataFile;

    /**
     * Map of converters for transforming byte data to specific Java types.
     */
    private static final Map<Class<?>, DatatypeConverter<ArrayDatatype, ?>> CONVERTERS = new HashMap<>();

    static {
        CONVERTERS.put(String.class, (bytes, dt) -> {
            return dt.toString(bytes);
        });
        CONVERTERS.put(HdfArray.class, HdfArray::new);
        CONVERTERS.put(HdfData.class, HdfArray::new);
        CONVERTERS.put(HdfData[].class, (bytes, dt) -> dt.toHdfDataArray(bytes));
        CONVERTERS.put(byte[][].class, (bytes, dt) -> dt.toByteArrayArray(bytes));
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes);
    }

    /**
     * Constructs an ArrayDatatype representing an HDF5 array datatype.
     *
     * @param classAndVersion    The class and version information for the datatype.
     * @param classBitField      A BitSet containing class-specific bit field information.
     * @param size               The total size of the array datatype in bytes.
     * @param dimensionality     The number of dimensions in the array (must be at least 1).
     * @param dimensionSizes     An array specifying the size of each dimension.
     * @param permutationIndices An array specifying the permutation indices for the dimensions (must be in canonical order: 0, 1, ..., n-1).
     * @param datatype           The base datatype of the array elements.
     * @param dataFile
     * @throws IllegalArgumentException if:
     *                                  <ul>
     *                                    <li>dimensionality is less than 1</li>
     *                                    <li>the length of dimensionSizes or permutationIndices does not match dimensionality</li>
     *                                    <li>the total size does not match the product of dimension sizes and base type size</li>
     *                                    <li>permutationIndices are not in canonical order (0, 1, ..., n-1)</li>
     *                                  </ul>
     */
    public ArrayDatatype(int classAndVersion, BitSet classBitField, int size, int dimensionality,
                         int[] dimensionSizes, int[] permutationIndices, Datatype datatype, HdfDataFile dataFile) {
        if (dimensionality < 1) {
            throw new IllegalArgumentException("Dimensionality must be at least 1");
        }
        if (dimensionSizes.length != dimensionality || permutationIndices.length != dimensionality) {
            throw new IllegalArgumentException("Dimension sizes and permutation indices must match dimensionality");
        }
        // Validate size matches product of dimensions * base type size
        long expectedSize = datatype.getSize() * Arrays.stream(dimensionSizes).asLongStream().reduce(1, (a, b) -> a * b);
        if (expectedSize != size) {
            throw new IllegalArgumentException("Size (" + size + ") does not match base type size * dimensions (" + expectedSize + ")");
        }
        // Note: Permutation indices are not supported in HDF5 spec version 2; should be 0, 1, ..., n-1
        for (int i = 0; i < dimensionality; i++) {
            if (permutationIndices[i] != i) {
                throw new IllegalArgumentException("Permutation indices must be in canonical order (0, 1, ..., n-1) as reordering is not supported");
            }
        }
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.dimensionality = dimensionality;
        this.dimensionSizes = dimensionSizes.clone();
        this.permutationIndices = permutationIndices.clone();
        this.datatype = datatype;
        this.dataFile = dataFile;
    }

    /**
     * Parses an HDF5 array datatype from a ByteBuffer as per the HDF5 specification.
     *
     * @param classAndVersion the class and version byte of the datatype
     * @param classBitField   the BitSet containing class-specific bit field information
     * @param size            the total size of the array datatype in bytes
     * @param buffer          the ByteBuffer containing the datatype definition
     * @return a new ArrayDatatype instance parsed from the buffer
     */
    public static ArrayDatatype parseArrayDatatype(int classAndVersion, BitSet classBitField,
                                                   int size, ByteBuffer buffer, HdfDataFile hdfDataFile) {
        int dimensionality = Byte.toUnsignedInt(buffer.get()); // Unsigned byte
        buffer.get(new byte[3]); // Skip 3 reserved bytes (zero)

        // Read dimension sizes (slowest to fastest changing)
        int[] dimensionSizes = new int[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            dimensionSizes[i] = buffer.getInt();
        }

        // Read permutation indices (expected to be 0, 1, ..., n-1)
        int[] permutationIndices = new int[dimensionality];
        for (int i = 0; i < dimensionality; i++) {
            permutationIndices[i] = buffer.getInt();
        }

        // Parse base type
        Datatype baseType = DatatypeMessage.getHdfDatatype(buffer, hdfDataFile);

        return new ArrayDatatype(classAndVersion, classBitField, size, dimensionality,
                dimensionSizes, permutationIndices, baseType, hdfDataFile);
    }

    /**
     * Creates a BitSet representing the class bit field for an HDF5 array datatype.
     *
     * @return a new 24-bit BitSet with all bits set to zero, indicating no settings are used
     */
    public static BitSet createClassBitField() {
        return new BitSet(24); // All bits zero, no settings used
    }

    /**
     * Creates a fixed class and version byte for an HDF5 array datatype.
     *
     * @return a byte representing class 10 and version 2, as defined by the HDF5 specification
     */
    public static byte createClassAndVersion() {
        return (byte) ((10 << 4) | 2); // Class 10, version 2 (fixed per spec)
    }

    /**
     * Registers a converter for transforming ArrayDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the DatatypeConverter for converting between ArrayDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, DatatypeConverter<ArrayDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    /**
     * Converts byte data to an instance of the specified class using registered converters.
     *
     * @param <T>   the type of the instance to be created
     * @param clazz the Class object representing the target type
     * @param bytes the byte array containing the data
     * @return an instance of type T created from the byte array
     * @throws UnsupportedOperationException if no suitable converter is found for the specified class
     */
    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        @SuppressWarnings("unchecked")
        DatatypeConverter<ArrayDatatype, T> converter = (DatatypeConverter<ArrayDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, DatatypeConverter<ArrayDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    /**
     * Converts the byte array to a string representation of the array elements.
     *
     * @param bytes the byte array to convert
     * @return a string representation of the array elements
     * @throws IllegalArgumentException if the byte array length does not match the datatype size
     */
    public String toString(byte[] bytes) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length +
                    ") does not match datatype size (" + size + ")");
        }
        int elementSize = datatype.getSize();
        int totalElements = size / elementSize;
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < totalElements; i++) {
            byte[] elementBytes = Arrays.copyOfRange(bytes, i * elementSize, (i + 1) * elementSize);
            sb.append(datatype.getInstance(String.class, elementBytes));
            if (i < totalElements - 1) sb.append(", ");
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public HdfDataFile getDataFile() {
        return dataFile;
    }

    /**
     * Converts the byte array to an array of HdfData objects.
     *
     * @param bytes the byte array to convert
     * @return an array of HdfData objects representing the array elements
     * @throws IllegalArgumentException if the byte array length does not match the datatype size
     */
    public HdfData[] toHdfDataArray(byte[] bytes) throws InvocationTargetException, InstantiationException, IllegalAccessException, IOException {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length +
                    ") does not match datatype size (" + size + ")");
        }
        int elementSize = datatype.getSize();
        int totalElements = size / elementSize;
        HdfData[] array = new HdfData[totalElements];
        for (int i = 0; i < totalElements; i++) {
            byte[] elementBytes = Arrays.copyOfRange(bytes, i * elementSize, (i + 1) * elementSize);
            array[i] = datatype.getInstance(HdfData.class, elementBytes);
        }
        return array;
    }

    // New private method for byte[][] conversion
    private byte[][] toByteArrayArray(byte[] bytes) {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length +
                    ") does not match datatype size (" + size + ")");
        }
        int elementSize = datatype.getSize();
        int totalElements = size / elementSize;
        byte[][] result = new byte[totalElements][];
        for (int i = 0; i < totalElements; i++) {
            result[i] = Arrays.copyOfRange(bytes, i * elementSize, (i + 1) * elementSize);
        }
        return result;
    }

    /**
     * Returns the datatype class for this array datatype.
     *
     * @return DatatypeClass.ARRAY, indicating an HDF5 array datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.ARRAY;
    }

    /**
     * Returns the class bit field for this datatype.
     *
     * @return the BitSet containing class-specific bit field information
     */
    @Override
    public BitSet getClassBitField() {
        return classBitField;
    }

    /**
     * Returns the size of the datatype message data.
     *
     * @return the size of the message data in bytes, as a short
     */
    @Override
    public int getSizeMessageData() {
        return (short) (4 + 4 * dimensionality + 4 * dimensionality + datatype.getSizeMessageData());
        // 1 byte dim + 3 reserved + 4 bytes per dim size + 4 bytes per perm index + base type
    }

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.put((byte) dimensionality);
        buffer.put(new byte[]{0, 0, 0}); // 3 reserved bytes
        for (int dimSize : dimensionSizes) {
            buffer.putInt(dimSize);
        }
        for (int permIndex : permutationIndices) {
            buffer.putInt(permIndex);
        }
        datatype.writeDefinitionToByteBuffer(buffer);
    }

    /**
     * Sets the global heap for this datatype (no-op for ArrayDatatype).
     *
     * @param globalHeap the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        datatype.setGlobalHeap(globalHeap);
        // Empty implementation to satisfy interface
    }

    /**
     * Returns a string representation of this ArrayDatatype.
     *
     * @return a string describing the datatype's size, dimensionality, dimension sizes, permutation indices, and base type
     */
    @Override
    public String toString() {
        return "ArrayDatatype{" +
                "size=" + size +
                ", dimensionality=" + dimensionality +
                ", dimensionSizes=" + Arrays.toString(dimensionSizes) +
                ", permutationIndices=" + Arrays.toString(permutationIndices) +
                ", datatype=" + datatype +
                '}';
    }

    /**
     * Returns the class and version byte for this datatype.
     *
     * @return the class and version byte
     */
    @Override
    public int getClassAndVersion() {
        return classAndVersion;
    }

    /**
     * Returns the total size of the array datatype in bytes.
     *
     * @return the total size in bytes
     */
    @Override
    public int getSize() {
        return size;
    }

    @Override
    public List<ReferenceDatatype> getReferenceInstances() {
        return datatype.getReferenceInstances();
    }
}