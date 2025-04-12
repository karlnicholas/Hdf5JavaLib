package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfArray;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.file.dataobject.message.DatatypeMessage;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

@Getter
public class ArrayDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;             // Total size of one array element in bytes
    private final int dimensionality;   // Number of dimensions
    private final int[] dimensionSizes; // Size of each dimension (slowest to fastest changing)
    private final int[] permutationIndices; // Permutation indices (currently 0, 1, ..., n-1)
    private final HdfDatatype baseType; // Base type of array elements

    private static final Map<Class<?>, HdfConverter<ArrayDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfArray.class, HdfArray::new);
        CONVERTERS.put(HdfData.class, HdfArray::new);
        CONVERTERS.put(HdfData[].class, (bytes, dt) -> dt.toHdfDataArray(bytes));
        CONVERTERS.put(byte[][].class, (bytes, dt) -> dt.toByteArrayArray(bytes));
    }

    public ArrayDatatype(byte classAndVersion, BitSet classBitField, int size, int dimensionality,
                         int[] dimensionSizes, int[] permutationIndices, HdfDatatype baseType) {
        if (dimensionality < 1) {
            throw new IllegalArgumentException("Dimensionality must be at least 1");
        }
        if (dimensionSizes.length != dimensionality || permutationIndices.length != dimensionality) {
            throw new IllegalArgumentException("Dimension sizes and permutation indices must match dimensionality");
        }
        // Validate size matches product of dimensions * base type size
        long expectedSize = (long) baseType.getSize() * Arrays.stream(dimensionSizes).asLongStream().reduce(1, (a, b) -> a * b);
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
        this.baseType = baseType;
    }

    public static ArrayDatatype parseArrayDatatype(byte classAndVersion, BitSet classBitField,
                                                   int size, ByteBuffer buffer) {
        int dimensionality = buffer.get() & 0xFF; // Unsigned byte
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
        HdfDatatype baseType = DatatypeMessage.getHdfDatatype(buffer);

        return new ArrayDatatype(classAndVersion, classBitField, size, dimensionality,
                dimensionSizes, permutationIndices, baseType);
    }

    public static BitSet createClassBitField() {
        return new BitSet(24); // All bits zero, no settings used
    }

    public static byte createClassAndVersion() {
        return (byte) ((10 << 4) | 2); // Class 10, version 2 (fixed per spec)
    }

    public static <T> void addConverter(Class<T> clazz, HdfConverter<ArrayDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        @SuppressWarnings("unchecked")
        HdfConverter<ArrayDatatype, T> converter = (HdfConverter<ArrayDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<ArrayDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    @Override
    public boolean requiresGlobalHeap(boolean required) {
        return required | false;
    }

    public String toString(byte[] bytes) {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length +
                    ") does not match datatype size (" + size + ")");
        }
        int elementSize = baseType.getSize();
        int totalElements = size / elementSize;
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < totalElements; i++) {
            byte[] elementBytes = Arrays.copyOfRange(bytes, i * elementSize, (i + 1) * elementSize);
            sb.append(baseType.getInstance(String.class, elementBytes));
            if (i < totalElements - 1) sb.append(", ");
        }
        sb.append("]");
        return sb.toString();
    }

    public HdfData[] toHdfDataArray(byte[] bytes) {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length +
                    ") does not match datatype size (" + size + ")");
        }
        int elementSize = baseType.getSize();
        int totalElements = size / elementSize;
        HdfData[] array = new HdfData[totalElements];
        for (int i = 0; i < totalElements; i++) {
            byte[] elementBytes = Arrays.copyOfRange(bytes, i * elementSize, (i + 1) * elementSize);
            array[i] = baseType.getInstance(HdfData.class, elementBytes);
        }
        return array;
    }

    // New private method for byte[][] conversion
    private byte[][] toByteArrayArray(byte[] bytes) {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length +
                    ") does not match datatype size (" + size + ")");
        }
        int elementSize = baseType.getSize();
        int totalElements = size / elementSize;
        byte[][] result = new byte[totalElements][];
        for (int i = 0; i < totalElements; i++) {
            result[i] = Arrays.copyOfRange(bytes, i * elementSize, (i + 1) * elementSize);
        }
        return result;
    }

    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.ARRAY;
    }

    @Override
    public BitSet getClassBitField() {
        return classBitField;
    }

    @Override
    public short getSizeMessageData() {
        return (short) (4 + 4 * dimensionality + 4 * dimensionality + baseType.getSizeMessageData());
        // 1 byte dim + 3 reserved + 4 bytes per dim size + 4 bytes per perm index + base type
    }

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
        baseType.writeDefinitionToByteBuffer(buffer);
    }

    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        // Empty implementation to satisfy interface
    }

    @Override
    public String toString() {
        return "ArrayDatatype{" +
                "size=" + size +
                ", dimensionality=" + dimensionality +
                ", dimensionSizes=" + Arrays.toString(dimensionSizes) +
                ", permutationIndices=" + Arrays.toString(permutationIndices) +
                ", baseType=" + baseType +
                '}';
    }
}