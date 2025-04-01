package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfReference;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

@Getter
public class ReferenceDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size; // Size of the reference data in bytes (e.g., 8 for object reference)

    public enum ReferenceType {
        OBJECT_REFERENCE(0, "Object Reference"),
        DATASET_REGION_REFERENCE(1, "Dataset Region Reference");

        private final int value;
        private final String description;

        ReferenceType(int value, String description) {
            this.value = value;
            this.description = description;
        }

        public int getValue() {
            return value;
        }

        public static ReferenceType fromValue(int value) {
            for (ReferenceType type : values()) {
                if (type.value == value) return type;
            }
            throw new IllegalArgumentException("Unknown reference type value: " + value);
        }
    }

    private static final Map<Class<?>, HdfConverter<ReferenceDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfReference.class, HdfReference::new);
        CONVERTERS.put(HdfData.class, HdfReference::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes); // Raw reference bytes
    }

    public ReferenceDatatype(byte classAndVersion, BitSet classBitField, int size) {
        int typeValue = getTypeValue(classBitField);
        if (typeValue > 1) { // Only 0 and 1 are defined
            throw new IllegalArgumentException("Invalid reference type value: " + typeValue);
        }
        // Check reserved bits (4-23) are zero
        for (int i = 4; i < 24; i++) {
            if (classBitField.get(i)) {
                throw new IllegalArgumentException("Reserved bits (4-23) must be zero in Reference classBitField");
            }
        }
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
    }

    public static ReferenceDatatype parseReferenceDatatype(byte classAndVersion, BitSet classBitField,
                                                           int size, ByteBuffer buffer) {
        // No properties to parse for Reference datatype
        return new ReferenceDatatype(classAndVersion, classBitField, size);
    }

    public static BitSet createClassBitField(ReferenceType type) {
        BitSet bits = new BitSet(24);
        int typeValue = type.getValue();
        for (int i = 0; i < 4; i++) {
            bits.set(i, (typeValue & (1 << i)) != 0);
        }
        // Bits 4-23 remain zero (reserved)
        return bits;
    }

    public static byte createClassAndVersion() {
        return (byte) ((7 << 4) | 1); // Class 7, version 1 (no version 2 defined)
    }

    public static int getTypeValue(BitSet classBitField) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            if (classBitField.get(i)) {
                value |= 1 << i;
            }
        }
        return value;
    }

    public ReferenceType getReferenceType() {
        return ReferenceType.fromValue(getTypeValue(classBitField));
    }

    public static <T> void addConverter(Class<T> clazz, HdfConverter<ReferenceDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        @SuppressWarnings("unchecked")
        HdfConverter<ReferenceDatatype, T> converter = (HdfConverter<ReferenceDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<ReferenceDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    public String toString(byte[] bytes) {
        if (bytes.length != size) {
            throw new IllegalArgumentException("Byte array length (" + bytes.length +
                    ") does not match datatype size (" + size + ")");
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return "Reference[" + getReferenceType().description + "]=" + sb.toString();
    }

    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.REFERENCE;
    }

    @Override
    public BitSet getClassBitField() {
        return classBitField;
    }

    @Override
    public short getSizeMessageData() {
        return 0; // No properties in the datatype message
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        // No properties to write
    }

    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        // Empty implementation to satisfy interface
    }

    @Override
    public String toString() {
        return "ReferenceDatatype{" +
                "size=" + size +
                ", type=" + getReferenceType().description +
                '}';
    }
}