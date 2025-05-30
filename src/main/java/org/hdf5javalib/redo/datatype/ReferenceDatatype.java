package org.hdf5javalib.redo.datatype;

import org.hdf5javalib.redo.dataclass.HdfData;
import org.hdf5javalib.redo.dataclass.HdfReference;
import org.hdf5javalib.redo.hdffile.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

/** HDF5 Reference Datatype (Class 7) for objects, dataset regions, or attributes. */
public class ReferenceDatatype implements HdfDatatype {
    private final int classAndVersion;
    private final BitSet classBitField;
    private final int size;

    public enum ReferenceType {
        OBJECT1(0, "Object Reference (H5R_OBJECT1)"),
        DATASET_REGION1(1, "Dataset Region Reference (H5R_DATASET_REGION1)"),
        OBJECT2(2, "Object Reference (H5R_OBJECT2)"),
        DATASET_REGION2(3, "Dataset Region Reference (H5R_DATASET_REGION2)"),
        ATTR(4, "Attribute Reference (H5R_ATTR)");

        private final int value;
        private final String description;

        ReferenceType(int value, String description) {
            this.value = value;
            this.description = description;
        }

        public int getValue() { return value; }

        public String getDescription() { return description; }

        public static ReferenceType fromValue(int value) {
            for (ReferenceType type : values()) {
                if (type.value == value) return type;
            }
            throw new IllegalArgumentException("Unknown reference type: " + value);
        }
    }

    private static final Map<Class<?>, HdfConverter<ReferenceDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfReference.class, (bytes, dt) -> new HdfReference(bytes, dt));
        CONVERTERS.put(HdfData.class, (bytes, dt) -> new HdfReference(bytes, dt));
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes.clone());
    }

    public ReferenceDatatype(int classAndVersion, BitSet classBitField, int size) {
        int typeValue = getTypeValue(classBitField);
        if (typeValue > 4) throw new IllegalArgumentException("Invalid reference type: " + typeValue);

        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
    }

    public static ReferenceDatatype parseReferenceDatatype(int classAndVersion, BitSet classBitField, int size, ByteBuffer buffer) {
        return new ReferenceDatatype(classAndVersion, classBitField, size);
    }

    public static BitSet createClassBitField(ReferenceType type) {
        BitSet bits = new BitSet(24);
        int typeValue = type.getValue();
        for (int i = 0; i < 4; i++) bits.set(i, (typeValue & (1 << i)) != 0);
        return bits;
    }

    public static byte createClassAndVersion() {
        return (byte) (7 << 4);
    }

    public static int getTypeValue(BitSet classBitField) {
        int value = 0;
        for (int i = 0; i < 4; i++) if (classBitField.get(i)) value |= 1 << i;
        return value;
    }

    public ReferenceType getReferenceType() {
        return ReferenceType.fromValue(getTypeValue(classBitField));
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        if (bytes.length != size) throw new IllegalArgumentException("Byte array length mismatch");
        HdfConverter<ReferenceDatatype, T> converter = (HdfConverter<ReferenceDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) return clazz.cast(converter.convert(bytes, this));
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    @Override
    public boolean requiresGlobalHeap(boolean required) {
        ReferenceType type = getReferenceType();
        return required || type == ReferenceType.DATASET_REGION1 || type == ReferenceType.DATASET_REGION2;
    }

    public String toString(byte[] bytes) {
        if (bytes.length != size) throw new IllegalArgumentException("Byte array length mismatch");
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) sb.append(String.format("%02X", b));
        return "Reference[" + getReferenceType().description + "]=" + sb;
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
    public int getSizeMessageData() {
        return 0;
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {}

    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {}

    @Override
    public String toString() {
        int version = (0xF & (getClassAndVersion()>>>4));
        return "ReferenceDatatype{size=" + size
                + ", version=" + version
                + (version == 4 ? ", encoding=" + getEncoding() : "")
                + ", type=" + getReferenceType().description + "}";
    }

    private String getEncoding() {
        return classBitField.get(4) ? "1" : "0";
    }

    @Override
    public int getClassAndVersion() {
        return classAndVersion;
    }

    @Override
    public int getSize() {
        return size;
    }
}