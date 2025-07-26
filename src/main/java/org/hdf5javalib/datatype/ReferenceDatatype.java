package org.hdf5javalib.datatype;

import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfReference;
import org.hdf5javalib.dataclass.reference.HdfAttributeReference;
import org.hdf5javalib.dataclass.reference.HdfDatasetRegionReference;
import org.hdf5javalib.dataclass.reference.HdfObjectReference;
import org.hdf5javalib.dataclass.reference.HdfReferenceInstance;
import org.hdf5javalib.hdffile.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.utils.HdfDataHolder;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * HDF5 Reference Datatype (Class 7) for objects, dataset regions, or attributes.
 */
public class ReferenceDatatype implements Datatype {
    private final int classAndVersion;
    private final BitSet classBitField;
    private final int size;
    private final HdfDataFile dataFile;

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

        public int getValue() {
            return value;
        }

        public String getDescription() {
            return description;
        }

        public static ReferenceType fromValue(int value) {
            for (ReferenceType type : values()) {
                if (type.value == value) return type;
            }
            throw new IllegalArgumentException("Unknown reference type: " + value);
        }
    }

    private static final Map<Class<?>, DatatypeConverter<ReferenceDatatype, ?>> CONVERTERS = new HashMap<>();

    static {
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfReference.class, HdfReference::new);
        CONVERTERS.put(HdfData.class, HdfReference::new);
        CONVERTERS.put(HdfReferenceInstance.class, (bytes, dt) -> dt.toHdfReferenceInstance(bytes, dt));
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes.clone());
    }

    private HdfReferenceInstance toHdfReferenceInstance(byte[] bytes, ReferenceDatatype dt) {
        return switch (getReferenceType(classBitField)) {
            case OBJECT1 -> new HdfObjectReference(bytes, dt, false);
            case DATASET_REGION1 -> new HdfDatasetRegionReference(bytes, dt, false);
            case OBJECT2 -> new HdfObjectReference(bytes, dt, (bytes[1] & 0x01) == 1);
            case DATASET_REGION2 -> new HdfDatasetRegionReference(bytes, dt, (bytes[1] & 0x01) == 1);
            case ATTR -> new HdfAttributeReference(bytes, dt, (bytes[1] & 0x01) == 1);
        };
    }

    public ReferenceDatatype(int classAndVersion, BitSet classBitField, int size, HdfDataFile dataFile) {
        int typeValue = getTypeValue(classBitField);
        if (typeValue > 4) throw new IllegalArgumentException("Invalid reference type: " + typeValue);

        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.dataFile = dataFile;
    }

    public static ReferenceDatatype parseReferenceDatatype(int classAndVersion, BitSet classBitField, int size, ByteBuffer ignoredBuffer, HdfDataFile dataFile) {
        return new ReferenceDatatype(classAndVersion, classBitField, size, dataFile);
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

    //    public static int getTypeValue(BitSet classBitField) {
//        int value = 0;
//        for (int i = 0; i < 4; i++) if (classBitField.get(i)) value |= 1 << i;
//        return value;
//    }
//
    public static ReferenceType getReferenceType(BitSet classBitField) {
        return ReferenceType.fromValue(getTypeValue(classBitField));
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
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        @SuppressWarnings("unchecked")
        DatatypeConverter<ReferenceDatatype, T> converter = (DatatypeConverter<ReferenceDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, DatatypeConverter<ReferenceDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    /**
     * Retrieves the reference type value from the class bit field.
     *
     * @param classBitField the BitSet indicating the reference type
     * @return the numeric value of the reference type
     */
    public static int getTypeValue(BitSet classBitField) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            if (classBitField.get(i)) {
                value |= 1 << i;
            }
        }
        return value;
    }

    public String toString(byte[] bytes) {
//        if (bytes.length != size) throw new IllegalArgumentException("Byte array length mismatch");
//        StringBuilder sb = new StringBuilder();
//        for (byte b : bytes) sb.append(String.format("%02X", b));
//        return "Reference[" + getReferenceType(classBitField).description + "]=" + sb;
        HdfReferenceInstance referenceInstance = getInstance(HdfReferenceInstance.class, bytes);
        HdfDataHolder data = referenceInstance.getData();
        if ( data.isScalar()) {
            return data.getScalar().toString();
        } else {
            return HdfDataHolder.arrayToString(data.getArray(), data.getDimensions());
        }
    }

    @Override
    public HdfDataFile getDataFile() {
        return dataFile;
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
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
    }

    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
    }

    @Override
    public String toString() {
        int version = (0xF & (getClassAndVersion() >>> 4));
        return "ReferenceDatatype{size=" + size
                + ", version=" + version
                + (version == 4 ? ", encoding=" + getEncoding() : "")
                + ", type=" + getReferenceType(classBitField).description + "}";
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

    @Override
    public List<ReferenceDatatype> getReferenceInstances() {
        return Collections.singletonList(this);
    }
}