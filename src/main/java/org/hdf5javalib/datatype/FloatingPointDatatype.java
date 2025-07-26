package org.hdf5javalib.datatype;

import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfFloatPoint;
import org.hdf5javalib.hdffile.infrastructure.HdfGlobalHeap;
import org.hdf5javalib.hdfjava.HdfDataFile;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents an HDF5 Floating-Point Datatype as defined in the HDF5 specification.
 * <p>
 * The {@code FloatingPointDatatype} class models a floating-point number in HDF5, supporting parsing
 * from a {@link ByteBuffer} and conversion to Java types such as {@link Double}, {@link Float},
 * or {@link HdfFloatPoint}. It handles byte order, exponent, mantissa, and sign configuration as per
 * the HDF5 floating-point datatype (class 1).
 * </p>
 *
 * @see Datatype
 * @see HdfGlobalHeap
 */
public class FloatingPointDatatype implements Datatype {
    /**
     * The class and version information for the datatype (class 1, version 1).
     */
    private final int classAndVersion;
    /**
     * A BitSet containing class-specific bit field information (byte order, padding, sign location).
     */
    private final BitSet classBitField;
    /**
     * The total size of the floating-point datatype in bytes.
     */
    private final int size;
    /**
     * The bit offset of the first significant bit.
     */
    private final int bitOffset;
    /**
     * The total number of bits of precision.
     */
    private final int bitPrecision;
    /**
     * The bit position of the exponent.
     */
    private final int exponentLocation;
    /**
     * The size of the exponent in bits.
     */
    private final int exponentSize;
    /**
     * The bit position of the mantissa.
     */
    private final int mantissaLocation;
    /**
     * The size of the mantissa in bits.
     */
    private final int mantissaSize;
    /**
     * The exponent bias value.
     */
    private final int exponentBias;
    private final HdfDataFile dataFile;

    /**
     * Map of converters for transforming byte data to specific Java types.
     */
    private static final Map<Class<?>, DatatypeConverter<FloatingPointDatatype, ?>> CONVERTERS = new HashMap<>();

    static {
        CONVERTERS.put(Double.class, (bytes, dt) -> dt.toDouble(bytes));
        CONVERTERS.put(Float.class, (bytes, dt) -> dt.toFloat(bytes));
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfFloatPoint.class, HdfFloatPoint::new);
        CONVERTERS.put(HdfData.class, HdfFloatPoint::new);
        CONVERTERS.put(byte[].class, (bytes, dt) -> bytes);
    }

    /**
     * Constructs a FloatingPointDatatype representing an HDF5 floating-point datatype.
     *
     * @param classAndVersion  the class and version information for the datatype
     * @param classBitField    a BitSet containing class-specific bit field information
     * @param size             the total size of the datatype in bytes
     * @param bitOffset        the bit offset of the first significant bit
     * @param bitPrecision     the total number of bits of precision
     * @param exponentLocation the bit position of the exponent
     * @param exponentSize     the size of the exponent in bits
     * @param mantissaLocation the bit position of the mantissa
     * @param mantissaSize     the size of the mantissa in bits
     * @param exponentBias     the exponent bias value
     * @param dataFile
     */
    public FloatingPointDatatype(
            int classAndVersion,
            BitSet classBitField,
            int size,
            int bitOffset,
            int bitPrecision,
            int exponentLocation,
            int exponentSize,
            int mantissaLocation,
            int mantissaSize, int exponentBias, HdfDataFile dataFile
    ) {
        this.classAndVersion = classAndVersion;
        this.classBitField = classBitField;
        this.size = size;
        this.bitOffset = bitOffset;
        this.bitPrecision = bitPrecision;
        this.exponentLocation = exponentLocation;
        this.exponentSize = exponentSize;
        this.mantissaLocation = mantissaLocation;
        this.mantissaSize = mantissaSize;
        this.exponentBias = exponentBias;
        this.dataFile = dataFile;
    }

    /**
     * Parses an HDF5 floating-point datatype from a ByteBuffer as per the HDF5 specification.
     *
     * @param version       the version byte of the datatype
     * @param classBitField the BitSet containing class-specific bit field information
     * @param size          the total size of the datatype in bytes
     * @param buffer        the ByteBuffer containing the datatype definition
     * @return a new FloatingPointDatatype instance parsed from the buffer
     */
    public static FloatingPointDatatype parseFloatingPointType(int version, BitSet classBitField, int size, ByteBuffer buffer, HdfDataFile dataFile) {
        int bitOffset = Short.toUnsignedInt(buffer.getShort());
        int bitPrecision = Short.toUnsignedInt(buffer.getShort());
        int exponentLocation = Byte.toUnsignedInt(buffer.get());
        int exponentSize = Byte.toUnsignedInt(buffer.get());
        int mantissaLocation = Byte.toUnsignedInt(buffer.get());
        int mantissaSize = Byte.toUnsignedInt(buffer.get());
        int exponentBias = buffer.getInt();
        return new FloatingPointDatatype(version, classBitField, size, bitOffset, bitPrecision, exponentLocation, exponentSize, mantissaLocation, mantissaSize, exponentBias, dataFile);
    }

    /**
     * Creates a BitSet representing the class bit field for an HDF5 floating-point datatype.
     *
     * @param bigEndian true for big-endian byte order, false for little-endian
     * @return a BitSet encoding the byte order
     */
    public static BitSet createClassBitField(boolean bigEndian) {
        BitSet classBitField = new BitSet();
        if (bigEndian) classBitField.set(0);
        return classBitField;
    }

    /**
     * Creates a fixed class and version byte for an HDF5 floating-point datatype.
     *
     * @return a byte representing class 1 and version 1, as defined by the HDF5 specification
     */
    @SuppressWarnings("SameReturnValue")
    public static byte createClassAndVersion() {
        return 0x11;
    }

    /**
     * Returns the datatype class for this floating-point datatype.
     *
     * @return DatatypeClass.FLOAT, indicating an HDF5 floating-point datatype
     */
    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.FLOAT;
    }

    /**
     * Returns the size of the datatype message data.
     *
     * @return the size of the message data in bytes, as a short
     */
    @Override
    public int getSizeMessageData() {
        return (short) (12 + 8);
    }

    /**
     * Registers a converter for transforming FloatingPointDatatype data to a specific Java type.
     *
     * @param <T>       the type of the class to be converted
     * @param clazz     the Class object representing the target type
     * @param converter the DatatypeConverter for converting between FloatingPointDatatype and the target type
     */
    public static <T> void addConverter(Class<T> clazz, DatatypeConverter<FloatingPointDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    /**
     * Converts byte data to an instance of the specified class using registered converters.
     *
     * @param <T>   the type of the instance to be created
     * @param clazz the Class object representing the target type
     * @param bytes the byte array containing the data
     * @return an instance of type T created from the byte array
     * @throws UnsupportedOperationException if no suitable converter is found
     */
    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        @SuppressWarnings("unchecked")
        DatatypeConverter<FloatingPointDatatype, T> converter = (DatatypeConverter<FloatingPointDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, DatatypeConverter<FloatingPointDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    /**
     * Indicates whether a global heap is required for this datatype.
     *
     * @param required true if the global heap is required, false otherwise
     * @return false, as FloatingPointDatatype does not require a global heap
     */
    @Override
    public boolean requiresGlobalHeap(boolean required) {
        return required || false;
    }

    /**
     * Converts the byte array to a Float value.
     *
     * @param bytes the byte array to convert
     * @return the Float value
     * @throws IllegalArgumentException      if the byte array size does not match the datatype size
     * @throws UnsupportedOperationException if the datatype size is not supported
     */
    public Float toFloat(byte[] bytes) {
        double value = toDoubleValue(bytes);
        return (float) value; // Cast to float, may lose precision if size > 4 bytes
    }

    /**
     * Converts the byte array to a Double value.
     *
     * @param bytes the byte array to convert
     * @return the Double value
     * @throws IllegalArgumentException      if the byte array size does not match the datatype size
     * @throws UnsupportedOperationException if the datatype size is not supported
     */
    public Double toDouble(byte[] bytes) {
        return toDoubleValue(bytes);
    }

    public double toDoubleValue(byte[] rawBuffer) {
        // 1. Read 'size' bytes into 'overallBits' assuming LITTLE_ENDIAN.
        long overallBits = 0L;
        // This loop will throw ArrayIndexOutOfBoundsException if rawBuffer.length < this.size
        for (int i = 0; i < this.size; i++) {
            overallBits |= ((long) (rawBuffer[i] & 0xFF)) << (i * 8);
        }

        // 2. Apply bitOffset and mask to bitPrecision.
        long workingBits = overallBits >>> this.bitOffset;

        if (this.bitPrecision < 64 && this.bitPrecision > 0) { // bitPrecision=0 would be 1L<<0 -1 = 0 mask
            long precisionMask = (1L << this.bitPrecision) - 1;
            workingBits &= precisionMask;
        } else if (this.bitPrecision == 0) {
            workingBits = 0; // No bits of precision means value is effectively zero before interpretation
        }
        // If bitPrecision >= 64, workingBits uses all 64 bits of the long.

        // 3. Extract components.
        int sign = 1;
        if (this.bitPrecision > 0) { // Avoid 1L << -1 if bitPrecision is 0
            long signBitMaskInWindow = 1L << (this.bitPrecision - 1);
            if ((workingBits & signBitMaskInWindow) != 0) {
                sign = -1;
            }
        }

        long rawExponent = 0;
        if (this.exponentSize > 0) {
            long exponentFieldMask = (this.exponentSize >= 64) ? -1L : ((1L << this.exponentSize) - 1);
            rawExponent = (workingBits >>> this.exponentLocation) & exponentFieldMask;
        }

        long rawMantissa = 0;
        if (this.mantissaSize > 0) {
            long mantissaFieldMask = (this.mantissaSize >= 64) ? -1L : ((1L << this.mantissaSize) - 1);
            rawMantissa = (workingBits >>> this.mantissaLocation) & mantissaFieldMask;
        }

        // --- Numerical Interpretation ---
        boolean allExponentBitsSet = this.exponentSize > 0 && rawExponent == ((1L << this.exponentSize) - 1);

        if (allExponentBitsSet) {
            if (rawMantissa != 0) return Double.NaN;
            return (sign == 1) ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY;
        }

        int exponent = (int) (rawExponent - this.exponentBias);

        boolean isDenormalizedSource = (this.exponentSize > 0 && rawExponent == 0 && rawMantissa != 0);
        boolean isTrueZero = ((this.exponentSize == 0 || rawExponent == 0) &&
                (this.mantissaSize == 0 || rawMantissa == 0));


        if (isTrueZero) return sign * 0.0;

        double mantissaValue;
        if (isDenormalizedSource) {
            mantissaValue = (this.mantissaSize == 0) ? 0.0 : (double) rawMantissa / (1L << this.mantissaSize);
            exponent = 1 - this.exponentBias;
        } else {
            // Normalized number: implicit leading 1
            // If mantissaSize is 0, this implies 1.0 for the fractional part.
            // If mantissaSize > 0, it's 1.fractional_part
            mantissaValue = (this.mantissaSize == 0 && this.exponentSize == 0 && this.bitPrecision > 0 && rawMantissa == 0 && rawExponent == 0) ? 0.0 : 1.0; // if only sign bit, it's +/- value based on exp=0, man=0
            if (this.mantissaSize > 0) {
                mantissaValue = 1.0 + (double) rawMantissa / (1L << this.mantissaSize);
            } else if (this.exponentSize == 0 && this.bitPrecision > 0 && rawMantissa == 0 && rawExponent == 0) {
                // If there's only a sign bit defined by bitPrecision=1, expSize=0, manSize=0.
                // e.g. descriptor (1,0,1, 0,0,0,0, 0) -> S * 1.0 * 2^(0 - 0) -> S * 1.0
                // This case needs careful thought if mantissaSize is truly 0.
                // Standard interpretation is that if a number is not denormalized and not zero,
                // it has an implicit 1. So mantissaValue should be 1.0.
                mantissaValue = 1.0;
            }

        }
        return sign * mantissaValue * Math.pow(2, exponent);
    }

    /**
     * Determines the byte order from the class bit field.
     *
     * @return the ByteOrder (LITTLE_ENDIAN, BIG_ENDIAN, or VAX_ENDIAN)
     * @throws IllegalArgumentException      if the byte order is reserved
     * @throws UnsupportedOperationException if VAX-endian is specified
     */
    public ByteOrder getByteOrder() {
        boolean bit0 = classBitField.get(0);
        boolean bit6 = classBitField.get(6);
        if (!bit6 && !bit0) return ByteOrder.LITTLE_ENDIAN;
        if (!bit6 && bit0) return ByteOrder.BIG_ENDIAN;
        if (bit6 && !bit0) throw new IllegalArgumentException("Reserved byte order");
        if (bit6 && bit0) throw new UnsupportedOperationException("VAX-endian not supported");
        return ByteOrder.LITTLE_ENDIAN; // Default, should never reach here
    }

    private int getSignLocation() {
        int signLoc = 0;
        for (int i = 8; i <= 15; i++) {
            if (classBitField.get(i)) {
                signLoc |= (1 << (i - 8));
            }
        }
        return signLoc;
    }

    /**
     * Returns a string representation of this FloatingPointDatatype.
     *
     * @return a string describing the datatype's size, bit offset, precision, exponent, mantissa, and bias
     */
    @Override
    public String toString() {
        return "FloatingPointDatatype{" +
                "size=" + size +
                ", bitOffset=" + bitOffset +
                ", bitPrecision=" + bitPrecision +
                ", exponentLocation=" + exponentLocation +
                ", exponentSize=" + exponentSize +
                ", mantissaLocation=" + mantissaLocation +
                ", mantissaSize=" + mantissaSize +
                ", exponentBias=" + exponentBias +
                '}';
    }

    /**
     * Writes the datatype definition to the provided ByteBuffer.
     *
     * @param buffer the ByteBuffer to write the datatype definition to
     */
    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putShort((short) bitOffset);
        buffer.putShort((short) bitPrecision);
        buffer.put((byte) exponentLocation);
        buffer.put((byte) exponentSize);
        buffer.put((byte) mantissaLocation);
        buffer.put((byte) mantissaSize);
        buffer.putInt(exponentBias);
    }

    /**
     * Sets the global heap for this datatype (no-op for FloatingPointDatatype).
     *
     * @param grok the HdfGlobalHeap to set
     */
    @Override
    public void setGlobalHeap(HdfGlobalHeap grok) {
    }

    /**
     * Converts the byte array to a string representation using Float or Double.
     *
     * @param bytes the byte array to convert
     * @return a string representation of the floating-point value
     * @throws UnsupportedOperationException if the datatype size is not 4 or 8 bytes
     */
    @Override
    public String toString(byte[] bytes) {
        if (size == 4) {
            return toFloat(bytes).toString();
        } else if (size == 8) {
            return toDouble(bytes).toString();
        } else {
            throw new UnsupportedOperationException("Unsupported floating-point size: " + size);
        }
    }

    @Override
    public HdfDataFile getDataFile() {
        return dataFile;
    }

    @Override
    public List<ReferenceDatatype> getReferenceInstances() {
        return List.of();
    }

    /**
     * Represents the class bit field configuration for an HDF5 floating-point datatype.
     */
    public static class ClassBitField {
        // Constants for bit positions
        private static final int BYTE_ORDER_LOW = 0;  // Bit 0
        private static final int PADDING_LOW = 1;     // Bit 1
        private static final int PADDING_HIGH = 2;    // Bit 2
        private static final int PADDING_INTERNAL = 3;// Bit 3
        private static final int MANTISSA_NORM_LOW = 4;  // Bit 4
        private static final int MANTISSA_NORM_HIGH = 5; // Bit 5
        private static final int BYTE_ORDER_HIGH = 6; // Bit 6
        private static final int SIGN_LOCATION_START = 8; // Bits 8–15

        /**
         * Enum for byte order.
         */
        public enum ByteOrder {
            /**
             * Little-endian byte order (bit 0 = 0, bit 6 = 0).
             */
            LITTLE_ENDIAN(0b00),
            /**
             * Big-endian byte order (bit 0 = 1, bit 6 = 0).
             */
            BIG_ENDIAN(0b01),
            /**
             * VAX-endian byte order (bit 0 = 1, bit 6 = 1; not supported).
             */
            VAX_ENDIAN(0b11);

            private final int value;

            ByteOrder(int value) {
                this.value = value;
            }

            /**
             * Returns the numeric value of the byte order.
             *
             * @return the byte order value
             */
            public int getValue() {
                return value;
            }
        }

        /**
         * Enum for mantissa normalization.
         */
        public enum MantissaNormalization {
            /**
             * No mantissa normalization (bits 4-5 = 00).
             */
            NONE(0),
            /**
             * Mantissa always set (bits 4-5 = 01).
             */
            ALWAYS_SET(1),
            /**
             * Mantissa implied set (bits 4-5 = 10).
             */
            IMPLIED_SET(2),
            /**
             * Reserved mantissa normalization (bits 4-5 = 11).
             */
            RESERVED(3);

            private final int value;

            MantissaNormalization(int value) {
                this.value = value;
            }

            /**
             * Returns the numeric value of the mantissa normalization.
             *
             * @return the mantissa normalization value
             */
            public int getValue() {
                return value;
            }
        }

        /**
         * Creates a BitSet from the given HDF5 floating-point datatype properties.
         *
         * @param byteOrder    the byte order (LITTLE_ENDIAN, BIG_ENDIAN, VAX_ENDIAN)
         * @param lowPad       true for low bits padding, false otherwise
         * @param highPad      true for high bits padding, false otherwise
         * @param internalPad  true for internal bits padding, false otherwise
         * @param mantissaNorm the mantissa normalization type
         * @param signLocation the sign bit position (0–255)
         * @return a 24-bit BitSet representing the class bit field
         * @throws IllegalArgumentException if the sign location is not 0–255
         */
        public static BitSet createBitSet(ByteOrder byteOrder, boolean lowPad, boolean highPad,
                                          boolean internalPad, MantissaNormalization mantissaNorm,
                                          int signLocation) {
            BitSet bitSet = new BitSet(24);

            // Set byte order (bits 0 and 6)
            int boValue = byteOrder.getValue();
            bitSet.set(BYTE_ORDER_LOW, (boValue & 0b01) != 0);  // Bit 0
            bitSet.set(BYTE_ORDER_HIGH, (boValue & 0b10) != 0); // Bit 6

            // Set padding types (bits 1–3)
            bitSet.set(PADDING_LOW, lowPad);
            bitSet.set(PADDING_HIGH, highPad);
            bitSet.set(PADDING_INTERNAL, internalPad);

            // Set mantissa normalization (bits 4–5)
            int mnValue = mantissaNorm.getValue();
            bitSet.set(MANTISSA_NORM_LOW, (mnValue & 0b01) != 0);   // Bit 4
            bitSet.set(MANTISSA_NORM_HIGH, (mnValue & 0b10) != 0);  // Bit 5

            // Set sign location (bits 8–15, 8-bit value)
            if (signLocation < 0 || signLocation > 255) {
                throw new IllegalArgumentException("Sign location must be 0–255");
            }
            for (int i = 0; i < 8; i++) {
                bitSet.set(SIGN_LOCATION_START + i, (signLocation & (1 << i)) != 0);
            }

            // Bits 7, 16–23 are reserved and remain 0 by default
            return bitSet;
        }

        /**
         * Retrieves the ByteOrder from a BitSet.
         *
         * @param bitSet the BitSet containing class bit field information
         * @return the ByteOrder (LITTLE_ENDIAN, BIG_ENDIAN, or VAX_ENDIAN)
         * @throws IllegalStateException if the byte order value is reserved
         */
        public static ByteOrder getByteOrder(BitSet bitSet) {
            int value = (bitSet.get(BYTE_ORDER_HIGH) ? 0b10 : 0) |
                    (bitSet.get(BYTE_ORDER_LOW) ? 0b01 : 0);
            return switch (value) {
                case 0b00 -> ByteOrder.LITTLE_ENDIAN;
                case 0b01 -> ByteOrder.BIG_ENDIAN;
                case 0b11 -> ByteOrder.VAX_ENDIAN;
                default -> throw new IllegalStateException("Reserved byte order value: " + value);
            };
        }

        /**
         * Retrieves the low bits padding type from a BitSet.
         *
         * @param bitSet the BitSet containing class bit field information
         * @return true if low padding is enabled, false otherwise
         */
        public static boolean getLowPad(BitSet bitSet) {
            return bitSet.get(PADDING_LOW);
        }

        /**
         * Retrieves the high bits padding type from a BitSet.
         *
         * @param bitSet the BitSet containing class bit field information
         * @return true if high padding is enabled, false otherwise
         */
        public static boolean getHighPad(BitSet bitSet) {
            return bitSet.get(PADDING_HIGH);
        }

        /**
         * Retrieves the internal bits padding type from a BitSet.
         *
         * @param bitSet the BitSet containing class bit field information
         * @return true if internal padding is enabled, false otherwise
         */
        public static boolean getInternalPad(BitSet bitSet) {
            return bitSet.get(PADDING_INTERNAL);
        }

        /**
         * Retrieves the MantissaNormalization from a BitSet.
         *
         * @param bitSet the BitSet containing class bit field information
         * @return the MantissaNormalization type
         * @throws IllegalStateException if the mantissa normalization value is invalid
         */
        public static MantissaNormalization getMantissaNormalization(BitSet bitSet) {
            int value = (bitSet.get(MANTISSA_NORM_HIGH) ? 0b10 : 0) |
                    (bitSet.get(MANTISSA_NORM_LOW) ? 0b01 : 0);
            return switch (value) {
                case 0 -> MantissaNormalization.NONE;
                case 1 -> MantissaNormalization.ALWAYS_SET;
                case 2 -> MantissaNormalization.IMPLIED_SET;
                case 3 -> MantissaNormalization.RESERVED;
                default -> throw new IllegalStateException("Invalid mantissa norm value: " + value);
            };
        }

        /**
         * Retrieves the sign location from a BitSet (bits 8–15).
         *
         * @param bitSet the BitSet containing class bit field information
         * @return the sign bit position (0–255)
         */
        public static int getSignLocation(BitSet bitSet) {
            int signLocation = 0;
            for (int i = 0; i < 8; i++) {
                if (bitSet.get(SIGN_LOCATION_START + i)) {
                    signLocation |= (1 << i);
                }
            }
            return signLocation;
        }
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
     * Returns the total size of the floating-point datatype in bytes.
     *
     * @return the size in bytes
     */
    @Override
    public int getSize() {
        return size;
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
}