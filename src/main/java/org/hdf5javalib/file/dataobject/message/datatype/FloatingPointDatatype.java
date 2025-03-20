package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfFloatPoint;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

@Getter
public class FloatingPointDatatype implements HdfDatatype {
    private final byte classAndVersion;
    private final BitSet classBitField;
    private final int size;
    private final short bitOffset;
    private final short bitPrecision;
    private final byte exponentLocation;
    private final byte exponentSize;
    private final byte mantissaLocation;
    private final byte mantissaSize;
    private final int exponentBias;
    // In your HdfDataType class
    private static final Map<Class<?>, HdfConverter<FloatingPointDatatype, ?>> CONVERTERS = new HashMap<>();
    static {
        CONVERTERS.put(Double.class, (bytes, dt) -> dt.toDouble(bytes));
        CONVERTERS.put(Float.class, (bytes, dt) -> dt.toFloat(bytes));
        CONVERTERS.put(String.class, (bytes, dt) -> dt.toString(bytes));
        CONVERTERS.put(HdfFloatPoint.class, HdfFloatPoint::new);
        CONVERTERS.put(HdfData.class, HdfFloatPoint::new);
    }

    public FloatingPointDatatype(byte classAndVersion, BitSet classBitField, int size, short bitOffset, short bitPrecision, byte exponentLocation, byte exponentSize, byte mantissaLocation, byte mantissaSize, int exponentBias) {
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
    }


    public static FloatingPointDatatype parseFloatingPointType(byte version, BitSet classBitField, int size, ByteBuffer buffer) {
        short bitOffset = buffer.getShort();
        short bitPrecision = buffer.getShort();
        byte exponentLocation = buffer.get();
        byte exponentSize = buffer.get();
        byte mantissaLocation = buffer.get();
        byte mantissaSize = buffer.get();
        int exponentBias = buffer.getInt();
        return new FloatingPointDatatype(version, classBitField, size,bitOffset, bitPrecision, exponentLocation, exponentSize, mantissaLocation, mantissaSize, exponentBias);
    }

    public static BitSet createClassBitField(boolean bigEndian) {
        BitSet classBitField = new BitSet();
        if (bigEndian) classBitField.set(0);
        return classBitField;
    }

    public static byte createClassAndVersion() {
        return 0x11;
    }

    @Override
    public DatatypeClass getDatatypeClass() {
        return DatatypeClass.FLOAT;
    }

    @Override
    public short getSizeMessageData() {
        return (short) size;
    }

    // Public method to add user-defined converters
    public static <T> void addConverter(Class<T> clazz, HdfConverter<FloatingPointDatatype, T> converter) {
        CONVERTERS.put(clazz, converter);
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        @SuppressWarnings("unchecked")
        HdfConverter<FloatingPointDatatype, T> converter = (HdfConverter<FloatingPointDatatype, T>) CONVERTERS.get(clazz);
        if (converter != null) {
            return clazz.cast(converter.convert(bytes, this));
        }
        for (Map.Entry<Class<?>, HdfConverter<FloatingPointDatatype, ?>> entry : CONVERTERS.entrySet()) {
            if (entry.getKey().isAssignableFrom(clazz)) {
                return clazz.cast(entry.getValue().convert(bytes, this));
            }
        }
        throw new UnsupportedOperationException("Unknown type: " + clazz);
    }

    // Convert buffer to Float
    public Float toFloat(byte[] bytes) {
        double value = toDoubleValue(bytes);
        return (float) value; // Cast to float, may lose precision if size > 4 bytes
    }

    // Convert buffer to Double
    public Double toDouble(byte[] bytes) {
        return toDoubleValue(bytes);
    }

    private double toDoubleValue(byte[] buffer) {
        if (buffer.length != size) {
            throw new IllegalArgumentException("Buffer size (" + buffer.length + ") must match datatype size (" + size + ")");
        }

        // Determine byte order from classBitField
        ByteOrder order = getByteOrder();
        ByteBuffer bb = ByteBuffer.wrap(buffer).order(order);

        // Convert buffer to long (up to 64 bits) for bit manipulation
        long bits = 0;
        if (size <= 4) {
            bits = bb.getInt() & 0xFFFFFFFFL; // 32-bit unsigned
        } else if (size <= 8) {
            bits = bb.getLong();
        } else {
            throw new UnsupportedOperationException("Size > 8 bytes not supported");
        }

        // Shift bits to align with datatype.getBitOffset()
        bits >>>= bitOffset; // Unsigned right shift to discard lower bits

        // Extract sign bit (from signLocation in classBitField bits 8-15)
        int signLocation = getSignLocation();
        long signMask = 1L << signLocation;
        int sign = (bits & signMask) != 0 ? -1 : 1;

        // Extract exponent
        long exponentMask = (1L << exponentSize) - 1;
        long rawExponent = (bits >>> exponentLocation) & exponentMask;
        int exponent = (int) (rawExponent - exponentBias);

        // Extract mantissa
        long mantissaMask = (1L << mantissaSize) - 1;
        long mantissa = (bits >>> mantissaLocation) & mantissaMask;

        // Assume normalized number (implied leading 1, common in HDF5 float spec)
        double mantissaValue = 1.0 + (double) mantissa / (1L << mantissaSize); // Normalize mantissa

        // Combine: sign * mantissa * 2^exponent
        double value = sign * mantissaValue * Math.pow(2, exponent);

        return value;
    }

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

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.putShort(bitOffset);
        buffer.putShort(bitPrecision);
        buffer.put(exponentLocation);
        buffer.put(exponentSize);
        buffer.put(mantissaLocation);
        buffer.put(mantissaSize);
        buffer.putInt(exponentBias);
    }

    @Override
    public void setGlobalHeap(HdfGlobalHeap grok) {}

    @Override
    public String toString(byte[] bytes) {
        return toDouble(bytes).toString();
    }

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

        // Enum for byte order
        public enum ByteOrder {
            LITTLE_ENDIAN(0b00),  // 00
            BIG_ENDIAN(0b01),     // 01
            VAX_ENDIAN(0b11);     // 11 (10 is reserved)

            private final int value;
            ByteOrder(int value) { this.value = value; }
            public int getValue() { return value; }
        }

        // Enum for mantissa normalization
        public enum MantissaNormalization {
            NONE(0),           // 00
            ALWAYS_SET(1),     // 01
            IMPLIED_SET(2),    // 10
            RESERVED(3);       // 11

            private final int value;
            MantissaNormalization(int value) { this.value = value; }
            public int getValue() { return value; }
        }

        /**
         * Creates a BitSet from the given HDF5 floating-point datatype properties.
         * @param byteOrder Byte order (little-endian, big-endian, VAX-endian)
         * @param lowPad Padding for low bits (true = 1, false = 0)
         * @param highPad Padding for high bits
         * @param internalPad Padding for internal bits
         * @param mantissaNorm Mantissa normalization type
         * @param signLocation Sign bit position (0–255 for 8 bits)
         * @return BitSet representing the 24-bit class bit field
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
         * Gets the ByteOrder from a BitSet.
         */
        public static ByteOrder getByteOrder(BitSet bitSet) {
            int value = (bitSet.get(BYTE_ORDER_HIGH) ? 0b10 : 0) |
                    (bitSet.get(BYTE_ORDER_LOW) ? 0b01 : 0);
            switch (value) {
                case 0b00: return ByteOrder.LITTLE_ENDIAN;
                case 0b01: return ByteOrder.BIG_ENDIAN;
                case 0b11: return ByteOrder.VAX_ENDIAN;
                default: throw new IllegalStateException("Reserved byte order value: " + value);
            }
        }

        /**
         * Gets the low bits padding type from a BitSet.
         */
        public static boolean getLowPad(BitSet bitSet) {
            return bitSet.get(PADDING_LOW);
        }

        /**
         * Gets the high bits padding type from a BitSet.
         */
        public static boolean getHighPad(BitSet bitSet) {
            return bitSet.get(PADDING_HIGH);
        }

        /**
         * Gets the internal bits padding type from a BitSet.
         */
        public static boolean getInternalPad(BitSet bitSet) {
            return bitSet.get(PADDING_INTERNAL);
        }

        /**
         * Gets the MantissaNormalization from a BitSet.
         */
        public static MantissaNormalization getMantissaNormalization(BitSet bitSet) {
            int value = (bitSet.get(MANTISSA_NORM_HIGH) ? 0b10 : 0) |
                    (bitSet.get(MANTISSA_NORM_LOW) ? 0b01 : 0);
            switch (value) {
                case 0: return MantissaNormalization.NONE;
                case 1: return MantissaNormalization.ALWAYS_SET;
                case 2: return MantissaNormalization.IMPLIED_SET;
                case 3: return MantissaNormalization.RESERVED;
                default: throw new IllegalStateException("Invalid mantissa norm value: " + value);
            }
        }

        /**
         * Gets the sign location from a BitSet (bits 8–15).
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

        // Example usage
        public static void main(String[] args) {
            // Create a BitSet matching your example: 20 1F 00
            BitSet bitSet = createBitSet(
                    ByteOrder.LITTLE_ENDIAN, // 00 at bits 6, 0
                    false, false, false,     // 000 at bits 1–3
                    MantissaNormalization.IMPLIED_SET, // 10 at bits 4–5
                    31                       // 00011111 at bits 8–15
            );

            // Convert to hex for verification
            byte[] bytes = bitSet.toByteArray();
            StringBuilder hex = new StringBuilder();
            for (int i = 0; i < 3; i++) {
                hex.append(String.format("%02X ", i < bytes.length ? bytes[i] & 0xFF : 0));
            }
            System.out.println("BitSet as hex: " + hex); // Should print "20 1F 00 "

            // Extract values
            System.out.println("Byte Order: " + getByteOrder(bitSet));
            System.out.println("Low Pad: " + getLowPad(bitSet));
            System.out.println("High Pad: " + getHighPad(bitSet));
            System.out.println("Internal Pad: " + getInternalPad(bitSet));
            System.out.println("Mantissa Norm: " + getMantissaNormalization(bitSet));
            System.out.println("Sign Location: " + getSignLocation(bitSet));
        }
    }
}
