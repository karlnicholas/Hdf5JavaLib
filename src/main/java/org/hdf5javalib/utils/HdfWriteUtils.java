package org.hdf5javalib.utils;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfFloatPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.file.dataobject.message.datatype.*;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hdf5javalib.utils.HdfReadUtils.reverseBytesInPlace;

public class HdfWriteUtils {

    /**
     * Writes an `HdfFixedPoint` value to the `ByteBuffer' accounting for endian-ness.
     * If undefined, fills with 0xFF.
     */
    public static void writeFixedPointToBuffer(ByteBuffer buffer, HdfFixedPoint value) {
        int size = value.getDatatype().getSize();
        byte[] bytesToWrite = new byte[size];

        if (value.isUndefined()) {
            Arrays.fill(bytesToWrite, (byte) 0xFF); // Undefined value → fill with 0xFF
        } else {
            byte[] valueBytes = value.getBytes();
            int copySize = Math.min(valueBytes.length, size);

            // Store in **little-endian format** by reversing byte order
            if ( value.getDatatype().isBigEndian() && buffer.order() == ByteOrder.BIG_ENDIAN
            || !value.getDatatype().isBigEndian() && buffer.order() == ByteOrder.LITTLE_ENDIAN) {
                for (int i = 0; i < copySize; i++) {
                    bytesToWrite[i] = valueBytes[i];
                }
            } else {
                for (int i = 0; i < copySize; i++) {
                    bytesToWrite[i] = valueBytes[copySize - 1 - i];
                }
            }
        }

        buffer.put(bytesToWrite);
    }

    public static void writeFixedPointToBuffer(ByteBuffer buffer, BigInteger value, int size) {
//        short size = value.getSizeMessageData();
        byte[] bytesToWrite = new byte[size];

//        if (value.isUndefined()) {
//            Arrays.fill(bytesToWrite, (byte) 0xFF); // Undefined value → fill with 0xFF
//        } else {
            byte[] valueBytes = value.toByteArray();
            int copySize = Math.min(valueBytes.length, size);

            // Store in **little-endian format** by reversing byte order
            if ( buffer.order() == ByteOrder.BIG_ENDIAN ) {
                for (int i = 0; i < copySize; i++) {
                    bytesToWrite[i] = valueBytes[i];
                }
            } else {
                for (int i = 0; i < copySize; i++) {
                    bytesToWrite[i] = valueBytes[copySize - 1 - i];
                }
            }
//        }

        buffer.put(bytesToWrite);
    }

    public static void writeBigIntegerAsHdfFixedPoint(BigInteger value, FixedPointDatatype datatype, ByteBuffer buffer) {
        HdfFixedPoint fixedPoint = new HdfFixedPoint(value.toByteArray(), datatype);
        fixedPoint.writeValueToByteBuffer(buffer);
    }

    public static void writeBigDecimalAsHdfFixedPoint(BigDecimal value, FixedPointDatatype datatype, ByteBuffer buffer) {
        HdfFixedPoint fixedPoint = new HdfFixedPoint(value.unscaledValue().toByteArray(), datatype);
        fixedPoint.writeValueToByteBuffer(buffer);
    }

    public static <T> void writeCompoundTypeToBuffer(T instance, CompoundDatatype compoundType, ByteBuffer buffer, Class<T> dataClass) {
        Map<String, Field> nameToFieldMap = Arrays.stream(dataClass.getDeclaredFields())
                .collect(Collectors.toMap(Field::getName, f -> f));
        Map<String, CompoundMemberDatatype> nameToMemberMap = compoundType.getMembers().stream()
                .collect(Collectors.toMap(CompoundMemberDatatype::getName, compoundMember -> compoundMember));

        try {
            for (CompoundMemberDatatype member : nameToMemberMap.values()) {
                Field field = nameToFieldMap.get(member.getName());
                if (field == null) {
                    throw new NoSuchFieldException(member.getName());
                }
                field.setAccessible(true);
                Object value = field.get(instance);
                if (value == null) {
                    throw new IllegalArgumentException("Null value for field: " + member.getName());
                }

                Class<?> fieldType = field.getType();
                HdfDatatype memberType = member.getType();

                if (memberType instanceof StringDatatype membertyped) {
                    if (fieldType != String.class) {
                        throw new IllegalArgumentException("Field " + member.getName() + " must be String for StringDatatype, got " + fieldType.getName());
                    }
                    Charset charset = membertyped.getCharacterSet() == StringDatatype.CharacterSet.ASCII
                            ? StandardCharsets.US_ASCII : StandardCharsets.UTF_8;
                    byte[] bytes = ((String) value).getBytes(charset);
                    HdfString hdfString = new HdfString(bytes, membertyped);
                    buffer.position(member.getOffset());
                    hdfString.writeValueToByteBuffer(buffer);
                } else if (memberType instanceof VariableLengthDatatype membertyped) {
                    if (fieldType != String.class) {
                        throw new IllegalArgumentException("Field " + member.getName() + " must be String for VariableLengthDatatype, got " + fieldType.getName());
                    }
                    Charset charset = membertyped.getCharacterSet() == VariableLengthDatatype.CharacterSet.ASCII
                            ? StandardCharsets.US_ASCII : StandardCharsets.UTF_8;
                    byte[] bytes = ((String) value).getBytes(charset);
                    HdfGlobalHeap hdfGlobalHeap = membertyped.getGlobalHeap();
                    byte[] varInstanceBytes = hdfGlobalHeap.addToHeap(bytes);

                    HdfVariableLength hdfVariableLength = new HdfVariableLength(varInstanceBytes, membertyped);
                    buffer.position(member.getOffset());
                    hdfVariableLength.writeValueToByteBuffer(buffer);
                } else if (memberType instanceof FixedPointDatatype membertyped) {
                    byte[] bytes = toFixedPointBytes(value, membertyped, fieldType);
                    HdfFixedPoint hdfFixedPoint = new HdfFixedPoint(bytes, membertyped);
                    buffer.position(member.getOffset());
                    hdfFixedPoint.writeValueToByteBuffer(buffer);
                } else if (memberType instanceof FloatingPointDatatype membertyped) {
                    byte[] bytes = toFloatPointBytes(value, membertyped, fieldType);
                    HdfFloatPoint hdfFloatPoint = new HdfFloatPoint(bytes, membertyped);
                    buffer.position(member.getOffset());
                    hdfFloatPoint.writeValueToByteBuffer(buffer);
                } else {
                    throw new UnsupportedOperationException("Unsupported member datatype: " + memberType.getClass().getName());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // Convert field value to byte array for FixedPointDatatype
    private static byte[] toFixedPointBytes(Object value, FixedPointDatatype datatype, Class<?> fieldType) {
        int size = datatype.getSize();
        ByteBuffer temp = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);

        if (fieldType == Byte.class || fieldType == byte.class) {
            temp.put((Byte) value);
        } else if (fieldType == Short.class || fieldType == short.class) {
            temp.putShort((Short) value);
        } else if (fieldType == Integer.class || fieldType == int.class) {
            temp.putInt((Integer) value);
        } else if (fieldType == Long.class || fieldType == long.class) {
            temp.putLong((Long) value);
        } else if (fieldType == BigInteger.class) {
            byte[] bytes = ((BigInteger) value).toByteArray();
            reverseBytesInPlace(bytes);
            if (bytes.length > size) {
                bytes = trimTrailingZeros(bytes);
            }
            temp.put(bytes, 0, bytes.length);
        } else if (fieldType == BigDecimal.class) {
            BigDecimal bdValue = (BigDecimal) value;
            // Scale by 2^bitOffset to match fixed-point representation
            BigDecimal scaledValue = bdValue.multiply(BigDecimal.valueOf(1L << datatype.getBitOffset()));
            BigInteger intValue = scaledValue.toBigInteger();

            // Apply bitPrecision mask if necessary
            short bitPrecision = datatype.getBitPrecision();
            if (bitPrecision > 0 && bitPrecision < size * 8) {
                BigInteger mask = BigInteger.ONE.shiftLeft(bitPrecision).subtract(BigInteger.ONE);
                intValue = intValue.and(mask);
            }

            byte[] bytes = intValue.toByteArray();
            reverseBytesInPlace(bytes);
            if (bytes.length > size) {
                bytes = trimTrailingZeros(bytes);
            }
            temp.put(bytes, 0, bytes.length);
        } else {
            throw new IllegalArgumentException("Field " + fieldType.getName() + " not supported for FixedPointDatatype");
        }

        byte[] bytes = trimTrailingZeros(temp.array());
        if (bytes.length > size) {
            throw new IllegalArgumentException("Value " + value + " too large for " + size + " bytes");
        }
        if (datatype.isBigEndian()) {
            reverseBytesInPlace(bytes);
        }
        byte[] result = new byte[size];
        System.arraycopy(temp.array(), 0, result, 0, Math.min(size, temp.position()));
        return result;
    }

    // Convert field value to byte array for FloatingPointDatatype
    private static byte[] toFloatPointBytes(Object value, FloatingPointDatatype datatype, Class<?> fieldType) {
        int size = datatype.getSize();
        ByteBuffer temp = ByteBuffer.allocate(size).order(datatype.getByteOrder());

        if (fieldType == Float.class || fieldType == float.class) {
            if (size != 4) throw new IllegalArgumentException("Float requires 4 bytes, got " + size);
            temp.putFloat((Float) value);
        } else if (fieldType == Double.class || fieldType == double.class) {
            if (size != 8) throw new IllegalArgumentException("Double requires 8 bytes, got " + size);
            temp.putDouble((Double) value);
        } else {
            throw new IllegalArgumentException("Field " + fieldType.getName() + " not supported for FloatingPointDatatype");
        }

        return temp.array();
    }

    public static byte[] trimLeadingZeros(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return bytes;

        int start = 0;
        while (start < bytes.length && bytes[start] == 0) {
            start++;
        }

        if (start == bytes.length) return new byte[]{0}; // All zeros case
        byte[] result = new byte[bytes.length - start];
        System.arraycopy(bytes, start, result, 0, result.length);
        return result;
    }

    public static byte[] trimTrailingZeros(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return bytes;

        int end = bytes.length - 1;
        while (end >= 0 && bytes[end] == 0) {
            end--;
        }

        if (end < 0) return new byte[]{0}; // All zeros case
        byte[] result = new byte[end + 1];
        System.arraycopy(bytes, 0, result, 0, result.length);
        return result;
    }

}
