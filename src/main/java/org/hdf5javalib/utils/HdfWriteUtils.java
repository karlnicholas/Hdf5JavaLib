package org.hdf5javalib.utils;

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfFloatPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.dataclass.HdfVariableLength;
import org.hdf5javalib.datatype.*;
import org.hdf5javalib.hdffile.infrastructure.HdfGlobalHeap;

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

/**
 * Utility class for writing data to HDF5 files.
 * <p>
 * The {@code HdfWriteUtils} class provides methods to write fixed-point, floating-point,
 * string, and compound data types to a {@link ByteBuffer}, handling endianness, scaling,
 * and global heap interactions as required by the HDF5 file format. It supports various
 * Java types and ensures compatibility with HDF5 datatypes.
 * </p>
 */
public class HdfWriteUtils {
    /**
     * Writes an {@link HdfFixedPoint} value to a {@link ByteBuffer}, accounting for endianness.
     * <p>
     * If the value is undefined, the buffer is filled with 0xFF. Otherwise, the value is written
     * in the appropriate endianness as specified by the datatype and buffer order.
     * </p>
     *
     * @param buffer the ByteBuffer to write to
     * @param value  the HdfFixedPoint value to write
     */
    public static void writeFixedPointToBuffer(ByteBuffer buffer, HdfFixedPoint value) {
        int size = value.getDatatype().getSize();
        byte[] bytesToWrite = new byte[size];

        if (value.isUndefined()) {
            Arrays.fill(bytesToWrite, (byte) 0xFF); // Undefined value → fill with 0xFF
        } else {
            byte[] valueBytes = value.getBytes();
            int copySize = Math.min(valueBytes.length, size);

            // Store in matching endianness
            if (value.getDatatype().isBigEndian() == (buffer.order() == ByteOrder.BIG_ENDIAN)) {
                System.arraycopy(valueBytes, 0, bytesToWrite, 0, copySize);
            } else {
                for (int i = 0; i < copySize; i++) {
                    bytesToWrite[i] = valueBytes[copySize - 1 - i];
                }
            }
        }

        buffer.put(bytesToWrite);
    }

    /**
     * Writes a compound type instance to a {@link ByteBuffer}.
     * <p>
     * Maps fields of the provided instance to the members of the compound datatype and
     * writes their values to the buffer at the appropriate offsets, handling strings,
     * variable-length strings, fixed-point, and floating-point types.
     * </p>
     *
     * @param instance     the instance to write
     * @param compoundType the compound datatype defining the structure
     * @param buffer       the ByteBuffer to write to
     * @param dataClass    the class of the instance
     * @param <T>          the type of the instance
     * @throws RuntimeException if reflection or datatype errors occur
     */
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
                Datatype memberType = member.getHdfDatatype();

                if (memberType instanceof StringDatatype membertyped) {
                    if (fieldType != String.class) {
                        throw new IllegalArgumentException("Field " + member.getName() + " must be String for StringDatatype, got " + fieldType.getName());
                    }
                    Charset charset = membertyped.getCharacterSet() == StringDatatype.CharacterSet.ASCII
                            ? StandardCharsets.US_ASCII : StandardCharsets.UTF_8;
                    byte[] bytes = ((String) value).getBytes(charset);
                    HdfString hdfString = new HdfString(bytes, membertyped);
                    buffer.position(Math.toIntExact(member.getOffset()));
                    hdfString.writeValueToByteBuffer(buffer);
                } else if (memberType instanceof VariableLengthDatatype membertyped) {
                    if (fieldType != String.class) {
                        throw new IllegalArgumentException("Field " + member.getName() + " must be String for VariableLengthDatatype, got " + fieldType.getName());
                    }
                    Charset charset = membertyped.getCharacterSet() == StringDatatype.CharacterSet.ASCII
                            ? StandardCharsets.US_ASCII : StandardCharsets.UTF_8;
                    byte[] bytes = ((String) value).getBytes(charset);
                    HdfGlobalHeap hdfGlobalHeap = membertyped.getGlobalHeap();
                    byte[] varInstanceBytes = hdfGlobalHeap.addToHeap(bytes);

                    HdfVariableLength hdfVariableLength = new HdfVariableLength(varInstanceBytes, membertyped);
                    buffer.position(Math.toIntExact(member.getOffset()));
                    hdfVariableLength.writeValueToByteBuffer(buffer);
                } else if (memberType instanceof FixedPointDatatype membertyped) {
                    byte[] bytes = toFixedPointBytes(value, membertyped, fieldType);
                    HdfFixedPoint hdfFixedPoint = new HdfFixedPoint(bytes, membertyped);
                    buffer.position(Math.toIntExact(member.getOffset()));
                    hdfFixedPoint.writeValueToByteBuffer(buffer);
                } else if (memberType instanceof FloatingPointDatatype membertyped) {
                    byte[] bytes = toFloatPointBytes(value, membertyped, fieldType);
                    HdfFloatPoint hdfFloatPoint = new HdfFloatPoint(bytes, membertyped);
                    buffer.position(Math.toIntExact(member.getOffset()));
                    hdfFloatPoint.writeValueToByteBuffer(buffer);
                } else {
                    throw new UnsupportedOperationException("Unsupported member datatype: " + memberType.getClass().getName());
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates an {@link HdfFixedPoint} from a long value using the specified datatype.
     *
     * @param value              the long value to convert
     * @param fixedPointDatatype the fixed-point datatype defining the format
     * @return the created HdfFixedPoint
     * @throws IllegalArgumentException if the datatype size is unsupported
     */
    public static HdfFixedPoint hdfFixedPointFromValue(long value, FixedPointDatatype fixedPointDatatype) {
        Class<?> fieldType = switch (fixedPointDatatype.getSize()) {
            case 1 -> Byte.class;
            case 2 -> Short.class;
            case 4 -> Integer.class;
            case 8 -> Long.class;
            default ->
                    throw new IllegalArgumentException("Unsupported size for FixedPointDatatype: " + fixedPointDatatype.getSize());
        };
        return new HdfFixedPoint(toFixedPointBytes(value, fixedPointDatatype, fieldType), fixedPointDatatype);
    }

    /**
     * Converts a BigDecimal value to a BigInteger, applying scaling and precision masking.
     * This helper method isolates the complex logic from the main conversion method.
     */
    private static BigInteger convertBigDecimalToBigInteger(BigDecimal value, FixedPointDatatype datatype) {
        // Scale by 2^bitOffset to match fixed-point representation
        BigDecimal scaledValue = value.multiply(BigDecimal.valueOf(1L << datatype.getBitOffset()));
        BigInteger intValue = scaledValue.toBigInteger();

        // Apply bitPrecision mask if necessary
        int bitPrecision = datatype.getBitPrecision();
        if (bitPrecision > 0 && bitPrecision < datatype.getSize() * 8) {
            BigInteger mask = BigInteger.ONE.shiftLeft(bitPrecision).subtract(BigInteger.ONE);
            intValue = intValue.and(mask);
        }
        return intValue;
    }

    /**
     * Converts a field value to a byte array for a FixedPointDatatype.
     *
     * @param value     the value to convert
     * @param datatype  the fixed-point datatype
     * @param fieldType the Java type of the field
     * @return the byte array representing the value
     * @throws IllegalArgumentException if the field type or value is unsupported
     */
    public static byte[] toFixedPointBytes(Object value, FixedPointDatatype datatype, Class<?> fieldType) {
        int size = datatype.getSize();
        BigInteger intValue;

        // 1. Convert various numeric types to a canonical BigInteger representation.
        if (value instanceof BigInteger) {
            intValue = (BigInteger) value;
        } else if (value instanceof BigDecimal) {
            intValue = convertBigDecimalToBigInteger((BigDecimal) value, datatype);
        } else if (value instanceof Long || value instanceof Integer || value instanceof Short || value instanceof Byte) {
            intValue = BigInteger.valueOf(((Number) value).longValue());
        } else {
            throw new IllegalArgumentException("Field " + fieldType.getName() + " not supported for FixedPointDatatype");
        }

        // 2. Convert BigInteger to a little-endian byte array.
        byte[] valueBytes = intValue.toByteArray();
        reverseBytesInPlace(valueBytes); // toByteArray() is big-endian, so reverse for LE processing.

        // 3. Trim, validate size, and copy to a result array of the correct final size.
        byte[] trimmedBytes = trimTrailingZeros(valueBytes);
        if (trimmedBytes.length > size) {
            throw new IllegalArgumentException("Value " + value + " too large for " + size + " bytes");
        }

        byte[] result = new byte[size]; // Padded with zeros by default.
        System.arraycopy(trimmedBytes, 0, result, 0, trimmedBytes.length);

        // 4. Adjust to the target endianness.
        if (datatype.isBigEndian()) {
            // `result` is currently in little-endian format, so reverse it.
            reverseBytesInPlace(result);
        }

        return result;
    }


    /**
     * Converts a field value to a byte array for a FloatingPointDatatype.
     *
     * @param value     the value to convert
     * @param datatype  the floating-point datatype
     * @param fieldType the Java type of the field
     * @return the byte array representing the value
     * @throws IllegalArgumentException if the field type or size is unsupported
     */
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

    /**
     * Trims trailing zeros from a byte array.
     *
     * @param bytes the byte array to trim
     * @return the trimmed byte array, or a single zero byte if all zeros
     */
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