package com.github.karlnicholas.hdf5javalib.datatype;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class HdfString implements HdfDataType {
    private final byte[] bytes;
    private final int length;
    private final boolean nullTerminated;
    private final boolean utf8Encoding;

    // Constructor for HDF metadata-based initialization (comprehensive parameters)
    public HdfString(byte[] bytes, boolean nullTerminated, boolean utf8Encoding) {
        if (bytes == null) {
            throw new IllegalArgumentException("Byte array cannot be null");
        }

        if (nullTerminated && bytes[bytes.length - 1] != 0) {
            throw new IllegalArgumentException("Null-terminated string must end with a null byte");
        }

        int zeroLocation = 0;
        for(byte b : bytes) {
            if (b != 0) {
                zeroLocation++;
            } else {
                break;
            }
        }
        if ( nullTerminated) zeroLocation++;
        this.bytes = Arrays.copyOf(bytes, Math.min(bytes.length, zeroLocation));
        this.nullTerminated = nullTerminated;
        this.utf8Encoding = utf8Encoding;
        this.length = this.bytes.length;
    }

    // Constructor without HDF metadata parameters, defaults based on HDF specification
    public HdfString(byte[] bytes) {
        this(bytes, true, true); // Defaults to null-terminated and UTF-8 encoding
    }

    // Java-specific value-based constructor
    public HdfString(String value, boolean utf8Encoding) {
        if (value == null) {
            throw new IllegalArgumentException("String value cannot be null");
        }

        this.nullTerminated = true; // Always default to null-terminated
        this.utf8Encoding = utf8Encoding;

        byte[] encodedBytes = utf8Encoding
                ? value.getBytes(StandardCharsets.UTF_8)
                : value.getBytes(StandardCharsets.US_ASCII);

        this.length = encodedBytes.length + 1; // Include null terminator
        this.bytes = new byte[length];
        System.arraycopy(encodedBytes, 0, this.bytes, 0, encodedBytes.length);
        this.bytes[encodedBytes.length] = 0; // Add null terminator
    }

    // Get the string value for application use
    public String getValue() {
        int effectiveLength = nullTerminated ? length - 1 : length;
        byte[] effectiveBytes = Arrays.copyOf(bytes, effectiveLength);

        return utf8Encoding
                ? new String(effectiveBytes, StandardCharsets.UTF_8)
                : new String(effectiveBytes, StandardCharsets.US_ASCII);
    }

    // Get the HDF byte[] representation for storage, always returns a copy
    public byte[] getHdfBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    // Validation for any input values provided
    private void validateStringInput(String value, boolean utf8Encoding) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("String value cannot be null or empty");
        }
    }

    // Immutability: Ensure defensive copying and final fields
    private byte[] defensiveCopy(byte[] input) {
        return input == null ? null : Arrays.copyOf(input, input.length);
    }

    // String representation for debugging and user-friendly output
    @Override
    public String toString() {
        return getValue();
    }

    @Override
    public short getSizeMessageData() {
        return (short)bytes.length;
    }

    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        // TODO: IMPLEMENT
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        buffer.put(bytes);
        if ( nullTerminated ) {buffer.put((byte)0);}
    }

//    @Override
//    public String toString() {
//        return "HdfString{" +
//                "value='" + getValue() + '\'' +
//                ", length=" + length +
//                ", nullTerminated=" + nullTerminated +
//                ", utf8Encoding=" + utf8Encoding +
//                '}';
//    }
}
