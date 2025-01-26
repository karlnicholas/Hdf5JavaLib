package com.github.karlnicholas.hdf5javalib.message;

public class NullMessage implements HdfMessage {

    public static HdfMessage parseHeaderMessage(byte flag, byte[] data, int offsetSize, int lengthSize) {
        // No data to parse for null message
        return new NullMessage();
    }

    @Override
    public String toString() {
        return "NullMessage{}";
    }
}
