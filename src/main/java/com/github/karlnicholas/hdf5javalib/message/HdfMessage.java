package com.github.karlnicholas.hdf5javalib.message;

public interface HdfMessage {
    /**
     *
     * @param flags
     * @param offsetSize
     * @param lengthSize
     * @return
     */
    HdfMessage parseHeaderMessage(byte flags, byte[] data, int offsetSize, int lengthSize);

    /**
     * Generate a string representation of the message.
     *
     * @return A string describing the message.
     */
    @Override
    String toString();
}
