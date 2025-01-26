package com.github.karlnicholas.hdf5javalib.message;

import com.github.karlnicholas.hdf5javalib.HdfDataObjectHeader;

import java.nio.ByteBuffer;

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
