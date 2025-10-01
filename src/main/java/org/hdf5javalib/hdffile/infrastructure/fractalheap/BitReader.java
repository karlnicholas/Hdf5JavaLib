package org.hdf5javalib.hdffile.infrastructure.fractalheap;

//##############################################################################
//### HDF5 UTILITY CLASSES
//##############################################################################
class BitReader {
    private final byte[] data;
    private int bitPosition;

    public BitReader(byte[] data) {
        this.data = data;
        this.bitPosition = 0;
    }

    public long read(int numBits) {
        if (numBits == 0) return 0;
        long value = 0;
        for (int i = 0; i < numBits; i++) {
            int currentBitPos = bitPosition++;
            int byteIndex = currentBitPos / 8;
            int bitIndexInByte = currentBitPos % 8;
            if ((data[byteIndex] & (1 << bitIndexInByte)) != 0) {
                value |= (1L << i);
            }
        }
        return value;
    }
}
