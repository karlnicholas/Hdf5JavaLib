package org.hdf5javalib.maydo.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

/**
 * An implementation of the HDF5 scale-offset filter for integer data.
 * This filter "deflates" the data by finding the minimum value in a chunk,
 * subtracting it from all elements, and then bit-packing the resulting
 * non-negative integers.
 *
 * This implementation is based on the logic in the HDF5 library's H5Zscale.c.
 */
public class ScaleOffsetByteBufferDeflater implements ByteBufferDeflater {

    // Constants mapping to indices in the clientData array, from H5Zscale.c
    private static final int H5Z_SCALEOFFSET_PARM_SCALETYPE = 0;
    private static final int H5Z_SCALEOFFSET_PARM_SCALEFACTOR = 1;
    private static final int H5Z_SCALEOFFSET_PARM_NELMTS = 2;
    private static final int H5Z_SCALEOFFSET_PARM_CLASS = 3;
    private static final int H5Z_SCALEOFFSET_PARM_SIZE = 4;
    private static final int H5Z_SCALEOFFSET_PARM_SIGN = 5;
    private static final int H5Z_SCALEOFFSET_PARM_ORDER = 6;
    private static final int H5Z_SCALEOFFSET_PARM_FILAVAIL = 7;
    private static final int H5Z_SCALEOFFSET_PARM_FILVAL = 8;

    // The compressed data is prefixed with a header containing minbits and minval.
    // The C-code uses a fixed offset of 21 for this header.
    private static final int HEADER_SIZE = 21;

    private final int nelmts;
    private final int dtypeSize;
    private final ByteOrder byteOrder;
    private final boolean fillValueDefined;
    private final int fillValue;
    private final int scaleFactor; // User-provided minbits hint

    /**
     * Constructs a deflater for the HDF5 Scale-Offset filter.
     *
     * @param clientData The filter parameters provided by the HDF5 file.
     */
    public ScaleOffsetByteBufferDeflater(int[] clientData) {
        // We assume integer data as per the problem description.
        // clientData[0] (scale type) and clientData[3] (class) would be used for a more general implementation.
        this.scaleFactor = clientData[H5Z_SCALEOFFSET_PARM_SCALEFACTOR];
        this.nelmts = clientData[H5Z_SCALEOFFSET_PARM_NELMTS];
        this.dtypeSize = clientData[H5Z_SCALEOFFSET_PARM_SIZE];
        // H5Z_SCALEOFFSET_SGN_2 (1) is signed, H5Z_SCALEOFFSET_SGN_NONE (0) is unsigned.
        // clientData[5] -> isSigned is not used as Java's int is always signed, but retained for completeness.
        // H5Z_SCALEOFFSET_ORDER_LE (0) is little-endian.
        this.byteOrder = (clientData[H5Z_SCALEOFFSET_PARM_ORDER] == 0) ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        this.fillValueDefined = clientData[H5Z_SCALEOFFSET_PARM_FILAVAIL] == 1;

        // For a 32-bit integer, the fill value is stored directly in clientData[8].
        // This is a simplification based on the H5Z_scaleoffset_save_filval C macro.
        if (this.fillValueDefined && this.dtypeSize == 4) {
            this.fillValue = clientData[H5Z_SCALEOFFSET_PARM_FILVAL];
        } else {
            this.fillValue = 0; // Default if not defined or type is different
        }
    }

    /**
     * Applies scale-offset deflation to the input buffer.
     *
     * @param input The uncompressed chunk data.
     * @return A new ByteBuffer containing the compressed data.
     * @throws IOException (Not thrown by this implementation, but required by the interface).
     */
    @Override
    public ByteBuffer deflate(ByteBuffer input) throws IOException {
        if (nelmts == 0) {
            return ByteBuffer.allocate(0);
        }

        // 1. Read input data into a primitive array
        input.order(this.byteOrder);
        IntBuffer intBuffer = input.asIntBuffer();
        int[] data = new int[nelmts];
        intBuffer.get(data);

        // 2. Find min and max values, ignoring the fill value if defined
        long minVal = Long.MAX_VALUE;
        long maxVal = Long.MIN_VALUE;
        boolean foundData = false;

        for (int val : data) {
            if (fillValueDefined && val == fillValue) {
                continue;
            }
            foundData = true;
            if (val < minVal) {
                minVal = val;
            }
            if (val > maxVal) {
                maxVal = val;
            }
        }

        if (!foundData) {
            // All elements were fill values.
            minVal = 0;
            maxVal = -1; // Makes range < 0, resulting in minbits = 0.
        }

        // 3. Calculate minbits needed
        int minbits;
        if (scaleFactor > 0) {
            // A non-zero scale_factor from the user is a request for a fixed number of bits.
            minbits = scaleFactor;
        } else {
            long range = maxVal - minVal;
            if (range < 0) {
                minbits = 0; // Happens if all data are fill values.
            } else {
                long valuesToRepresent = range + 1;
                if (fillValueDefined) {
                    valuesToRepresent++; // An extra value is needed to represent the fill value.
                }
                minbits = calculateMinBits(valuesToRepresent);
            }
        }

        if (minbits >= dtypeSize * 8) {
            // If the required bits are the same or more than the original,
            // we store the data un-packed.
            minbits = dtypeSize * 8;
        }

        // 4. Allocate the output buffer
        final int dataSizeBytes;
        if (minbits == dtypeSize * 8) {
            dataSizeBytes = nelmts * dtypeSize;
        } else if (minbits > 0) {
            // The size calculation from H5Zscale.c is `floor(total_bits / 8) + 1` for packed data.
            // This can allocate one more byte than is strictly necessary but must be replicated for compatibility.
            dataSizeBytes = (int) ((((long) nelmts * minbits) / 8) + 1);
        } else { // minbits == 0
            dataSizeBytes = 0;
        }

        ByteBuffer output = ByteBuffer.allocate(HEADER_SIZE + dataSizeBytes);
        output.order(ByteOrder.LITTLE_ENDIAN); // Header and packed data are LE.

        // 5. Write the 21-byte header
        output.putInt(minbits);
        output.put((byte) 8); // Corresponds to sizeof(unsigned long long) on 64-bit systems
        output.putLong(minVal);
        output.put(new byte[8]); // Padding to reach 21 bytes

        // 6. Write the data payload
        if (minbits == dtypeSize * 8) {
            // Full precision: copy the original data, no transformation.
            input.rewind();
            output.put(input);
        } else if (minbits > 0) {
            // Bit-packing required.
            long fillValueRepresentation = (1L << minbits) - 1;
            long bitBuffer = 0;
            int bitsInLBuffer = 0;

            for (int val : data) {
                long transformedVal;
                if (fillValueDefined && val == fillValue) {
                    transformedVal = fillValueRepresentation;
                } else {
                    transformedVal = (long) val - minVal;
                }

                // Add the new bits to the buffer.
                bitBuffer |= (transformedVal << bitsInLBuffer);
                bitsInLBuffer += minbits;

                // Write out full bytes from the buffer.
                while (bitsInLBuffer >= 8) {
                    output.put((byte) bitBuffer); // Puts the lower 8 bits.
                    bitBuffer >>>= 8; // Unsigned shift to process next bits.
                    bitsInLBuffer -= 8;
                }
            }
            // Write any remaining bits in the buffer.
            if (bitsInLBuffer > 0) {
                output.put((byte) bitBuffer);
            }
        }
        // If minbits is 0, the data payload is empty.

        // 7. Prepare the buffer for reading by the caller.
        output.flip();
        return output;
    }

    /**
     * Calculates the minimum number of bits required to store a given number of distinct values.
     * This is equivalent to ceil(log2(numberOfValues)).
     *
     * @param numberOfValues The number of values to represent.
     * @return The number of bits required.
     */
    private int calculateMinBits(long numberOfValues) {
        if (numberOfValues <= 1) {
            return 0;
        }
        // This is a robust Java equivalent of ceil(log2(n)).
        // It calculates floor(log2(n-1)) + 1.
        return 64 - Long.numberOfLeadingZeros(numberOfValues - 1);
    }
}