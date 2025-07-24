package org.hdf5javalib.utils;

import java.util.zip.Checksum;

public class Fletcher32 implements Checksum {
    private long c0 = 0;
    private long c1 = 0;
    private int wordsSinceMod = 0;
    private boolean hasLeftover = false;
    private byte leftover = 0;

    @Override
    public void update(int b) {
        update(new byte[]{(byte) b}, 0, 1);
    }

    @Override
    public void update(byte[] b, int off, int len) {
        if (b == null) {
            throw new NullPointerException();
        }
        if (off < 0 || len < 0 || off > b.length - len) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if (len == 0) {
            return;
        }

        int pos = off;
        int remaining = len;

        if (hasLeftover) {
            int high = b[pos] & 0xFF;
            pos++;
            remaining--;
            int word = (high << 8) | (leftover & 0xFF);
            c0 += word;
            c1 += c0;
            wordsSinceMod++;
            if (wordsSinceMod >= 360) {
                c0 %= 65535;
                c1 %= 65535;
                wordsSinceMod = 0;
            }
            hasLeftover = false;
        }

        while (remaining >= 2) {
            int low = b[pos] & 0xFF;
            pos++;
            int high = b[pos] & 0xFF;
            pos++;
            int word = (high << 8) | low;
            c0 += word;
            c1 += c0;
            wordsSinceMod++;
            if (wordsSinceMod >= 360) {
                c0 %= 65535;
                c1 %= 65535;
                wordsSinceMod = 0;
            }
            remaining -= 2;
        }

        if (remaining > 0) {
            leftover = b[pos];
            hasLeftover = true;
        }
    }

    @Override
    public long getValue() {
        long temp0 = c0;
        long temp1 = c1;
        if (hasLeftover) {
            int word = leftover & 0xFF;  // Pad with 0 in high byte
            temp0 += word;
            temp1 += temp0;
        }
        temp0 %= 65535;
        temp1 %= 65535;
        return (temp1 << 16) | temp0;
    }

    @Override
    public void reset() {
        c0 = 0;
        c1 = 0;
        wordsSinceMod = 0;
        hasLeftover = false;
        leftover = 0;
    }
}
