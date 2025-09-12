package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;

/**
 * A marker interface for blocks within the Fractal Heap (Direct or Indirect).
 */
public interface Block {
    long getBlockOffset();
}