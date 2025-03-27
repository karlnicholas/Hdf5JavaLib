package org.hdf5javalib.utils;

import java.lang.reflect.Array;

public class FlattenedArrayUtils {

    /**
     * Computes the total number of elements based on the shape.
     *
     * @param shape the shape of the multi-dimensional array
     * @return the total number of elements
     */
    public static int totalSize(int[] shape) {
        int size = 1;
        for (int dim : shape) {
            size *= dim;
        }
        return size;
    }

    /**
     * Validates if the flattened array matches the shape.
     *
     * @param data  the flattened array
     * @param shape the shape of the array
     * @return true if the array size matches the shape, false otherwise
     */
    public static <T> boolean validateShape(T[] data, int[] shape) {
        return data.length == totalSize(shape);
    }

    /**
     * Computes the strides for the given shape.
     *
     * @param shape the shape of the multi-dimensional array
     * @return the strides array
     */
    public static int[] computeStrides(int[] shape) {
        int[] strides = new int[shape.length];
        strides[shape.length - 1] = 1;
        for (int i = shape.length - 2; i >= 0; i--) {
            strides[i] = strides[i + 1] * shape[i + 1];
        }
        return strides;
    }

    /**
     * Retrieves an element from the flattened array using multi-dimensional indices.
     *
     * @param data    the flattened array
     * @param shape   the shape of the array
     * @param indices the multi-dimensional indices
     * @return the element at the specified indices
     * @throws IllegalArgumentException if the number of indices doesn’t match the shape
     * @throws IndexOutOfBoundsException if an index is out of bounds
     */
    public static <T> T getElement(T[] data, int[] shape, int... indices) {
        if (indices.length != shape.length) {
            throw new IllegalArgumentException("Number of indices must match shape length");
        }
        int[] strides = computeStrides(shape);
        int flatIndex = 0;
        for (int i = 0; i < indices.length; i++) {
            if (indices[i] < 0 || indices[i] >= shape[i]) {
                throw new IndexOutOfBoundsException("Index " + indices[i] + " out of bounds for dimension " + i);
            }
            flatIndex += indices[i] * strides[i];
        }
        return data[flatIndex];
    }

    /**
     * Sets an element in the flattened array using multi-dimensional indices.
     *
     * @param data    the flattened array
     * @param shape   the shape of the array
     * @param value   the value to set
     * @param indices the multi-dimensional indices
     * @throws IllegalArgumentException if the number of indices doesn’t match the shape
     * @throws IndexOutOfBoundsException if an index is out of bounds
     */
    public static <T> void setElement(T[] data, int[] shape, T value, int... indices) {
        if (indices.length != shape.length) {
            throw new IllegalArgumentException("Number of indices must match shape length");
        }
        int[] strides = computeStrides(shape);
        int flatIndex = 0;
        for (int i = 0; i < indices.length; i++) {
            if (indices[i] < 0 || indices[i] >= shape[i]) {
                throw new IndexOutOfBoundsException("Index " + indices[i] + " out of bounds for dimension " + i);
            }
            flatIndex += indices[i] * strides[i];
        }
        data[flatIndex] = value;
    }

    /**
     * Creates a flattened array with the given shape.
     *
     * @param clazz the class of the array elements
     * @param shape the shape of the array
     * @return the flattened array
     */
    public static <T> T[] createFlattenedArray(Class<T> clazz, int[] shape) {
        int totalSize = totalSize(shape);
        @SuppressWarnings("unchecked")
        T[] array = (T[]) Array.newInstance(clazz, totalSize);
        return array;
    }
}