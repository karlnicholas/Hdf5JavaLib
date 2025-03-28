package org.hdf5javalib.utils;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

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
//    public static int[] computeStrides(int[] shape) {
//        int[] strides = new int[shape.length];
//        strides[shape.length - 1] = 1;
//        for (int i = shape.length - 2; i >= 0; i--) {
//            strides[i] = strides[i + 1] * shape[i + 1];
//        }
//        return strides;
//    }

    private static int[] computeStrides(int[] shape) {
        int[] strides = new int[shape.length];
        int stride = 1;
        for (int i = shape.length - 1; i >= 0; i--) {
            strides[i] = stride;
            stride *= shape[i];
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
    /**
     * Generic method to convert a Stream<T> to an N-D array. Converts a streamed flattened N-D array into an N-dimensional primitive int array.
     *
     * @param stream The stream of integers representing the flattened data.
     * @param shape An array of integers defining the dimensions [d1, d2, ..., dN].
     * @return An Object representing the N-D array (e.g., int[] for N=1, int[][] for N=2, etc.).
     */
    public static <T> Object streamToNDArray(Stream<T> stream, int[] shape, Class<T> clazz) {
        int totalSize = Arrays.stream(shape).reduce(1, (a, b) -> a * b);
        Object ndArray = Array.newInstance(clazz, shape);
        int[] strides = computeStrides(shape);
        AtomicInteger index = new AtomicInteger(0);

        stream.forEach(value -> {
            int flat = index.getAndIncrement();
            int[] coord = unflattenIndex(flat, strides, shape);
            setValue(ndArray, coord, value);
        });

        return ndArray;
    }

    private static int[] unflattenIndex(int index, int[] strides, int[] shape) {
        int[] coord = new int[shape.length];
        for (int i = 0; i < shape.length; i++) {
            coord[i] = (index / strides[i]) % shape[i];
        }
        return coord;
    }

    private static <T> void setValue(Object array, int[] coord, T value) {
        Object current = array;
        for (int i = 0; i < coord.length - 1; i++) {
            current = Array.get(current, coord[i]);
        }
        Array.set(current, coord[coord.length - 1], value);
    }

    public static <T> Object reduceAlongAxis(
            Stream<T> stream, int[] shape, int axis, BinaryOperator<T> reducer, Class<T> clazz) {

        if (axis < 0 || axis >= shape.length) {
            throw new IllegalArgumentException("Invalid axis for reduction: " + axis);
        }

        // ✅ Handle 1D → scalar reduction
        if (shape.length == 1) {
            return stream.limit(shape[0]).reduce(reducer).orElse(null);
        }

        // Compute shape of the result (N-1)
        int[] reducedShape = new int[shape.length - 1];
        for (int i = 0, j = 0; i < shape.length; i++) {
            if (i != axis) reducedShape[j++] = shape[i];
        }

        Object resultArray = Array.newInstance(clazz, reducedShape);

        int totalSize = Arrays.stream(shape).reduce(1, (a, b) -> a * b);
        int[] strides = computeStrides(shape);
        AtomicInteger index = new AtomicInteger(0);

        stream.limit(totalSize).forEach(value -> {
            int flat = index.getAndIncrement();
            int[] coord = unflattenIndex(flat, strides, shape);

            // Create coordinate for the reduced array (exclude the axis)
            int[] reducedCoord = new int[coord.length - 1];
            for (int i = 0, j = 0; i < coord.length; i++) {
                if (i != axis) reducedCoord[j++] = coord[i];
            }

            // Navigate to target cell in result array
            Object current = resultArray;
            for (int i = 0; i < reducedCoord.length - 1; i++) {
                current = Array.get(current, reducedCoord[i]);
            }

            int lastIndex = reducedCoord[reducedCoord.length - 1];
            T currentValue = (T) Array.get(current, lastIndex);

            if (currentValue == null) {
                Array.set(current, lastIndex, value);
            } else {
                Array.set(current, lastIndex, reducer.apply(currentValue, value));
            }
        });

        return resultArray;
    }

//    private static int[] computeStrides(int[] shape) {
//        int[] strides = new int[shape.length];
//        int stride = 1;
//        for (int i = shape.length - 1; i >= 0; i--) {
//            strides[i] = stride;
//            stride *= shape[i];
//        }
//        return strides;
//    }
//
//    private static int[] unflattenIndex(int index, int[] strides, int[] shape) {
//        int[] coord = new int[shape.length];
//        for (int i = 0; i < shape.length; i++) {
//            coord[i] = (index / strides[i]) % shape[i];
//        }
//        return coord;
//    }
}