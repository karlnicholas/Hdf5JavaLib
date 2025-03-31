package org.hdf5javalib.utils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
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
    public static int[] computeStrides(int[] shape) {
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
        int flatIndex = findElement(shape, indices);;
        return data[flatIndex];
    }

    private static int findElement(int[] shape, int[] indices) {
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
        return flatIndex;
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
        int flatIndex = findElement(shape, indices);
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

    public static int[] unflattenIndex(int index, int[] strides, int[] shape) {
        int[] coord = new int[shape.length];
        for (int i = 0; i < shape.length; i++) {
            coord[i] = (index / strides[i]) % shape[i];
        }
        return coord;
    }

    public static <T> void setValue(Object array, int[] coord, T value) {
        Object current = array;
        for (int i = 0; i < coord.length - 1; i++) {
            current = Array.get(current, coord[i]);
        }
        Array.set(current, coord[coord.length - 1], value);
    }

    @SuppressWarnings("unchecked")
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

    /**
     * Slices an N-dimensional dataset streamed as a flattened {@code Stream<T>} and returns the result
     * as a properly shaped multi-dimensional Java array (e.g., {@code T[][]...}).
     * <p>
     * This method performs slicing similar to NumPy syntax using a descriptor for each axis.
     * It avoids storing intermediate collections by directly writing matched values into the output array
     * in row-major order.
     *
     * <p><b>Slicing Descriptor Format:</b></p>
     * The slicing descriptor is an {@code int[][]} where each inner array corresponds to one axis:
     * <ul>
     *     <li>{@code []} → full slice (equivalent to ":" in Python)</li>
     *     <li>{@code [index]} → fixed index (select a single element along that axis, reducing dimensionality)</li>
     *     <li>{@code [start, end]} → range slice (inclusive start, exclusive end)</li>
     * </ul>
     *
     * <p><b>Example:</b></p>
     * <pre>{@code
     * Stream<Integer> stream = IntStream.range(0, 3 * 3 * 3 * 5).boxed();
     * int[] shape = {3, 3, 3, 5};
     * int[][] slice = {
     *     {0},    // fix x = 0
     *     {},     // full y
     *     {},     // full z
     *     {1, 4}  // time 1 through 3
     * };
     * Object result = sliceStream(stream, shape, slice, Integer.class);
     * }</pre>
     *
     * @param data              A {@code Stream<T>} representing a flattened N-dimensional array in row-major order.
     * @param shape             The shape of the original N-dimensional array.
     * @param slicingDescriptor A {@code int[][]} where each entry specifies slicing behavior per dimension.
     * @param clazz             The class of the element type {@code T} (e.g., {@code Integer.class}).
     * @param <T>               The type of elements in the dataset.
     * @return A multi-dimensional Java array (e.g., {@code T[][]...}) representing the sliced data,
     *         or a scalar {@code T} if all axes are fixed.
     * @throws IllegalArgumentException if shape and descriptor lengths mismatch, or if slicing indices are invalid.
     */
    public static <T> Object sliceStream(
            Stream<T> data,
            int[] shape,
            int[][] slicingDescriptor,
            Class<T> clazz
    ) {
        int dims = shape.length;
        if (slicingDescriptor.length != dims)
            throw new IllegalArgumentException("Slicing descriptor must match shape dimensions");

        // Step 1: Compute input strides
        int[] inStrides = new int[dims];
        inStrides[dims - 1] = 1;
        for (int i = dims - 2; i >= 0; i--) {
            inStrides[i] = inStrides[i + 1] * shape[i + 1];
        }

        // Step 2: Compute output shape and output strides
        int outRank = (int) Arrays.stream(slicingDescriptor).filter(desc -> desc.length != 1).count();

        int[] outShape = new int[outRank];
        int[] sliceStarts = new int[dims];
        int outIndex = 0;
        for (int i = 0; i < dims; i++) {
            int[] desc = slicingDescriptor[i];
            if (desc.length == 1) {
                sliceStarts[i] = desc[0];
            } else if (desc.length == 2) {
                sliceStarts[i] = desc[0];
                outShape[outIndex++] = desc[1] - desc[0];
            } else if (desc.length == 0) {
                sliceStarts[i] = 0;
                outShape[outIndex++] = shape[i];
            } else {
                throw new IllegalArgumentException("Invalid slice spec at dimension " + i);
            }
        }

        int[] outStrides = new int[outRank];
        if (outRank > 0) {
            outStrides[outRank - 1] = 1;
            for (int i = outRank - 2; i >= 0; i--) {
                outStrides[i] = outStrides[i + 1] * outShape[i + 1];
            }
        }

        // Step 3: Preallocate output array
        Object outputArray;
        if (outRank == 0) {
            outputArray = Array.newInstance(clazz, 1); // scalar result
        } else {
            outputArray = Array.newInstance(clazz, outShape);
        }

        // Step 4: Iterate stream and populate output directly
        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger writeIndex = new AtomicInteger(0);

        data.forEach(value -> {
            int flatIndex = counter.getAndIncrement();

            // Convert flatIndex to coordinates
            int[] coords = new int[dims];
            int rem = flatIndex;
            for (int i = 0; i < dims; i++) {
                coords[i] = rem / inStrides[i];
                rem = rem % inStrides[i];
            }

            // Check slice match
            boolean match = true;
            for (int i = 0; i < dims; i++) {
                int[] desc = slicingDescriptor[i];
                if (desc.length == 1) {
                    if (coords[i] != desc[0]) {
                        match = false;
                        break;
                    }
                } else if (desc.length == 2) {
                    if (coords[i] < desc[0] || coords[i] >= desc[1]) {
                        match = false;
                        break;
                    }
                }
                // [] means accept all
            }

            if (!match) return;

            // Compute output coordinate
            int[] outCoord = new int[outRank];
            int outIdx = 0;
            for (int i = 0; i < dims; i++) {
                int[] desc = slicingDescriptor[i];
                if (desc.length != 1) {
                    outCoord[outIdx++] = coords[i] - sliceStarts[i];
                }
            }

            // Set the value in the nested output array
            Object arrayRef = outputArray;
            for (int i = 0; i < outCoord.length - 1; i++) {
                arrayRef = Array.get(arrayRef, outCoord[i]);
            }
            Array.set(arrayRef, outCoord[outCoord.length - 1], value);
        });

        // Return scalar if output shape is empty
        if (outRank == 0) {
            return Array.get(outputArray, 0);
        }

        return outputArray;
    }
    /**
     * Computes the shape of a multi-dimensional Java array (e.g. T[][][]).
     *
     * @param array the multi-dimensional array
     * @return an int[] where each entry is the length of the array at that dimension
     */
    public static int[] computeShape(Object array) {
        List<Integer> shape = new ArrayList<>();
        Object current = array;
        while (current != null && current.getClass().isArray()) {
            int length = Array.getLength(current);
            shape.add(length);
            current = length > 0 ? Array.get(current, 0) : null;
        }
        return shape.stream().mapToInt(i -> i).toArray();
    }

    /**
     * Streams a flattened array and writes non-null or non-zero values directly
     * into a new N-dimensional output array, where all positions not matching the predicate are null.
     * No intermediate list is used.
     *
     * @param stream the input stream of flattened values in row-major order
     * @param shape the original N-dimensional shape of the dataset
     * @param clazz the class of T
     * @param filter predicate to test which values to include (e.g., v -> v != 0)
     * @return a new Object (T[][]...) array of the same shape with only matching values set
     */
    public static <T> Object filterToNDArray(
            Stream<T> stream, int[] shape, Class<T> clazz, Predicate<T> filter
    ) {
        Object ndArray = Array.newInstance(clazz, shape);
        int[] strides = computeStrides(shape);
        AtomicInteger index = new AtomicInteger(0);

        stream.forEach(value -> {
            int flat = index.getAndIncrement();
            if (filter.test(value)) {
                int[] coord = unflattenIndex(flat, strides, shape);
                setValue(ndArray, coord, value);
            }
        });

        return ndArray;
    }
    /**
     * Scans a flattened stream and collects matching values into a list of coordinate/value/flatIndex records.
     *
     * @param stream the input stream of values
     * @param shape the shape of the array (used to compute coordinates)
     * @param filter predicate to match values
     * @return a list of MatchingEntry<T> with coordinates, flat index, and value
     */
    public static <T> List<MatchingEntry<T>> filterToCoordinateList(
            Stream<T> stream, int[] shape, java.util.function.Predicate<T> filter
    ) {
        int[] strides = computeStrides(shape);
        AtomicInteger index = new AtomicInteger(0);
        List<MatchingEntry<T>> result = new ArrayList<>();

        stream.forEach(value -> {
            int flat = index.getAndIncrement();
            if (filter.test(value)) {
                int[] coord = unflattenIndex(flat, strides, shape);
                result.add(new MatchingEntry<>(coord, flat, value));
            }
        });

        return result;
    }

    /**
     * Simple record to hold the results of coordinate-wise matching.
     */
    public static class MatchingEntry<T> {
        public final int[] coordinates;
        public final int flatIndex;
        public final T value;

        public MatchingEntry(int[] coordinates, int flatIndex, T value) {
            this.coordinates = coordinates;
            this.flatIndex = flatIndex;
            this.value = value;
        }
    }
}