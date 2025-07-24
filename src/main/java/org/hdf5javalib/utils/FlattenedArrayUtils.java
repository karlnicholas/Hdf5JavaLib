package org.hdf5javalib.utils;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BinaryOperator;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Utility class for manipulating and processing multi-dimensional arrays in a flattened format.
 * <p>
 * The {@code FlattenedArrayUtils} class provides methods for handling multi-dimensional
 * arrays, including computing total sizes, validating shapes, accessing and setting elements,
 * computing strides, and performing operations like slicing, reduction, and filtering on
 * flattened streams. It supports operations similar to NumPy for HDF5 data processing.
 * </p>
 */
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
     * @param <T>   the type of elements in the array
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
     * @param <T>     the type of elements in the array
     * @param data    the flattened array
     * @param shape   the shape of the array
     * @param indices the multi-dimensional indices
     * @return the element at the specified indices
     * @throws IllegalArgumentException  if the number of indices doesn’t match the shape
     * @throws IndexOutOfBoundsException if an index is out of bounds
     */
    public static <T> T getElement(T[] data, int[] shape, int... indices) {
        int flatIndex = findElement(shape, indices);
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
     * @param <T>     the type of elements in the array
     * @param data    the flattened array
     * @param shape   the shape of the array
     * @param value   the value to set
     * @param indices the multi-dimensional indices
     * @throws IllegalArgumentException  if the number of indices doesn’t match the shape
     * @throws IndexOutOfBoundsException if an index is out of bounds
     */
    public static <T> void setElement(T[] data, int[] shape, T value, int... indices) {
        int flatIndex = findElement(shape, indices);
        data[flatIndex] = value;
    }

    /**
     * Creates a flattened array with the given shape.
     *
     * @param <T>   the type of elements in the array
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
     * Converts a Stream of elements to an N-dimensional array.
     * <p>
     * Converts a streamed flattened N-D array into an N-dimensional array of the
     * specified type.
     * </p>
     *
     * @param <T>    the type of elements in the stream and the resulting array
     * @param stream the Stream of elements representing the flattened data
     * @param shape  an array of integers defining the dimensions [d1, d2, ..., dN]
     * @param clazz  the Class object representing the type of elements in the array
     * @return an Object representing the N-D array (e.g., int[] for N=1, int[][] for N=2, etc.)
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

    /**
     * Converts a flat index to multi-dimensional coordinates.
     *
     * @param index   the flat index
     * @param strides the strides of the array
     * @param shape   the shape of the array
     * @return the multi-dimensional coordinates
     */
    public static int[] unflattenIndex(int index, int[] strides, int[] shape) {
        int[] coord = new int[shape.length];
        for (int i = 0; i < shape.length; i++) {
            coord[i] = (index / strides[i]) % shape[i];
        }
        return coord;
    }

    /**
     * Sets a value in a multi-dimensional array at the specified coordinates.
     *
     * @param <T>   the type of the value to set
     * @param array the multi-dimensional array
     * @param coord the coordinates
     * @param value the value to set
     */
    public static <T> void setValue(Object array, int[] coord, T value) {
        Object current = array;
        for (int i = 0; i < coord.length - 1; i++) {
            current = Array.get(current, coord[i]);
        }
        Array.set(current, coord[coord.length - 1], value);
    }

    /**
     * Reduces a multi-dimensional array along a specified axis using a binary operator.
     *
     * @param <T>     the type of elements in the stream and the resulting array
     * @param stream  the input stream of flattened values
     * @param shape   the shape of the array
     * @param axis    the axis to reduce along
     * @param reducer the binary operator for reduction
     * @param clazz   the class of the array elements
     * @return the reduced array (N-1 dimensions) or scalar if 1D
     * @throws IllegalArgumentException if the axis is invalid
     */
    @SuppressWarnings("unchecked")
    public static <T> Object reduceAlongAxis(
            Stream<T> stream, int[] shape, int axis, BinaryOperator<T> reducer, Class<T> clazz) {

        if (axis < 0 || axis >= shape.length) {
            throw new IllegalArgumentException("Invalid axis for reduction: " + axis);
        }

        // Handle 1D → scalar reduction
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
     * It avoids storing intermediate collections by directly writing matched values into the
     * output array in row-major order.
     * </p>
     *
     * <p><b>Slicing Descriptor Format:</b></p>
     * The slicing descriptor is an {@code int[][]} where each inner array corresponds to one axis:
     * <ul>
     *     <li>{@code []} → full slice (equivalent to ":" in Python)</li>
     *     <li>{@code [index]} → fixed index (select a single element along that axis, reducing dimensionality)</li>
     *     <li>{@code [start, end]} → range slice (inclusive start, exclusive end)</li>
     * </ul>
     *
     * @param <T>               the type of elements in the dataset
     * @param data              the Stream of flattened array elements
     * @param shape             the shape of the original N-dimensional array
     * @param slicingDescriptor the slicing specifications for each dimension
     * @param clazz             the Class object representing the element type
     * @return a multi-dimensional Java array or scalar representing the sliced data
     * @throws IllegalArgumentException if shape and descriptor lengths mismatch or slicing indices are invalid
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
     * Computes the shape of a multi-dimensional Java array (e.g., T[][][]).
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
     * Filters a flattened stream and writes non-null or non-zero values directly into a new
     * N-dimensional array.
     * <p>
     * Only values matching the predicate are included; unmatched positions are null.
     * </p>
     *
     * @param <T>    the type of elements in the stream and the resulting array
     * @param stream the input stream of flattened values in row-major order
     * @param shape  the shape of the original N-dimensional array
     * @param clazz  the class of the element type
     * @param filter predicate to test which values to include
     * @return a new N-dimensional array with only matching values set
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
     * @param <T>    the type of elements in the stream
     * @param stream the input stream of values
     * @param shape  the shape of the array (used to compute coordinates)
     * @param filter predicate to match values
     * @return a list of MatchingEntry objects with coordinates, flat index, and value
     */
    public static <T> List<MatchingEntry<T>> filterToCoordinateList(
            Stream<T> stream, int[] shape, Predicate<T> filter
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
     * Record to hold the results of coordinate-wise matching.
     */
    public static class MatchingEntry<T> {
        /**
         * The multi-dimensional coordinates of the matched value.
         */
        public final int[] coordinates;
        /**
         * The flat index in the array.
         */
        public final int flatIndex;
        /**
         * The matched value.
         */
        public final T value;

        /**
         * Constructs a MatchingEntry.
         *
         * @param coordinates the coordinates of the value
         * @param flatIndex   the flat index in the array
         * @param value       the matched value
         */
        public MatchingEntry(int[] coordinates, int flatIndex, T value) {
            this.coordinates = coordinates;
            this.flatIndex = flatIndex;
            this.value = value;
        }
    }

    /**
     * Slices an N-dimensional dataset streamed as a flattened {@code Stream<T>} and returns
     * a new {@code Stream<T>} containing only the elements within the specified slice.
     * The elements in the output stream maintain their original flattened (row-major) order
     * relative to each other within the slice.
     * <p>
     * This method is useful for processing slices of very large datasets without materializing
     * the entire slice in memory, enabling further stream-based operations.
     * </p>
     *
     * <p><b>Slicing Descriptor Format:</b></p>
     * The slicing descriptor is an {@code int[][]} where each inner array corresponds to one axis:
     * <ul>
     *     <li>{@code []} → full slice (equivalent to ":" in Python)</li>
     *     <li>{@code [index]} → fixed index (select a single element along that axis)</li>
     *     <li>{@code [start, end]} → range slice (inclusive start, exclusive end)</li>
     * </ul>
     * Note: When an axis is fixed with {@code [index]}, elements from that axis are still
     * included in the output stream if they are part of the overall N-D slice. This method
     * does NOT reduce the dimensionality in the stream itself; the output stream is still 1D.
     * The "shape" of the resulting slice would need to be tracked separately if N-D interpretation
     * of the output stream is required later.
     *
     * @param <T>               the type of elements in the dataset
     * @param dataStream        the Stream of flattened array elements
     * @param originalShape     the shape of the original N-dimensional array from which the stream was derived
     * @param slicingDescriptor the slicing specifications for each dimension
     * @return a new {@code Stream<T>} containing only the elements belonging to the slice
     * @throws IllegalArgumentException if shape and descriptor lengths mismatch or slicing indices are invalid
     */
    public static <T> Stream<T> sliceToStream(
            Stream<T> dataStream,
            int[] originalShape,
            int[][] slicingDescriptor
    ) {
        int dims = originalShape.length;
        if (slicingDescriptor.length != dims) {
            throw new IllegalArgumentException("Slicing descriptor must match shape dimensions");
        }

        // Validate slicingDescriptor ranges against originalShape (important for safety)
        for (int i = 0; i < dims; i++) {
            int[] desc = slicingDescriptor[i];
            if (desc.length == 1) { // Fixed index
                if (desc[0] < 0 || desc[0] >= originalShape[i]) {
                    throw new IllegalArgumentException("Fixed index " + desc[0] + " for dimension " + i +
                            " is out of bounds for shape " + originalShape[i]);
                }
            } else if (desc.length == 2) { // Range [start, end)
                if (desc[0] < 0 || desc[1] > originalShape[i] || desc[0] >= desc[1]) {
                    throw new IllegalArgumentException("Invalid range [" + desc[0] + "," + desc[1] +
                            ") for dimension " + i + " with shape " + originalShape[i]);
                }
            } else if (desc.length != 0) { // Empty array means full slice
                throw new IllegalArgumentException("Invalid slice spec (length " + desc.length +
                        ") at dimension " + i + ". Must be length 0, 1, or 2.");
            }
        }

        // Pre-compute strides for converting flat index to N-D coordinates
        final int[] strides = computeStrides(originalShape); // Use the existing helper

        // We need to associate each element from the stream with its original flat index.
        // A simple way is to create an indexed stream first.
        AtomicInteger flatIndexCounter = new AtomicInteger(0);

        return dataStream
                .map(value -> new IndexedValue<>(flatIndexCounter.getAndIncrement(), value))
                .filter(indexedValue -> {
                    int flatIndex = indexedValue.index;
                    int[] coords = unflattenIndex(flatIndex, strides, originalShape); // Use existing helper

                    // Check if these coordinates match the slicing descriptor
                    for (int i = 0; i < dims; i++) {
                        int[] desc = slicingDescriptor[i];
                        int coord_i = coords[i];

                        if (desc.length == 1) { // Fixed index
                            if (coord_i != desc[0]) {
                                return false; // Does not match fixed index
                            }
                        } else if (desc.length == 2) { // Range [start, end)
                            if (coord_i < desc[0] || coord_i >= desc[1]) {
                                return false; // Outside specified range
                            }
                        }
                        // If desc.length == 0, it's a full slice for this dimension, so always matches.
                    }
                    return true; // All dimension constraints met
                })
                .map(indexedValue -> indexedValue.value); // Extract the original value
    }

    /**
     * Helper class to pair a value with its original flat index.
     */
    private static class IndexedValue<T> {
        final int index;
        final T value;

        IndexedValue(int index, T value) {
            this.index = index;
            this.value = value;
        }
    }


    // --- For a "complete" streaming OLAP pipeline, we might need methods
    // --- that operate on streams and return streams.

    /**
     * Reduces a stream of elements using a binary operator.
     * This is a general stream reduction, not axis-specific like reduceAlongAxis.
     * If the stream is empty, returns null or an appropriate identity if provided.
     *
     * @param <T>     the type of elements
     * @param stream  the input stream
     * @param reducer the binary operator
     * @return the reduced value, or null if the stream is empty
     */
    public static <T> T reduceStream(Stream<T> stream, BinaryOperator<T> reducer) {
        return stream.reduce(reducer).orElse(null); // Or orElse(identity)
    }

    /**
     * Filters a stream based on a predicate. (Standard stream operation, just for completeness)
     *
     * @param <T>    the type of elements
     * @param stream the input stream
     * @param filter the predicate
     * @return a new filtered stream
     */
    public static <T> Stream<T> filterStream(Stream<T> stream, Predicate<T> filter) {
        return stream.filter(filter);
    }


}