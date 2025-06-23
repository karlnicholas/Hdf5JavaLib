package org.hdf5javalib.maydo.utils;

import org.hdf5javalib.maydo.dataclass.HdfData;

import java.lang.reflect.Array;
import java.util.*;

/**
 * A container for HDF5 data that can hold either a single scalar value
 * or an n-dimensional array of values.
 * <p>
 * This class implements {@link Iterable} to allow for easy, flattened iteration
 * over all elements, regardless of dimensionality. It also provides methods
 * for type-safe retrieval of the entire underlying data object (scalar or array).
 */
public class HdfDataHolder implements Iterable<HdfData> {
    private final HdfData singleInstance; // Scalar value (dimensionality = 0)
    private final Object array;           // n-dimensional array (dimensionality >= 1)
    private final int dimensionality;     // Number of dimensions (0 for scalar, n for array)
    private final int[] dimensions;       // Shape of the array (e.g., [3,4] for 2D)

    // Private constructor to enforce factory method usage
    private HdfDataHolder(HdfData singleInstance, Object array, int dimensionality, int[] dimensions) {
        this.singleInstance = singleInstance;
        this.array = array;
        this.dimensionality = dimensionality;
        this.dimensions = dimensions != null ? dimensions : new int[0]; // No cloning
    }

    /**
     * Factory method for creating a holder for a scalar (single HdfData instance).
     *
     * @param instance The non-null HdfData scalar value.
     * @return A new HdfDataHolder instance for the scalar.
     */
    public static HdfDataHolder ofScalar(HdfData instance) {
        if (instance == null) {
            throw new IllegalArgumentException("Scalar instance cannot be null.");
        }
        return new HdfDataHolder(instance, null, 0, new int[0]);
    }

    /**
     * Factory method for creating a holder for an n-dimensional array.
     *
     * @param array      The non-null n-dimensional array object.
     * @param dimensions The non-null array describing the shape of the data array.
     * @return A new HdfDataHolder instance for the array.
     */
    public static HdfDataHolder ofArray(Object array, int[] dimensions) {
        if (array == null || dimensions == null) {
            throw new IllegalArgumentException("Array and dimensions cannot be null.");
        }
        if (dimensions.length == 0) {
            throw new IllegalArgumentException("Cannot create an array with 0 dimensions. Use ofScalar() instead.");
        }
        return new HdfDataHolder(null, array, dimensions.length, dimensions);
    }

    /**
     * Checks if the holder contains a scalar value.
     *
     * @return true if it holds a scalar, false otherwise.
     */
    public boolean isScalar() {
        return dimensionality == 0;
    }

    /**
     * Checks if the holder contains an array.
     *
     * @return true if it holds an array, false otherwise.
     */
    public boolean isArray() {
        return dimensionality > 0;
    }

    /**
     * Gets the dimensionality of the data.
     *
     * @return 0 for a scalar, or the number of dimensions for an array.
     */
    public int getDimensionality() {
        return dimensionality;
    }

    /**
     * Gets the dimensions (shape) of the array.
     *
     * @return An array of integers representing the size of each dimension. Returns an empty array for a scalar.
     */
    public int[] getDimensions() {
        return dimensions;
    }

    /**
     * Gets the scalar value.
     *
     * @return The HdfData instance.
     * @throws IllegalStateException if the holder contains an array.
     */
    public HdfData getScalar() {
        if (!isScalar()) {
            throw new IllegalStateException("Holder contains an array, not a scalar. Use get(int... indices) or an iterator.");
        }
        return singleInstance;
    }

    /**
     * Gets the raw, untyped n-dimensional array object.
     * For type-safe retrieval, prefer {@link #getAll(Class)}.
     *
     * @return The Object representing the array.
     * @throws IllegalStateException if the holder contains a scalar.
     */
    public Object getArray() {
        if (!isArray()) {
            throw new IllegalStateException("Holder contains a scalar, not an array.");
        }
        return array;
    }

    /**
     * Retrieves the HdfData element at the specified coordinates.
     * This provides random access to elements in the n-dimensional array.
     *
     * @param indices The coordinates of the element to retrieve (e.g., `get(row, col)` for 2D).
     * @return The HdfData object at that position.
     * @throws IllegalStateException     if the holder contains a scalar.
     * @throws IllegalArgumentException  if the number of indices does not match the dimensionality.
     * @throws IndexOutOfBoundsException if any index is out of bounds for its dimension.
     */
    public HdfData get(int... indices) {
        if (isScalar()) {
            throw new IllegalStateException("Cannot use indexed get() on a scalar holder.");
        }
        if (indices.length != dimensionality) {
            throw new IllegalArgumentException(
                    "Incorrect number of indices. Expected " + dimensionality + ", but got " + indices.length
            );
        }

        Object current = this.array;
        for (int i = 0; i < dimensionality; i++) {
            int index = indices[i];
            if (index < 0 || index >= dimensions[i]) {
                throw new IndexOutOfBoundsException(
                        "Index " + index + " is out of bounds for dimension " + i + " with size " + dimensions[i]
                );
            }
            // Descend into the next level of the array
            current = Array.get(current, index);
        }
        return (HdfData) current;
    }

    // Stringify n-dimensional array
    public static String arrayToString(Object array, int[] dimensions) {
        int nDimensions = dimensions.length;
        if (array == null) return "null";
        if (!array.getClass().isArray()) return array.toString();

        // Handle 1D or 2D with single row
        if (nDimensions == 1 || (nDimensions == 2 && dimensions[0] == 1)) {
            // 1D array or 2D with one row: flatten to [72.9, 73.0]
            Object targetArray = nDimensions == 1 ? array : Array.get(array, 0);
            return Arrays.toString((Object[]) targetArray);
        }

        // n>1 with multiple rows or n>2: preserve nested structure
        StringBuilder sb = new StringBuilder("[");
        int length = Array.getLength(array);
        for (int i = 0; i < length; i++) {
            Object element = Array.get(array, i);
            sb.append(arrayToString(element, Arrays.copyOfRange(dimensions, 1, dimensions.length)));
            if (i < length - 1) sb.append(", ");
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * Retrieves the entire underlying data object, casting it to the requested type.
     * <p>
     * This method provides a type-safe way to get the complete data structure, whether
     * it's a scalar {@code HdfData} object or a multi-dimensional array like
     * {@code HdfData[][]}. The caller is responsible for knowing the expected type,
     * typically by first inspecting {@link #getDimensionality()}.
     * <p>
     * Example Usage:
     * <pre>{@code
     * // For a 2D dataset
     * if (holder.getDimensionality() == 2) {
     *     HdfData[][] data = holder.getAll(HdfData[][].class);
     *     // ... work with the 2D array
     * }
     * // For a scalar dataset
     * if (holder.isScalar()) {
     *     HdfData data = holder.getAll(HdfData.class);
     * }
     * }</pre>
     *
     * @param clazz The expected {@link Class} of the data object. For arrays, use the
     *              array type class (e.g., {@code String[].class}).
     * @param <T>   The generic type parameter corresponding to {@code clazz}.
     * @return The underlying data object, cast to type T.
     * @throws IllegalArgumentException if the requested type {@code clazz} is not
     *                                  compatible with the actual type of the data stored in the holder.
     */
    public <T> T getAll(Class<T> clazz) {
        if (clazz == null) {
            throw new IllegalArgumentException("The requested class cannot be null.");
        }

        if (isScalar()) {
            // Check if the requested type is assignable from our scalar's type.
            if (clazz.isAssignableFrom(singleInstance.getClass())) {
                return clazz.cast(singleInstance);
            } else {
                throw new IllegalArgumentException(String.format(
                        "Type mismatch: Requested class '%s' is not compatible with the scalar content of type '%s'.",
                        clazz.getCanonicalName(),
                        singleInstance.getClass().getCanonicalName()
                ));
            }
        } else { // isArray()
            // Check if the requested type is assignable from our array's type.
            if (clazz.isAssignableFrom(array.getClass())) {
                return clazz.cast(array);
            } else {
                throw new IllegalArgumentException(String.format(
                        "Type mismatch: Requested array type '%s' is not compatible with the actual array type '%s'.",
                        clazz.getCanonicalName(),
                        array.getClass().getCanonicalName()
                ));
            }
        }
    }


    /**
     * Returns an iterator that traverses all HdfData elements in a flattened, sequential order.
     * This allows the holder to be used in a for-each loop.
     *
     * @return An iterator over HdfData elements.
     */
    @Override
    public Iterator<HdfData> iterator() {
        if (isScalar()) {
            // For a scalar, return an iterator for a single element.
            return Collections.singleton(singleInstance).iterator();
        }
        // For an array, return our custom n-dimensional iterator.
        return new HdfDataArrayIterator(array);
    }

    /**
     * An iterator for traversing an n-dimensional array of objects.
     * It uses a stack to manage the traversal state, allowing it to "flatten"
     * the nested array structure into a single sequence of HdfData elements.
     */
    private static class HdfDataArrayIterator implements Iterator<HdfData> {
        private final Deque<Iterator<?>> iterators = new ArrayDeque<>();
        private HdfData nextElement;

        public HdfDataArrayIterator(Object array) {
            if (Array.getLength(array) > 0) {
                // Start with an iterator for the top-level array
                this.iterators.push(Arrays.stream((Object[]) array).iterator());
                // Find the first leaf element
                findNext();
            }
        }

        @Override
        public boolean hasNext() {
            return nextElement != null;
        }

        @Override
        public HdfData next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            HdfData currentElement = nextElement;
            // Find the next element for the subsequent call to next() or hasNext()
            findNext();
            return currentElement;
        }

        /**
         * Core logic to find the next leaf node (HdfData element) in the n-dimensional structure.
         */
        private void findNext() {
            this.nextElement = null;
            while (!iterators.isEmpty()) {
                Iterator<?> currentIter = iterators.peek();
                if (currentIter.hasNext()) {
                    Object element = currentIter.next();
                    if (element.getClass().isArray()) {
                        // It's another array, go deeper.
                        if (Array.getLength(element) > 0) {
                            iterators.push(Arrays.stream((Object[]) element).iterator());
                        }
                    } else {
                        // We found a leaf node. This must be our HdfData element.
                        this.nextElement = (HdfData) element;
                        return; // Found it, exit the loop.
                    }
                } else {
                    // This level is exhausted, go up.
                    iterators.pop();
                }
            }
        }
    }

    /**
     * Returns a string representation of the HdfDataHolder.
     * For scalars, it delegates to the HdfData's toString() method.
     * For arrays, it provides a summary of the structure and elements.
     *
     * @return A string representation of the object.
     */
    @Override
    public String toString() {
        if (isScalar()) {
            return singleInstance.toString(); // Use HdfData's toString for scalar
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("HdfDataHolder{array, dimensionality=").append(dimensionality)
                    .append(", dimensions=").append(Arrays.toString(dimensions));
            if (array != null) {
                sb.append(", elements=");
                appendArrayElements(sb, array);
            } else {
                sb.append(", elements=null");
            }
            sb.append("}");
            return sb.toString();
        }
    }

    /**
     * Helper method to recursively append all array elements to a StringBuilder for toString().
     * Uses Arrays.deepToString for a standard and robust representation.
     */
    private void appendArrayElements(StringBuilder sb, Object current) {
        if (current instanceof Object[]) {
            sb.append(Arrays.deepToString((Object[]) current));
        } else {
            // Fallback for primitive arrays, though this class is designed for HdfData objects
            // This part might need adjustment if you also store primitive arrays directly
            sb.append("[...primitive array...]");
        }
    }
}