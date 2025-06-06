package org.hdf5javalib.redo.utils;

import org.hdf5javalib.redo.dataclass.HdfData;

import java.util.Arrays;
import java.lang.reflect.Array;

public class HdfDataHolder {
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

    // Factory method for scalar (single HdfData instance)
    public static HdfDataHolder ofScalar(HdfData instance) {
        return new HdfDataHolder(instance, null, 0, new int[0]);
    }

    // Factory method for n-dimensional array
    public static HdfDataHolder ofArray(Object array, int[] dimensions) {
        return new HdfDataHolder(null, array, dimensions.length, dimensions);
    }

    // Check if holding a scalar
    public boolean isScalar() {
        return dimensionality == 0;
    }

    // Check if holding an array
    public boolean isArray() {
        return dimensionality > 0;
    }

    // Get dimensionality
    public int getDimensionality() {
        return dimensionality;
    }

    // Get dimensions (returns reference, no cloning)
    public int[] getDimensions() {
        return dimensions;
    }

    // Get scalar, or throw if holding an array
    public HdfData getScalar() {
        if (!isScalar()) {
            throw new IllegalStateException("Holder contains an array, not a scalar");
        }
        return singleInstance;
    }

    // Get array, or throw if holding a scalar
    public Object getArray() {
        if (!isArray()) {
            throw new IllegalStateException("Holder contains a scalar, not an array");
        }
        return array;
    }

    // String representation using instance.toString() for scalars and all array elements
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
                appendArrayElements(sb, array, 0);
            } else {
                sb.append(", elements=null");
            }
            sb.append("}");
            return sb.toString();
        }
    }

    // Helper method to recursively append all array elements
    private void appendArrayElements(StringBuilder sb, Object current, int depth) {
        if (current == null) {
            sb.append("null");
            return;
        }
        if (!current.getClass().isArray()) {
            // Leaf level: should be HdfData
            sb.append(current.toString()); // Use HdfData's toString
            return;
        }
        // Array level: append elements recursively
        int length = Array.getLength(current);
        sb.append("[");
        for (int i = 0; i < length; i++) {
            appendArrayElements(sb, Array.get(current, i), depth + 1);
            if (i < length - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
    }
}