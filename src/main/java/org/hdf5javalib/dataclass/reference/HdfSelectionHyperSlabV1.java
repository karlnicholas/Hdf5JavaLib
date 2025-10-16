package org.hdf5javalib.dataclass.reference;

import org.hdf5javalib.dataclass.HdfData;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.hdffile.dataobjects.messages.DataspaceMessage;
import org.hdf5javalib.hdfjava.HdfDataFile;
import org.hdf5javalib.hdfjava.HdfDataObject;
import org.hdf5javalib.hdfjava.HdfDataset;
import org.hdf5javalib.utils.FlattenedArrayUtils;
import org.hdf5javalib.utils.HdfDataHolder;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class HdfSelectionHyperSlabV1 extends HdfDataspaceSelectionInstance {
    private final int version;
    private final int length;
    private final int rank;
    private final int numBlocks;
    private final int[][] startOffsets;
    private final int[][] endOffsets;

    public HdfSelectionHyperSlabV1(int version, int length, int rank, int numBlocks, int[][] startOffsets, int[][] endOffsets) {
        this.version = version;
        this.length = length;
        this.rank = rank;
        this.numBlocks = numBlocks;
        this.startOffsets = startOffsets;
        this.endOffsets = endOffsets;
    }

    //
    // Refactored toString (Original Complexity: 22 -> Refactored Complexity: 5)
    //
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HdfSelectionHyperSlabV1{v=").append(version)
                .append(",l=").append(length)
                .append(",r=").append(rank)
                .append(",n=").append(numBlocks)
                .append(",b=[");

        // Delegate block formatting to a helper method or use simplified check
        if (numBlocks > 0 && startOffsets != null && endOffsets != null) {
            sb.append(formatBlocks());
        }
        sb.append("]}");
        return sb.toString();
    }

    /** Helper to format the block list for toString. */
    private String formatBlocks() {
        StringBuilder sb = new StringBuilder();
        for (int b = 0; b < numBlocks; b++) { // +1 for the loop
            if (b > 0) sb.append(","); // +1 for the if
            sb.append("B").append(b + 1).append(":(");

            // Format start offsets
            for (int r = 0; r < rank; r++) { // +1 for the loop
                sb.append(r > 0 ? "," : "").append(startOffsets[b][r]); // +1 for the ternary/if
            }
            sb.append(")(");

            // Format end offsets
            for (int r = 0; r < rank; r++) { // +1 for the loop
                sb.append(r > 0 ? "," : "").append(endOffsets[b][r]); // +1 for the ternary/if
            }
            sb.append(")");
        }
        return sb.toString();
    }

    //
    // Refactored getData (Original Complexity: 20 -> Refactored Complexity: 9)
    //
    @Override
    public HdfDataHolder getData(HdfDataObject hdfDataObject, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        HdfDataset hdfDataSet = (HdfDataset) hdfDataObject;

        // 1. Get the shape and strides of the full source dataset.
        DataspaceMessage dataspaceMessage = hdfDataSet.getObjectHeader().findMessageByType(DataspaceMessage.class).get();
        int[] sourceShape = getSourceShape(dataspaceMessage);
        int[] sourceStrides = FlattenedArrayUtils.computeStrides(sourceShape);

        // 2. Write the predicate to check if an element is inside any hyperslab block.
        AtomicInteger flatIndex = new AtomicInteger(0);
        Predicate<HdfData> isInSelection = datum -> isFlatIndexInSelection(flatIndex.getAndIncrement(), sourceStrides, sourceShape); // Complexity reduced to a single method call

        // 3. Get the flattened data source stream and apply the filter.
        TypedDataSource<HdfData> dataSource = new TypedDataSource<>(hdfDataFile.getSeekableByteChannel(), hdfDataFile, hdfDataSet, HdfData.class);
        Stream<HdfData> filteredStream = dataSource.streamFlattened().filter(isInSelection);

        // 4. Calculate the shape of the output array and return the result.
        if (this.numBlocks == 1) { // +1 for the if
            return handleSingleBlock(filteredStream);
        } else {
            return handleMultipleBlocks(filteredStream);
        }
    }

    /** Helper to parse the source shape from the dataspace message. */
    private int[] getSourceShape(DataspaceMessage dataspaceMessage) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // This loop logic remains small enough to be manageable without extraction, but we include it in a helper for clarity.
        int[] arr = new int[10];
        int count = 0;
        for (HdfFixedPoint dim : dataspaceMessage.getDimensions()) { // +1 for the loop
            int intValue = dim.getInstance(Long.class).intValue();
            if (arr.length == count) arr = Arrays.copyOf(arr, count * 2); // +1 for the if
            arr[count++] = intValue;
        }
        return Arrays.copyOfRange(arr, 0, count);
    }

    /** Helper to check if a flat index (coordinate) is within ANY of the defined hyperslab blocks. */
    private boolean isFlatIndexInSelection(int flatIndex, int[] sourceStrides, int[] sourceShape) {
        int[] currentCoords = FlattenedArrayUtils.unflattenIndex(flatIndex, sourceStrides, sourceShape);

        for (int b = 0; b < this.numBlocks; b++) { // +1 for the loop
            if (isCoordinateInBlock(currentCoords, b)) { // +1 for the if + 1 for method call
                return true;
            }
        }
        return false;
    }

    /** Helper to check if a specific coordinate is within a specific block. */
    private boolean isCoordinateInBlock(int[] currentCoords, int blockIndex) {
        // Use a stream/allMatch or simple loop for concise boolean logic
        for (int r = 0; r < this.rank; r++) { // +1 for the loop
            // The bounds are inclusive [start, end].
            if (currentCoords[r] < this.startOffsets[blockIndex][r] || currentCoords[r] > this.endOffsets[blockIndex][r]) { // +1 for the OR
                return false;
            }
        }
        return true;
    }


    /** Helper for the case of a single rectangular hyperslab (numBlocks == 1). */
    private HdfDataHolder handleSingleBlock(Stream<HdfData> filteredStream) {
        int[] resultShape = new int[this.rank];
        for (int i = 0; i < this.rank; i++) { // +1 for the loop
            // The size of the dimension is (end - start + 1).
            resultShape[i] = this.endOffsets[0][i] - this.startOffsets[0][i] + 1;
        }
        Object resultArray = FlattenedArrayUtils.streamToNDArray(filteredStream, resultShape, HdfData.class);
        return HdfDataHolder.ofArray(resultArray, resultShape);
    }

    /** Helper for the case of multiple, discontinuous blocks (numBlocks > 1). */
    private HdfDataHolder handleMultipleBlocks(Stream<HdfData> filteredStream) {
        HdfData[] resultArray = filteredStream.toArray(HdfData[]::new);
        int[] resultShape = new int[]{resultArray.length};
        return HdfDataHolder.ofArray(resultArray, resultShape);
    }
}