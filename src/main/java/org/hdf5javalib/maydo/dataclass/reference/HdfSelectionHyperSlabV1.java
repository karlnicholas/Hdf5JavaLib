package org.hdf5javalib.maydo.dataclass.reference;

import org.hdf5javalib.maydo.dataclass.HdfData;
import org.hdf5javalib.maydo.datasource.TypedDataSource;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.hdffile.dataobjects.messages.DataspaceMessage;
import org.hdf5javalib.maydo.hdffile.infrastructure.HdfDataObject;
import org.hdf5javalib.maydo.hdffile.infrastructure.HdfDataSet;
import org.hdf5javalib.maydo.utils.FlattenedArrayUtils;
import org.hdf5javalib.maydo.utils.HdfDataHolder;

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

    @Override
    public String toString() {
        // ... (toString method remains the same)
        StringBuilder sb = new StringBuilder();
        sb.append("HdfSelectionHyperSlabV1{v=").append(version)
                .append(",l=").append(length)
                .append(",r=").append(rank)
                .append(",n=").append(numBlocks)
                .append(",b=[");

        if (numBlocks <= 0 || startOffsets == null || endOffsets == null) {
            sb.append("]");
        } else {
            for (int b = 0; b < numBlocks; b++) {
                if (b > 0) sb.append(",");
                sb.append("B").append(b + 1).append(":(");
                for (int r = 0; r < rank; r++) {
                    sb.append(r > 0 ? "," : "").append(startOffsets[b][r]);
                }
                sb.append(")(");
                for (int r = 0; r < rank; r++) {
                    sb.append(r > 0 ? "," : "").append(endOffsets[b][r]);
                }
                sb.append(")");
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public HdfDataHolder getData(HdfDataObject hdfDataObject, HdfDataFile hdfDataFile) {
        // Cast the data object to a dataset to access its properties
        HdfDataSet hdfDataSet = (HdfDataSet) hdfDataObject;

        // 1. Get the shape and strides of the full source dataset.
        DataspaceMessage dataspaceMessage = hdfDataSet.getDataObjectHeaderPrefix().findMessageByType(DataspaceMessage.class).get();
        int[] sourceShape = Arrays.stream(dataspaceMessage.getDimensions())
                .mapToInt(dim -> dim.getInstance(Long.class).intValue())
                .toArray();
        int[] sourceStrides = FlattenedArrayUtils.computeStrides(sourceShape);

        // 2. Write the predicate to check if an element is inside any hyperslab block.
        // We need a counter to track the flat index of each element in the stream.
        AtomicInteger flatIndex = new AtomicInteger(0);
        Predicate<HdfData> isInSelection = (datum) -> {
            // a. Convert the current flat index to N-D coordinates.
            int[] currentCoords = FlattenedArrayUtils.unflattenIndex(flatIndex.getAndIncrement(), sourceStrides, sourceShape);

            // b. Check if the coordinates fall within ANY of the defined hyperslab blocks.
            for (int b = 0; b < this.numBlocks; b++) {
                boolean isInThisBlock = true;
                // Check each dimension for the current block.
                for (int r = 0; r < this.rank; r++) {
                    // A point is outside the block if its coordinate in any dimension is out of bounds.
                    // The bounds are inclusive [start, end].
                    if (currentCoords[r] < this.startOffsets[b][r] || currentCoords[r] > this.endOffsets[b][r]) {
                        isInThisBlock = false;
                        break; // No need to check other dimensions for this block.
                    }
                }
                // If the point was inside this block, we can return true immediately.
                if (isInThisBlock) {
                    return true;
                }
            }
            // If the loops complete, the point was not in any of the defined blocks.
            return false;
        };

        // 3. Get the flattened data source stream and apply the filter.
        TypedDataSource<HdfData> dataSource = new TypedDataSource<>(hdfDataFile.getSeekableByteChannel(), hdfDataFile, hdfDataSet, HdfData.class);
        Stream<HdfData> filteredStream = dataSource.streamFlattened().filter(isInSelection);

        // 4. Calculate the shape of the output array.
        int[] resultShape;
        // The common case: a single rectangular hyperslab.
        if (this.numBlocks == 1) {
            resultShape = new int[this.rank];
            for (int i = 0; i < this.rank; i++) {
                // The size of the dimension is (end - start + 1).
                resultShape[i] = this.endOffsets[0][i] - this.startOffsets[0][i] + 1;
            }
            // Use the utility to reshape the filtered stream into a proper N-D array.
            Object resultArray = FlattenedArrayUtils.streamToNDArray(filteredStream, resultShape, HdfData.class);
            return HdfDataHolder.ofArray(resultArray, resultShape);

        } else {
            // For multiple, discontinuous blocks, a flattened 1D array is the most logical representation.
            HdfData[] resultArray = filteredStream.toArray(HdfData[]::new);
            resultShape = new int[]{resultArray.length};
            return HdfDataHolder.ofArray(resultArray, resultShape);
        }
    }
}