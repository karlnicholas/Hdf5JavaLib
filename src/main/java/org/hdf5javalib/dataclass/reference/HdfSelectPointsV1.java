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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HdfSelectPointsV1 extends HdfDataspaceSelectionInstance {
    private final int version;
    private final int length;
    private final int rank;
    private final long numPoints;
    private final int[][] values;

    public HdfSelectPointsV1(int version, int length, int rank, long numPoints, int[][] values) {
        this.version = version;
        this.length = length;
        this.rank = rank;
        this.numPoints = numPoints;
        this.values = values;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HdfSelectPointsV1{v=").append(version)
                .append(",r=").append(rank)
                .append(",n=").append(numPoints)
                .append(",p=[");

        if (numPoints <= 0 || values == null) {
            sb.append("]");
        } else {
            for (int i = 0; i < numPoints; i++) {
                if (i > 0) sb.append(",");
                sb.append("P").append(i + 1).append(":(");
                for (int j = 0; j < rank; j++) {
                    sb.append(j > 0 ? "," : "").append(values[i][j]);
                }
                sb.append(")");
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }

    @Override
    public HdfDataHolder getData(HdfDataObject hdfDataObject, HdfDataFile hdfDataFile) throws IOException, InvocationTargetException, InstantiationException, IllegalAccessException {
        // Cast the data object to a dataset to access its properties
        HdfDataset hdfDataSet = (HdfDataset) hdfDataObject;

        // 1. Get the shape of the full source dataset.
        DataspaceMessage dataspaceMessage = hdfDataSet.getObjectHeader().findMessageByType(DataspaceMessage.class).get();
        int[] arr = new int[10];
        int count = 0;
        for (HdfFixedPoint dim : dataspaceMessage.getDimensions()) {
            int i = dim.getInstance(Long.class).intValue();
            if (arr.length == count) arr = Arrays.copyOf(arr, count * 2);
            arr[count++] = i;
        }
        arr = Arrays.copyOfRange(arr, 0, count);
        int[] sourceShape = arr;
        int[] sourceStrides = FlattenedArrayUtils.computeStrides(sourceShape);

        // 2. For efficient O(1) lookups, convert the target int[][] coordinates to a Set<List<Integer>>.
        // A List<Integer> has a content-based hashCode/equals, unlike int[].
        Set<List<Integer>> targetPointsSet = Arrays.stream(this.values)
                .map(pointCoords -> Arrays.stream(pointCoords).boxed().toList())
                .collect(Collectors.toSet());

        // 3. Get the flattened data source stream.
        TypedDataSource<HdfData> dataSource = new TypedDataSource<>(hdfDataFile.getSeekableByteChannel(), hdfDataFile, hdfDataSet, HdfData.class);
        Stream<HdfData> flattenedStream = dataSource.streamFlattened();

        // 4. Write the predicate to filter the stream.
        // We need a counter to track the flat index of each element in the stream.
        AtomicInteger flatIndex = new AtomicInteger(0);

        Predicate<HdfData> isInSelection = (datum) -> {
            // a. Get the current flat index.
            int currentFlatIndex = flatIndex.getAndIncrement();
            // b. Convert the flat index to N-D coordinates using the utility.
            int[] currentCoords = FlattenedArrayUtils.unflattenIndex(currentFlatIndex, sourceStrides, sourceShape);
            // c. Convert to a List for Set comparison.
            List<Integer> coordList = Arrays.stream(currentCoords).boxed().toList();
            // d. The predicate is true if the coordinates are in our target set.
            return targetPointsSet.contains(coordList);
        };

        // 5. Filter the stream and collect the results into a new array.
        // The result is an array of HdfData objects matching the selected points.
        HdfData[] resultArray = flattenedStream.filter(isInSelection).toArray(HdfData[]::new);

        // 6. The shape of the result is a simple 1D array with a length equal to the number of points.
        int[] resultShape = new int[]{Math.toIntExact(this.numPoints)};

        // Wrap the resulting array and its shape in the HdfDataHolder and return.
        return HdfDataHolder.ofArray(resultArray, resultShape);
    }
}