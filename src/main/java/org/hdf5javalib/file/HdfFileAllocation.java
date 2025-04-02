package org.hdf5javalib.file;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.infrastructure.HdfLocalHeapContents;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class HdfFileAllocation {
    private final List<Allocation> allocations = new ArrayList<>();
    private long lastAddressUsed;
    private static final long MIN_FILE_SIZE = 2048;
    private static final int ALIGNMENT = 8; // HDF5 often uses 8-byte alignment

    public enum AllocationType {
        SUPERBLOCK,
        OBJECT_HEADER_PREFIX,
        BTREE,
        LOCAL_HEAP,
        LOCAL_HEAP_CONTENTS,
        DATA_GROUP,
        SNOD,
        MESSAGE_CONTINUATION,
        DATA_SEGMENT,
        GLOBAL_HEAP
    }

    public interface AllocationPolicy {
        boolean isMinimum();
        long getDefaultSize();
        long calculateNewSize(long currentSize, long requiredSpace);
    }

    private static class FixedSizePolicy implements AllocationPolicy {
        private final boolean isMinimum;
        private final long size;

        FixedSizePolicy(boolean isMinimum, long size) {
            this.isMinimum = isMinimum;
            this.size = size;
        }

        @Override
        public boolean isMinimum() { return isMinimum; }
        @Override
        public long getDefaultSize() { return size; }
        @Override
        public long calculateNewSize(long currentSize, long requiredSpace) { return currentSize; }
    }

    private static class DoublingSizePolicy implements AllocationPolicy {
        private final boolean isMinimum;
        private final long defaultSize;

        DoublingSizePolicy(boolean isMinimum, long defaultSize) {
            this.isMinimum = isMinimum;
            this.defaultSize = defaultSize;
        }

        @Override
        public boolean isMinimum() { return isMinimum; }
        @Override
        public long getDefaultSize() { return defaultSize; }
        @Override
        public long calculateNewSize(long currentSize, long requiredSpace) {
            return Math.max(currentSize * 2, currentSize + requiredSpace);
        }
    }

    private static class ChunkedSizePolicy implements AllocationPolicy {
        private final boolean isMinimum;
        private final long defaultSize;
        private final long chunkSize;

        ChunkedSizePolicy(boolean isMinimum, long defaultSize, long chunkSize) {
            this.isMinimum = isMinimum;
            this.defaultSize = defaultSize;
            this.chunkSize = chunkSize;
        }

        @Override
        public boolean isMinimum() { return isMinimum; }
        @Override
        public long getDefaultSize() { return defaultSize; }
        @Override
        public long calculateNewSize(long currentSize, long requiredSpace) {
            long increment = ((requiredSpace + chunkSize - 1) / chunkSize) * chunkSize;
            return currentSize + increment;
        }
    }

    private static class ExactSizePolicy implements AllocationPolicy {
        private final boolean isMinimum;
        private final long defaultSize;

        ExactSizePolicy(boolean isMinimum, long defaultSize) {
            this.isMinimum = isMinimum;
            this.defaultSize = defaultSize;
        }

        @Override
        public boolean isMinimum() { return isMinimum; }
        @Override
        public long getDefaultSize() { return defaultSize; }
        @Override
        public long calculateNewSize(long currentSize, long requiredSpace) { return requiredSpace; }
    }

    private final Map<AllocationType, AllocationPolicy> allocationPolicies = new HashMap<>();

    public HdfFileAllocation() {
        initializePolicies();
        initializeMinimumAllocations();
        recalculateAddresses();
    }

    private void initializePolicies() {
        allocationPolicies.put(AllocationType.SUPERBLOCK, new FixedSizePolicy(true, 96));
        allocationPolicies.put(AllocationType.OBJECT_HEADER_PREFIX, new FixedSizePolicy(true, 40));
        allocationPolicies.put(AllocationType.BTREE, new FixedSizePolicy(true, 544));
        allocationPolicies.put(AllocationType.LOCAL_HEAP, new FixedSizePolicy(true, 120));
        allocationPolicies.put(AllocationType.LOCAL_HEAP_CONTENTS, new DoublingSizePolicy(false, 88));
        allocationPolicies.put(AllocationType.DATA_GROUP, new FixedSizePolicy(true, 272));
        allocationPolicies.put(AllocationType.SNOD, new FixedSizePolicy(false, 328));
        allocationPolicies.put(AllocationType.MESSAGE_CONTINUATION, new ExactSizePolicy(true, 0));
        allocationPolicies.put(AllocationType.DATA_SEGMENT, new ChunkedSizePolicy(true, 256, 256));
        allocationPolicies.put(AllocationType.GLOBAL_HEAP, new DoublingSizePolicy(false, 256));
    }

    private void initializeMinimumAllocations() {
        for (Map.Entry<AllocationType, AllocationPolicy> entry : allocationPolicies.entrySet()) {
            if (entry.getValue().isMinimum()) {
                addAllocation(entry.getKey());
            }
        }
    }

    private void recalculateAddresses() {
        long currentAddress = 0;
        for (Allocation alloc : allocations) {
            alloc.setAddress(currentAddress);
            if (alloc.getSize() > 0) {
                currentAddress += alloc.getSize();
                currentAddress = alignAddress(currentAddress);
            }
        }
        lastAddressUsed = currentAddress;
        if (lastAddressUsed < MIN_FILE_SIZE) {
            Allocation dataSegment = getAllocation(AllocationType.DATA_SEGMENT, 0);
            dataSegment.setAddress(MIN_FILE_SIZE);
            lastAddressUsed = MIN_FILE_SIZE;
            currentAddress = MIN_FILE_SIZE;
            boolean pastDataSegment = false;
            for (Allocation alloc : allocations) {
                if (alloc.getType() == AllocationType.DATA_SEGMENT && alloc.getIndex() == 0) {
                    pastDataSegment = true;
                }
                if (pastDataSegment) {
                    alloc.setAddress(currentAddress);
                    if (alloc.getSize() > 0) {
                        currentAddress += alloc.getSize();
                        currentAddress = alignAddress(currentAddress);
                    }
                }
            }
            lastAddressUsed = currentAddress;
        }
    }

    private long alignAddress(long address) {
        return ((address + ALIGNMENT - 1) & ~(ALIGNMENT - 1));
    }

    public void addAllocation(AllocationType type) {
        int index = countInstances(type);
        long size = allocationPolicies.get(type).getDefaultSize();
        allocations.add(new Allocation(type, 0, size, index));
        recalculateAddresses();
    }

    public boolean removeAllocation(AllocationType type, int index) {
        Allocation toRemove = getAllocation(type, index);
        if (toRemove != null && !allocationPolicies.get(type).isMinimum()) {
            allocations.remove(toRemove);
            recalculateAddresses();
            return true;
        }
        return false;
    }

    public void resizeAllocation(AllocationType type, int index, long requiredSpace) {
        Allocation alloc = getAllocation(type, index);
        long currentSize = alloc.getSize();
        long newSize = allocationPolicies.get(type).calculateNewSize(currentSize, requiredSpace);
        alloc.setSize(newSize);
        recalculateAddresses();
    }

    public Allocation getAllocation(AllocationType type, int index) {
        for (Allocation alloc : allocations) {
            if (alloc.getType() == type && alloc.getIndex() == index) {
                return alloc;
            }
        }
        throw new IllegalArgumentException("No allocation found for type " + type + " at index " + index);
    }

    public Allocation getAllocation(AllocationType type) {
        return getAllocation(type, 0);
    }

    public HeapResizeResult resizeHeap(HdfLocalHeapContents oldContents, int freeListOffset, int requiredSpace) {
        Allocation localHeapContents = getAllocation(AllocationType.LOCAL_HEAP_CONTENTS, 0);
        byte[] oldHeapData = oldContents.getHeapData();
        int currentSize = oldHeapData.length;
        long newSize = allocationPolicies.get(AllocationType.LOCAL_HEAP_CONTENTS).calculateNewSize(currentSize, requiredSpace);
        byte[] newHeapData = new byte[(int) newSize];
        System.arraycopy(oldHeapData, 0, newHeapData, 0, oldHeapData.length);
        HdfLocalHeapContents newContents = new HdfLocalHeapContents(newHeapData);
        resizeAllocation(AllocationType.LOCAL_HEAP_CONTENTS, 0, requiredSpace);
        return new HeapResizeResult(newContents, HdfFixedPoint.of(localHeapContents.getAddress()));
    }

    public void setDataGroupAndContinuationStorageSize(long objectHeaderSize, long continueSize) {
        resizeAllocation(AllocationType.DATA_GROUP, 0, 16 + objectHeaderSize);
        resizeAllocation(AllocationType.MESSAGE_CONTINUATION, 0, continueSize);
    }

    public int expandDataGroupStorageSize(int objectHeaderSize) {
        Allocation dataGroup = getAllocation(AllocationType.DATA_GROUP, 0);
        long newSize = allocationPolicies.get(AllocationType.DATA_GROUP).calculateNewSize(dataGroup.getSize(), 16 + objectHeaderSize);
        resizeAllocation(AllocationType.DATA_GROUP, 0, 16 + objectHeaderSize);
        return (int) (newSize - 16);
    }

    public void computeGlobalHeap(long requiredSize) {
        Allocation dataSegment = getAllocation(AllocationType.DATA_SEGMENT, 0);
        long globalHeapAddress = dataSegment.getAddress() + dataSegment.getSize();
        Allocation globalHeap;
        try {
            globalHeap = getAllocation(AllocationType.GLOBAL_HEAP, 0);
            resizeAllocation(AllocationType.GLOBAL_HEAP, 0, requiredSize);
        } catch (IllegalArgumentException e) {
            long initialSize = allocationPolicies.get(AllocationType.GLOBAL_HEAP).calculateNewSize(0, requiredSize);
            globalHeap = new Allocation(AllocationType.GLOBAL_HEAP, globalHeapAddress, initialSize, 0);
            allocations.add(globalHeap);
        }
        globalHeap.setAddress(globalHeapAddress);
        recalculateAddresses();
    }

    public HeapResizeResult createLocalHeapContents(long requiredSize) {
        Allocation localHeapContents;
        try {
            localHeapContents = getAllocation(AllocationType.LOCAL_HEAP_CONTENTS, 0);
            resizeAllocation(AllocationType.LOCAL_HEAP_CONTENTS, 0, requiredSize);
        } catch (IllegalArgumentException e) {
            long initialSize = allocationPolicies.get(AllocationType.LOCAL_HEAP_CONTENTS).calculateNewSize(0, requiredSize);
            localHeapContents = new Allocation(AllocationType.LOCAL_HEAP_CONTENTS, 0, initialSize, 0);
            allocations.add(localHeapContents);
            recalculateAddresses();
        }
        byte[] heapData = new byte[(int) localHeapContents.getSize()];
        HdfLocalHeapContents newContents = new HdfLocalHeapContents(heapData);
        return new HeapResizeResult(newContents, HdfFixedPoint.of(localHeapContents.getAddress()));
    }

    public HeapResizeResult createLocalHeapContents() {
        return createLocalHeapContents(allocationPolicies.get(AllocationType.LOCAL_HEAP_CONTENTS).getDefaultSize());
    }

    private int countInstances(AllocationType type) {
        return (int) allocations.stream().filter(a -> a.getType() == type).count();
    }

    public void dumpAllocations() {
        allocations.forEach(a ->
                System.out.println(a.getType() + " [" + a.getIndex() + "]: Address=" + a.getAddress() + ", Size=" + a.getSize()));
    }

    @Getter
    public static class Allocation {
        private final AllocationType type;
        private long address;
        private long size;
        private final int index;

        public Allocation(AllocationType type, long address, long size, int index) {
            this.type = type;
            this.address = address;
            this.size = size;
            this.index = index;
        }

        public void setAddress(long address) { this.address = address; }
        public void setSize(long size) { this.size = size; }
    }

    public static class HeapResizeResult {
        private final HdfLocalHeapContents newContents;
        private final HdfFixedPoint newAddress;

        public HeapResizeResult(HdfLocalHeapContents newContents, HdfFixedPoint newAddress) {
            this.newContents = newContents;
            this.newAddress = newAddress;
        }

        public HdfLocalHeapContents getNewContents() { return newContents; }
        public HdfFixedPoint getNewAddress() { return newAddress; }
    }
}