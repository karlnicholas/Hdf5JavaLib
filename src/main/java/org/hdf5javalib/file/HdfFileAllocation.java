package org.hdf5javalib.file;

import lombok.Getter;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.file.infrastructure.HdfLocalHeapContents;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Getter
public class HdfFileAllocation {
    private final List<Allocation> allocations = new ArrayList<>();
    private long lastAddressUsed;
    private static final long MIN_FILE_SIZE = 2048;
    private static final int ALIGNMENT = 8; // HDF5 often uses 8-byte alignment

    public enum AllocationType {
        SUPERBLOCK(true, 96),           // Fixed size per spec
        OBJECT_HEADER_PREFIX(true, 40), // Initial group metadata
        BTREE(true, 544),              // 32 header + 512 storage
        LOCAL_HEAP(true, 120),         // 32 header + 88 contents
        DATA_GROUP(true, 272),         // 16 header + 256 storage
        SNOD(true, 328),              // 8 header + 320 (10 * 32) entries
        MESSAGE_CONTINUATION(true, 0), // Grows as needed
        DATA_SEGMENT(true, 256),       // Default dataset size
        GLOBAL_HEAP(false, 256);       // Default global heap size

        private final boolean isMinimum;
        private final long defaultSize;

        AllocationType(boolean isMinimum, long defaultSize) {
            this.isMinimum = isMinimum;
            this.defaultSize = defaultSize;
        }

        public boolean isMinimum() { return isMinimum; }
        public long getDefaultSize() { return defaultSize; }
    }

    public HdfFileAllocation() {
        initializeMinimumAllocations();
        recalculateAddresses();
    }

    private void initializeMinimumAllocations() {
        for (AllocationType type : AllocationType.values()) {
            if (type.isMinimum()) {
                addAllocation(type);
            }
        }
    }

    private void recalculateAddresses() {
        long currentAddress = 0;
        for (Allocation alloc : allocations) {
            alloc.setAddress(currentAddress);
            if (alloc.getSize() > 0) {
                currentAddress += alloc.getSize();
                currentAddress = alignAddress(currentAddress); // Ensure 8-byte alignment
            }
        }
        lastAddressUsed = currentAddress;
        if (lastAddressUsed < MIN_FILE_SIZE) {
            Allocation dataSegment = getAllocation(AllocationType.DATA_SEGMENT).orElseThrow();
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
        long size = type.getDefaultSize(); // Use default size from enum
        allocations.add(new Allocation(type, 0, size, index));
        recalculateAddresses();
    }

    public boolean removeAllocation(AllocationType type, int index) {
        Allocation toRemove = getAllocation(type, index).orElse(null);
        if (toRemove != null && !type.isMinimum()) {
            allocations.remove(toRemove);
            recalculateAddresses();
            return true;
        }
        return false;
    }

    public void resizeAllocation(AllocationType type, int index, long requiredSpace) {
        getAllocation(type, index).ifPresent(alloc -> {
            long currentSize = alloc.getSize();
            long newSize = calculateNewSize(type, currentSize, requiredSpace);
            alloc.setSize(newSize);
            recalculateAddresses();
        });
    }

    private long calculateNewSize(AllocationType type, long currentSize, long requiredSpace) {
        switch (type) {
            case LOCAL_HEAP:
            case GLOBAL_HEAP:
                // Double size or add required space, whichever is larger
                return Math.max(currentSize * 2, currentSize + requiredSpace);
            case DATA_SEGMENT:
                // Add required space in 256-byte increments
                long increment = ((requiredSpace + 255) / 256) * 256;
                return currentSize + increment;
            case MESSAGE_CONTINUATION:
                // Exact size needed
                return requiredSpace;
            default:
                // Fixed-size types don’t resize
                return currentSize;
        }
    }

    public Optional<Allocation> getAllocation(AllocationType type, int index) {
        return allocations.stream()
                .filter(a -> a.getType() == type && a.getIndex() == index)
                .findFirst();
    }

    public Optional<Allocation> getAllocation(AllocationType type) {
        return getAllocation(type, 0);
    }

    public HeapResizeResult resizeHeap(HdfLocalHeapContents oldContents, int freeListOffset,
                                       int requiredSpace) {
        Allocation localHeap = getAllocation(AllocationType.LOCAL_HEAP).orElseThrow();
        byte[] oldHeapData = oldContents.getHeapData();
        int currentSize = oldHeapData.length;
        long newSize = calculateNewSize(AllocationType.LOCAL_HEAP, currentSize, requiredSpace);
        byte[] newHeapData = new byte[(int) newSize];
        System.arraycopy(oldHeapData, 0, newHeapData, 0, oldHeapData.length);
        HdfLocalHeapContents newContents = new HdfLocalHeapContents(newHeapData);
        resizeAllocation(AllocationType.LOCAL_HEAP, 0, newSize - currentSize + requiredSpace);
        return new HeapResizeResult(newContents, HdfFixedPoint.of(localHeap.getAddress()));
    }

    public void setDataGroupAndContinuationStorageSize(int objectHeaderSize, int continueSize) {
        resizeAllocation(AllocationType.DATA_GROUP, 0, 16 + objectHeaderSize);
        resizeAllocation(AllocationType.MESSAGE_CONTINUATION, 0, continueSize);
    }

    public int expandDataGroupStorageSize(int objectHeaderSize) {
        Allocation dataGroup = getAllocation(AllocationType.DATA_GROUP).orElseThrow();
        long newSize = calculateNewSize(AllocationType.DATA_GROUP, dataGroup.getSize(), 16 + objectHeaderSize);
        resizeAllocation(AllocationType.DATA_GROUP, 0, newSize);
        return (int) (newSize - 16);
    }

    public void computeGlobalHeapAddress() {
        Allocation dataSegment = getAllocation(AllocationType.DATA_SEGMENT).orElseThrow();
        long globalHeapAddress = dataSegment.getAddress() + dataSegment.getSize();
        addAllocation(AllocationType.GLOBAL_HEAP);
        getAllocation(AllocationType.GLOBAL_HEAP).ifPresent(alloc -> alloc.setAddress(globalHeapAddress));
    }

    private int countInstances(AllocationType type) {
        return (int) allocations.stream()
                .filter(a -> a.getType() == type)
                .count();
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