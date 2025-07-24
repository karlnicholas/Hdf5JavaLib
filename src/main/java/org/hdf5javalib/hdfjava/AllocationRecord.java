package org.hdf5javalib.hdfjava;

// --- Helper Classes ---

import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.utils.HdfWriteUtils;

/**
 * Represents a single allocation record with type, name, offset, and size.
 */
public class AllocationRecord {
    private AllocationType type;
    private String name;
    private HdfFixedPoint offset;
    private HdfFixedPoint size;

    /**
     * Constructs an allocation record.
     *
     * @param type   the allocation type
     * @param name   the name of the allocation
     * @param offset the starting offset
     * @param size   the size of the allocation
     */
    public AllocationRecord(AllocationType type, String name, HdfFixedPoint offset, HdfFixedPoint size, HdfFileAllocation fileAllocation) {
        this.type = type;
        this.name = name;
        this.offset = offset;
        this.size = size;
        // superblock problem
        if (fileAllocation != null) {
            fileAllocation.addAllocationBlock(this);
        }
    }

    /**
     * Gets the allocation type.
     *
     * @return the allocation type
     */
    public AllocationType getType() {
        return type;
    }

    /**
     * Gets the name of the allocation.
     *
     * @return the allocation name
     */
    public String getName() {
        return name;
    }

    /**
     * Gets the starting offset of the allocation.
     *
     * @return the offset
     */
    public HdfFixedPoint getOffset() {
        return offset;
    }

    /**
     * Gets the size of the allocation.
     *
     * @return the size
     */
    public HdfFixedPoint getSize() {
        return size;
    }

    /**
     * Sets the allocation type.
     *
     * @param type the allocation type
     */
    public void setType(AllocationType type) {
        this.type = type;
    }

    /**
     * Sets the name of the allocation.
     *
     * @param name the allocation name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Sets the starting offset of the allocation.
     *
     * @param offset the offset
     */
    public void setOffset(HdfFixedPoint offset) {
        this.offset = offset;
    }

    /**
     * Sets the size of the allocation.
     *
     * @param size the size
     */
    public void setSize(HdfFixedPoint size) {
        this.size = size;
    }

    /**
     * Sets the size of the allocation.
     *
     * @param size the size
     */
    public void setSize(long size) {
        this.size = HdfWriteUtils.hdfFixedPointFromValue(size, this.size.getDatatype());
    }
}
