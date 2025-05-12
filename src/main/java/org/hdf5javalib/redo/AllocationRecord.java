package org.hdf5javalib.redo;

// --- Helper Classes ---

/**
 * Represents a single allocation record with type, name, offset, and size.
 */
public class AllocationRecord {
    private AllocationType type;
    private String name;
    private long offset;
    private long size;

    /**
     * Constructs an allocation record.
     *
     * @param type   the allocation type
     * @param name   the name of the allocation
     * @param offset the starting offset
     * @param size   the size of the allocation
     */
    public AllocationRecord(AllocationType type, String name, long offset, long size) {
        this.type = type;
        this.name = name;
        this.offset = offset;
        this.size = size;
    }

    /**
     * Gets the allocation type.
     *
     * @return the allocation type
     */
    public AllocationType getType() { return type; }

    /**
     * Gets the name of the allocation.
     *
     * @return the allocation name
     */
    public String getName() { return name; }

    /**
     * Gets the starting offset of the allocation.
     *
     * @return the offset
     */
    public long getOffset() { return offset; }

    /**
     * Gets the size of the allocation.
     *
     * @return the size
     */
    public long getSize() { return size; }

    /**
     * Sets the allocation type.
     *
     * @param type the allocation type
     */
    public void setType(AllocationType type) { this.type = type; }

    /**
     * Sets the name of the allocation.
     *
     * @param name the allocation name
     */
    public void setName(String name) { this.name = name; }

    /**
     * Sets the starting offset of the allocation.
     *
     * @param offset the offset
     */
    public void setOffset(long offset) { this.offset = offset; }

    /**
     * Sets the size of the allocation.
     *
     * @param size the size
     */
    public void setSize(long size) { this.size = size; }
}
