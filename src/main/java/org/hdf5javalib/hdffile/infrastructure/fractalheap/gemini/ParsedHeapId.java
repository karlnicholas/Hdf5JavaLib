package org.hdf5javalib.hdffile.infrastructure.fractalheap.gemini;

//##############################################################################
//### HEAP ID PARSER
//##############################################################################
class ParsedHeapId {
    public final int type;
    public final int version;
    public final int offset;
    public final int length;
//    public final int flags;
//    public final long offset;
//    public final long length;
//    public final int row;
//    public final int col;

    public ParsedHeapId(byte[] rawId, FractalHeapHeader header) {
//        Hdf5Utils.BitReader reader = new Hdf5Utils.BitReader(rawId);
//        this.type = (int) reader.read(4);
//        this.flags = (int) reader.read(4);
//
//        IdSpec spec = new IdSpec(header);
//        this.offset = reader.read(spec.offsetBits);
//        this.length = reader.read(spec.lengthBits);
//        this.row = (int) reader.read(spec.rowBits);
//        this.col = (int) reader.read(spec.colBits);
        Hdf5Utils.BitReader reader = new Hdf5Utils.BitReader(rawId);
        int resrved = (int) reader.read(4);
        this.type = (int) reader.read(2);
        this.version = (int) reader.read(2);
        if ( header.maximumHeapSize == 8 ) {
            this.offset = (int) reader.read(8);
        } else if ( header.maximumHeapSize == 16 ) {
            this.offset = (int) reader.read(16);
        } else if ( header.maximumHeapSize == 24 ) {
            this.offset = (int) reader.read(24);
        } else if ( header.maximumHeapSize == 32 ) {
            this.offset = (int) reader.read(32);
        } else {
            throw new IllegalStateException("Cannot parse heap id: header.maximumHeapSize == " +  header.maximumHeapSize);
        }
        long minOfMaxs = Math.min(header.maximumDirectBlockSize, header.maximumSizeOfManagedObjects);
        if (  minOfMaxs < (1<<8) ) {
            this.length = (int) reader.read(8);
        } else if ( minOfMaxs < (1<<16) ) {
            this.length = (int) reader.read(16);
        } else if ( minOfMaxs < (1<<24) ) {
            this.length = (int) reader.read(24);
        } else {
            this.length = (int) reader.read(32);
        }

//        this.type = (int) reader.read(4);
//        this.flags = (int) reader.read(4);
//
//        IdSpec spec = new IdSpec(header);
//        this.offset = reader.read(spec.offsetBits);
//        this.length = reader.read(spec.lengthBits);
//        this.row = (int) reader.read(spec.rowBits);
//        this.col = (int) reader.read(spec.colBits);
        System.out.println(this);

    }

    @Override
    public String toString() {
        return "ParsedHeapId{" + "type=" + type + ", offset=" + offset +
                ", length=" + length  + '}';
    }

    /** Helper to calculate bit lengths for Heap ID fields based on the header */
    static class IdSpec {
        final int offsetBits;
        final int lengthBits;
        final int rowBits;
        final int colBits;

        IdSpec(FractalHeapHeader header) {
            this.offsetBits = Hdf5Utils.ceilLog2(header.maximumDirectBlockSize);
            this.lengthBits = Hdf5Utils.ceilLog2(header.maximumSizeOfManagedObjects + 1);

            int log2MaxDirect = Hdf5Utils.ceilLog2(header.maximumDirectBlockSize);
            int log2Start = Hdf5Utils.ceilLog2(header.startingBlockSize);
            int maxDblockRows = (log2MaxDirect - log2Start) + 2;
            this.rowBits = Hdf5Utils.ceilLog2(maxDblockRows);

            this.colBits = Hdf5Utils.ceilLog2(header.tableWidth);
        }
    }
}

