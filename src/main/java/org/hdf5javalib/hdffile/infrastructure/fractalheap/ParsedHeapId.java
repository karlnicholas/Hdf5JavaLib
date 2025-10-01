package org.hdf5javalib.hdffile.infrastructure.fractalheap;

//##############################################################################
//### HEAP ID PARSER
//##############################################################################
public class ParsedHeapId {
    public final int type;
    public final int version;
    public final int offset;
    public final int length;

    public ParsedHeapId(byte[] rawId, FractalHeap fractalHeap) {
        BitReader reader = new BitReader(rawId);
        FractalHeap.FractalHeapHeader header = fractalHeap.getHeader();
        int reserved = (int) reader.read(4);
        this.type = (int) reader.read(2);
        this.version = (int) reader.read(2);
        if ( fractalHeap.getHeader().maximumHeapSize == 8 ) {
            this.offset = (int) reader.read(8);
        } else if ( header.maximumHeapSize == 16 ) {
            this.offset = (int) reader.read(16);
        } else if ( header.maximumHeapSize == 24 ) {
            this.offset = (int) reader.read(24);
        } else if ( header.maximumHeapSize == 32 ) {
            this.offset = (int) reader.read(32);
        } else if ( header.maximumHeapSize == 40 ) {
            this.offset = (int) reader.read(40);
        } else {
            throw new IllegalStateException("Cannot parse heap id: header.maximumHeapSize == " +  header.maximumHeapSize);
        }
        long minOfMaxs = Math.min(header.maximumDirectBlockSize, header.sizeOfManagedObjects);
        if (  minOfMaxs < (1<<8) ) {
            this.length = (int) reader.read(8);
        } else if ( minOfMaxs < (1<<16) ) {
            this.length = (int) reader.read(16);
        } else if ( minOfMaxs < (1<<24) ) {
            this.length = (int) reader.read(24);
        } else {
            this.length = (int) reader.read(32);
        }
    }

    @Override
    public String toString() {
        return "ParsedHeapId{" + "type=" + type + ", offset=" + offset +
                ", length=" + length  + '}';
    }

}

