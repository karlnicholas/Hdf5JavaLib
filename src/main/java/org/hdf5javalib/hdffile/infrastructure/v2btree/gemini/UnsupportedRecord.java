package org.hdf5javalib.hdffile.infrastructure.v2btree.gemini;

class UnsupportedRecord implements BTreeV2Record {
    public final BTreeV2Type type;
    public UnsupportedRecord(BTreeV2Type type) { this.type = type; }
    @Override
    public String toString() { return "UnsupportedRecord{type=" + type + '}'; }
}