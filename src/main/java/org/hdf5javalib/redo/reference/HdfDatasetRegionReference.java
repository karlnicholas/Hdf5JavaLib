package org.hdf5javalib.redo.reference;

import org.hdf5javalib.redo.datatype.ReferenceDatatype;

public class HdfDatasetRegionReference implements HdfReferenceInstance {
    private final boolean external;
    public HdfDatasetRegionReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = external;
    }
}
