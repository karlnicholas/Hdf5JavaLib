package org.hdf5javalib.redo.reference;

import org.hdf5javalib.redo.datatype.ReferenceDatatype;

public class HdfAttributeReference implements HdfReferenceInstance {
    private final boolean external;
    public HdfAttributeReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = external;
    }
}
