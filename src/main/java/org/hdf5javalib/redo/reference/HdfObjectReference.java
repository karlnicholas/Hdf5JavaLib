package org.hdf5javalib.redo.reference;

import org.hdf5javalib.redo.datatype.ReferenceDatatype;
import org.hdf5javalib.redo.hdffile.dataobjects.HdfDataObject;

public class HdfObjectReference implements HdfReferenceInstance {
    private final boolean external;
    private final HdfDataObject hdfDataObject;

    public HdfObjectReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = external;
        this.hdfDataObject = null;
    }
}
