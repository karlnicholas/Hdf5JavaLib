package org.hdf5javalib.dataclass.reference;

import org.hdf5javalib.datatype.ReferenceDatatype;
import org.hdf5javalib.utils.HdfDataHolder;

public class HdfAttributeReference implements HdfReferenceInstance {
    private final boolean external;
    private final byte[] bytes;
    private final ReferenceDatatype dt;

    public HdfAttributeReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = external;
        this.bytes = bytes;
        this.dt = dt;
    }

    @Override
    public HdfDataHolder getData() {
        throw new UnsupportedOperationException("Attribute Reference not supported yet.");
    }
}
