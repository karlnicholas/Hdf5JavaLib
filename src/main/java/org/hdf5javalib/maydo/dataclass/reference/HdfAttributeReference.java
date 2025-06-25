package org.hdf5javalib.maydo.dataclass.reference;

import org.hdf5javalib.maydo.datatype.ReferenceDatatype;
import org.hdf5javalib.maydo.hdfjava.HdfDataFile;
import org.hdf5javalib.maydo.utils.HdfDataHolder;

public class HdfAttributeReference implements HdfReferenceInstance {
    private final boolean external;

    public HdfAttributeReference(byte[] bytes, ReferenceDatatype dt, boolean external) {
        this.external = external;
    }

    @Override
    public HdfDataHolder getData(HdfDataFile dataFile) {
        return HdfDataHolder.ofScalar(null);
    }
}
