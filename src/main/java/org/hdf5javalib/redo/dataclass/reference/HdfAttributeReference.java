package org.hdf5javalib.redo.dataclass.reference;

import org.hdf5javalib.redo.datatype.ReferenceDatatype;
import org.hdf5javalib.redo.hdffile.HdfDataFile;
import org.hdf5javalib.redo.utils.HdfDataHolder;

import java.util.List;

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
