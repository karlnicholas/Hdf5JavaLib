package org.hdf5javalib.file.dataobject.message.datatype;

// Converter interface
public interface HdfConverter<D extends HdfDatatype, T> {
    T convert(byte[] bytes, D datatype);
}