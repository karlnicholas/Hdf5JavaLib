package com.github.karlnicholas.hdf5javalib.datatype;

import java.nio.ByteBuffer;

public interface HdfDatatype {
    short getSizeMessageData();
    void writeDefinitionToByteBuffer(ByteBuffer buffer);
    int getSize();
    DatatypeClass getDatatypeClass(); // Updated to return enum
    // Enum defined within the interface
    enum DatatypeClass {
        FIXED(0),    // HDF5 integer types
        FLOAT(1),      // HDF5 floating-point types
        TIME(2),       // HDF5 time types
        STRING(3),     // HDF5 string types
        BITFIELD(4),   // HDF5 bitfield types
        OPAQUE(5),     // HDF5 opaque types
        COMPOUND(6),   // HDF5 compound types
        REFERENCE(7),  // HDF5 reference types
        ENUM(8),       // HDF5 enumerated types
        VLEN(9),       // HDF5 variable-length types
        ARRAY(10);     // HDF5 array types

        private final int value;

        DatatypeClass(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        // Optional: Lookup by value if needed for parsing
        public static DatatypeClass fromValue(int value) {
            for (DatatypeClass type : values()) {
                if (type.value == value) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Unknown datatype class value: " + value);
        }
    }
}