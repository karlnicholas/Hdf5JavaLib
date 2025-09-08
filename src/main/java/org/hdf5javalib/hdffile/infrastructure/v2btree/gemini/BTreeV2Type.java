package org.hdf5javalib.hdffile.infrastructure.v2btree.gemini;

import java.util.Arrays;

public enum BTreeV2Type {
    TESTING(0),
    HUGE_FRACTAL_HEAP_INDIRECT_V1(1),
    HUGE_FRACTAL_HEAP_INDIRECT_V2(2),
    HUGE_FRACTAL_HEAP_DIRECT_V1(3),
    HUGE_FRACTAL_HEAP_DIRECT_V2(4),
    GROUP_LINK_NAME(5),
    GROUP_LINK_CREATION_ORDER(6),
    SHARED_OBJECT_HEADER_MESSAGES(7),
    ATTRIBUTE_NAME(8),
    ATTRIBUTE_CREATION_ORDER(9),
    DATASET_CHUNK_V1(10),
    DATASET_CHUNK_V2(11);

    public final int value;

    BTreeV2Type(int value) {
        this.value = value;
    }

    public static BTreeV2Type from(int value) {
        return Arrays.stream(values())
                .filter(type -> type.value == value)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown B-tree v2 type: " + value));
    }
}