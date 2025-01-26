package com.github.karlnicholas.hdf5javalib.utils;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class HdfDataSource<T> {
    private final CompoundDataType compoundDataType;
    private final Class<T> clazz;
    private final Map<Field, CompoundDataType.Member> fieldToMemberMap = new HashMap<>();

    public HdfDataSource(CompoundDataType compoundDataType, Class<T> clazz) {
        this.compoundDataType = compoundDataType;
        this.clazz = clazz;

        // Parse fields and map them to CompoundDataType members
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);

            compoundDataType.getMembers().stream()
                    .filter(member -> member.getName().equals(field.getName()))
                    .findFirst()
                    .ifPresent(member -> fieldToMemberMap.put(field, member));
        }
    }

    /**
     * Populates a new instance of T with data from the buffer.
     */
    public T populateFromBuffer(ByteBuffer buffer) {
        try {
            // Create an instance of T
            T instance = clazz.getDeclaredConstructor().newInstance();

            // Populate fields using the pre-parsed map
            for (Map.Entry<Field, CompoundDataType.Member> entry : fieldToMemberMap.entrySet()) {
                Field field = entry.getKey();
                CompoundDataType.Member member = entry.getValue();

                buffer.position(member.getOffset());

                if (field.getType() == String.class && member.getType() instanceof CompoundDataType.StringMember) {
                    String value = ((CompoundDataType.StringMember) member.getType()).getInstance(buffer).getValue();
                    field.set(instance, value);
                } else if (field.getType() == BigInteger.class && member.getType() instanceof CompoundDataType.FixedPointMember) {
                    BigInteger value = ((CompoundDataType.FixedPointMember) member.getType()).getInstance(buffer).getBigIntegerValue();
                    field.set(instance, value);
                }
                // Add more type handling as needed
            }

            return instance;
        } catch (Exception e) {
            throw new RuntimeException("Error creating and populating instance of " + clazz.getName(), e);
        }
    }
}
