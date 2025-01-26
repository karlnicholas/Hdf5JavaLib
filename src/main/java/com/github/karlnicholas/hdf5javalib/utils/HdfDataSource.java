package com.github.karlnicholas.hdf5javalib.utils;

import com.github.karlnicholas.hdf5javalib.datatype.CompoundDataType;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

public class HdfDataSource<T extends HdfDataSource<T>> {
    private final CompoundDataType compoundDataType;

    public HdfDataSource(CompoundDataType compoundDataType) {
        this.compoundDataType = compoundDataType;
    }

    /**
     * Populates the subclass fields with values derived from the buffer.
     */
    @SuppressWarnings("unchecked")
    public T populateFromBuffer(ByteBuffer buffer) {
        try {
            // Create a new instance of the subclass
            Class<T> clazz = (Class<T>) this.getClass();
            T instance = clazz.getDeclaredConstructor(CompoundDataType.class).newInstance(compoundDataType);

            // Get all declared fields of the subclass
            Field[] fields = clazz.getDeclaredFields();

            for (Field field : fields) {
                field.setAccessible(true);
                compoundDataType.getMembers().stream().filter(member->member.getName().equals(field.getName())).findFirst().ifPresent(member->{
                    buffer.position(member.getOffset());
                    // Example logic: Populate field based on the field name and buffer
                    if (field.getType() == String.class) {
                        if ( member.getType() instanceof CompoundDataType.StringMember ) {
                            String value = ((CompoundDataType.StringMember)member.getType()).getInstance(buffer).getValue();
                            try {
                                field.set(instance, value);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else if ( field.getType() == BigInteger.class) {
                        if ( member.getType() instanceof CompoundDataType.FixedPointMember ) {
                            BigInteger value = ((CompoundDataType.FixedPointMember)member.getType()).getInstance(buffer).getBigIntegerValue();
                            try {
                                field.set(instance, value);
                            } catch (IllegalAccessException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                });

                // Additional logic for other types can be added here
            }

            return instance;
        } catch (Exception e) {
            throw new RuntimeException("Error populating instance from buffer", e);
        }
    }
}
