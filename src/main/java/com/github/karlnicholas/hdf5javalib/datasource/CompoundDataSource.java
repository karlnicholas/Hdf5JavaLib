package com.github.karlnicholas.hdf5javalib.datasource;

import com.github.karlnicholas.hdf5javalib.data.HdfFixedPoint;
import com.github.karlnicholas.hdf5javalib.data.HdfString;
import com.github.karlnicholas.hdf5javalib.file.dataobject.message.datatype.CompoundDatatype;
import com.github.karlnicholas.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import com.github.karlnicholas.hdf5javalib.file.dataobject.message.datatype.HdfCompoundDatatypeMember;
import com.github.karlnicholas.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class CompoundDataSource<T> {
    private final Class<T> clazz;
    private final Map<Field, HdfCompoundDatatypeMember> fieldToMemberMap = new HashMap<>();

    public CompoundDataSource(CompoundDatatype compoundDataType, Class<T> clazz) {
        this.clazz = clazz;

        // Parse fields and map them to CompoundDatatype members
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
            for (Map.Entry<Field, HdfCompoundDatatypeMember> entry : fieldToMemberMap.entrySet()) {
                Field field = entry.getKey();
                HdfCompoundDatatypeMember member = entry.getValue();

                buffer.position(member.getOffset());

                if (field.getType() == String.class && member.getType() instanceof StringDatatype) {
                    String value = ((StringDatatype) member.getType()).getInstance(buffer).getValue();
                    field.set(instance, value);
                } else if (field.getType() == BigInteger.class && member.getType() instanceof FixedPointDatatype) {
                    BigInteger value = ((FixedPointDatatype) member.getType()).getInstance(buffer).toBigInteger();
                    field.set(instance, value);
                }
                // Add more type handling as needed
            }

            return instance;
        } catch (Exception e) {
            throw new RuntimeException("Error creating and populating instance of " + clazz.getName(), e);
        }
    }
    /**
     * Writes the given instance of T into the provided ByteBuffer.
     */
    public void writeToBuffer(T instance, ByteBuffer buffer) {
        try {
            for (Map.Entry<Field, HdfCompoundDatatypeMember> entry : fieldToMemberMap.entrySet()) {
                Field field = entry.getKey();
                HdfCompoundDatatypeMember member = entry.getValue();

                // Move to the correct offset
                buffer.position(member.getOffset());

                Object value = field.get(instance);

                if (value instanceof String strValue && member.getType() instanceof StringDatatype stringDatatype) {
                    // Convert string to bytes and write to buffer
                    ByteBuffer stringBuffer = ByteBuffer.allocate(stringDatatype.getSize());
                    HdfString s = new HdfString(strValue.getBytes(StandardCharsets.US_ASCII), StringDatatype.getStringTypeBitSet(StringDatatype.PaddingType.NULL_PAD, StringDatatype.CharacterSet.ASCII));
                    s.writeValueToByteBuffer(stringBuffer);
                    buffer.put(stringBuffer.array());
                } else if (value instanceof BigInteger bigIntValue && member.getType() instanceof FixedPointDatatype fixedPointDatatype) {
                    // Convert BigInteger to bytes and write to buffer
                    new HdfFixedPoint(bigIntValue.toByteArray(), fixedPointDatatype.getSize(), fixedPointDatatype.isBigEndian(), fixedPointDatatype.isLopad(), fixedPointDatatype.isHipad(), fixedPointDatatype.isSigned(), fixedPointDatatype.getBitOffset(), fixedPointDatatype.getBitPrecision())
                            .writeValueToByteBuffer(buffer);
                }
                // Add more type handling as needed

            }
        } catch (Exception e) {
            throw new RuntimeException("Error writing instance of " + clazz.getName() + " to ByteBuffer", e);
        }
    }
}
