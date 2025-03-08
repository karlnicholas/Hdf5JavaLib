package org.hdf5javalib.dataclass;

import lombok.Getter;
import lombok.ToString;
import org.hdf5javalib.file.dataobject.message.datatype.*;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Getter
@ToString
public class HdfCompound<T> implements HdfData<T> {
    private final Class<T> clazz;
    private final CompoundDatatype datatype;
    private final List<HdfCompoundMember> members;

    public HdfCompound(Class<T> clazz, byte[] bytes, CompoundDatatype datatype) {
        this.clazz = clazz;
        this.datatype = datatype;
        Map<String, Field> nameToFieldMap = Arrays.stream(clazz.getDeclaredFields()).collect(Collectors.toMap(Field::getName, f -> f));
        Map<String, CompoundMemberDatatype> nameToMemberMap = datatype.getMembers().stream().collect(Collectors.toMap(CompoundMemberDatatype::getName, compoundMember -> compoundMember));

        members = new ArrayList<>();
        nameToMemberMap.forEach((name, member) -> {
            Field field = nameToFieldMap.get(name);
//            field.setAccessible(true);
            HdfData hdfData = switch (member.getDatatypeClass()) {
                case FIXED -> new HdfFixedPoint(field.getType(), Arrays.copyOfRange(bytes, member.getOffset(), member.getOffset()+member.getSize()), (FixedPointDatatype) member.getType());
                case FLOAT -> new HdfFloatPoint(field.getType(), Arrays.copyOfRange(bytes, member.getOffset(), member.getOffset()+member.getSize()), (FloatingPointDatatype) member.getType());
                case STRING -> new HdfString<>(field.getType(), Arrays.copyOfRange(bytes, member.getOffset(), member.getOffset()+member.getSize()), (StringDatatype) member.getType());
                default -> throw new IllegalStateException("Unexpected datatype class: " + member.getDatatypeClass());
            };
            HdfCompoundMember hdfMember = new HdfCompoundMember(hdfData.getClass(), member.getName(), hdfData);
            members.add(hdfMember);
        });
    }

    @Override
    public int getSizeMessageData() {
        return members.stream().mapToInt(HdfCompoundMember::getSizeMessageData).sum();
    }

    @Override
    public void writeValueToByteBuffer(ByteBuffer buffer) {
        members.forEach(member -> member.writeValueToByteBuffer(buffer));
    }

    @Override
    public T getInstance() {
        return null;
    }
}
