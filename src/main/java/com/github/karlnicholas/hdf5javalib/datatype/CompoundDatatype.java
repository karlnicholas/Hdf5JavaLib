package com.github.karlnicholas.hdf5javalib.datatype;

import com.github.karlnicholas.hdf5javalib.message.DatatypeMessage;
import lombok.Getter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

@Getter
public class CompoundDatatype implements HdfDatatype {
    private final int numberOfMembers; // Number of members in the compound datatype
    private final int size;
    private List<HdfCompoundDatatypeMember> members;     // Member definitions

    // New application-level constructor
    public CompoundDatatype(int numberOfMembers, int size, List<HdfCompoundDatatypeMember> members) {
        this.numberOfMembers = numberOfMembers;
        this.size = size;
        this.members = new ArrayList<>(members); // Deep copy to avoid external modification
    }

    public CompoundDatatype(BitSet classBitField, int size, byte[] data) {
        this.numberOfMembers = extractNumberOfMembersFromBitSet(classBitField);
        this.size = size;
        ByteBuffer cdtcBuffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        readFromByteBuffer(cdtcBuffer);
    }

    private short extractNumberOfMembersFromBitSet(BitSet classBitField) {
        short value = 0;
        for (int i = 0; i < classBitField.length(); i++) {
            if (classBitField.get(i)) {
                value |= (short) (1 << i);
            }
        }
        return value;
    }

    private void readFromByteBuffer(ByteBuffer buffer) {
        this.members = new ArrayList<>();
//        buffer.position(8);
        for (int i = 0; i < numberOfMembers; i++) {

            members.add(DatatypeMessage.parseMember(buffer));
        }
    }


    @Override
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        for (HdfCompoundDatatypeMember member: members) {
            buffer.put(member.getName().getBytes(StandardCharsets.US_ASCII));
            buffer.put((byte)0);
            int paddingSize = (8 -  ((member.getName().length()+1)% 8)) % 8;
            buffer.put(new byte[paddingSize]);
            buffer.putInt(member.getOffset());
            buffer.put((byte)member.dimensionality);
            buffer.put(new byte[3]);
            buffer.putInt(member.dimensionPermutation);
            buffer.put(new byte[4]);
            for( int ds: member.dimensionSizes) {
                buffer.putInt(ds);
            }
            member.type.writeDefinitionToByteBuffer(buffer);

        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("CompoundDatatype {")
                .append(" numberOfMembers: ").append(numberOfMembers)
                .append(", size: ").append(size)
                .append(", ");
        members.forEach(member->{
            builder.append("\r\n\t");
            builder.append(member);
        });
        return builder.toString();
    }

    @Override
    public short getSizeMessageData() {
        short size = 0;
        for(HdfCompoundDatatypeMember member: members) {
            size += member.getType().getSizeMessageData();
        }
        return size;
    }

}
