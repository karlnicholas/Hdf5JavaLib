package org.hdf5javalib.file.dataobject.message.datatype;

import lombok.Getter;
import org.hdf5javalib.file.infrastructure.HdfGlobalHeap;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.Optional;

@Getter
public class CompoundMemberDatatype implements HdfDatatype {
    private final String name;
    private final int offset;
    private final int dimensionality;
    private final int dimensionPermutation;
    private final int[] dimensionSizes;
    private final HdfDatatype type;
    private final short sizeMessageData;

    public CompoundMemberDatatype(String name, int offset, int dimensionality, int dimensionPermutation, int[] dimensionSizes, HdfDatatype type) {
        this.name = name;
        this.offset = offset;
        this.dimensionality = dimensionality;
        this.dimensionPermutation = dimensionPermutation;
        this.dimensionSizes = dimensionSizes;
        this.type = type;
        sizeMessageData = switch(type.getDatatypeClass()) {
            case FIXED -> computeFixedMessageDataSize(name);
            case FLOAT -> computeFloatMessageDataSize(name);
            case STRING -> computeStringMessageDataSize(name);
            case VLEN -> computeVariableLengthMessageDataSize(name);
            default -> throw new IllegalStateException("Unexpected datatype class: " + type.getDatatypeClass());
        };
    }

    private short computeFloatMessageDataSize(String name) {
        if (name.length() > 0 ) {
            int padding = (8 -  ((name.length()+1)% 8)) % 8;
            return (short) (name.length()+1 + padding + 44);
        } else {
            return 44;
        }
    }

    private short computeFixedMessageDataSize(String name) {
        if (name.length() > 0 ) {
            int padding = (8 -  ((name.length()+1)% 8)) % 8;
            return (short) (name.length()+1 + padding + 40 + 4);
        } else {
            return 44;
        }
    }

    private short computeStringMessageDataSize(String name) {
        int padding = (8 -  ((name.length()+1)% 8)) % 8;
        return (short) (name.length()+1 + padding + 40 + 0);
    }

    private short computeVariableLengthMessageDataSize(String name) {
        int padding = (8 -  ((name.length()+1)% 8)) % 8;
        return (short) (name.length()+1 + padding + 40 + 12);
    }

    @Override
    public String toString() {
        return "Member{" +
                "name='" + name + '\'' +
                ", offset=" + offset +
                ", dimensionality=" + dimensionality +
                ", dimensionPermutation=" + dimensionPermutation +
                ", dimensionSizes=" + java.util.Arrays.toString(dimensionSizes) +
                ", type=" + type +
                '}';
    }
    public void writeDefinitionToByteBuffer(ByteBuffer buffer) {
        buffer.put(name.getBytes(StandardCharsets.US_ASCII));
        buffer.put((byte)0);
        int paddingSize = (8 -  ((name.length()+1)% 8)) % 8;
        buffer.put(new byte[paddingSize]);
        buffer.putInt(offset);
        buffer.put((byte)dimensionality);
        buffer.put(new byte[3]);
        buffer.putInt(dimensionPermutation);
        buffer.put(new byte[4]);
        for( int ds: dimensionSizes) {
            buffer.putInt(ds);
        }

        // Datatype general information
        buffer.put(type.getClassAndVersion());    // 1
        // copy 3 bytes for ClassBitField
        byte[] bytes = type.getClassBitField().toByteArray();
        byte[] result = new byte[3];
        System.arraycopy(bytes, 0, result, 0, Math.min(bytes.length, 3));
        buffer.put(result);         // 3
        buffer.putInt(type.getSize());        // 4
        type.writeDefinitionToByteBuffer(buffer);
    }

    @Override
    public DatatypeClass getDatatypeClass() {
        return type.getDatatypeClass();
    }

    @Override
    public byte getClassAndVersion() {
        return type.getClassAndVersion();
    }

    @Override
    public BitSet getClassBitField() {
        return type.getClassBitField();
    }

    @Override
    public int getSize() {
        return type.getSize();
    }

    @Override
    public <T> T getInstance(Class<T> clazz, byte[] bytes) {
        return type.getInstance(clazz, bytes);
    }
    @Override
    public <T> T getInstance(Class<T> clazz, ByteBuffer buffer) {
        byte[] bytes = new byte[sizeMessageData];
        buffer.get(bytes);
        return getInstance(clazz, bytes);
    }

    @Override
    public Optional<HdfDatatype> needsGlobalHeap() {
        return type.needsGlobalHeap();
    }

    @Override
    public void setGlobalHeap(HdfGlobalHeap globalHeap) {
        type.setGlobalHeap(globalHeap);
    }
}
