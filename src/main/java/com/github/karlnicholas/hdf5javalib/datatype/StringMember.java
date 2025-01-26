package com.github.karlnicholas.hdf5javalib.datatype;

public class StringMember {
    private final long size;
    private final int paddingType;
    private final String paddingDescription;
    private final int charSet;
    private final String charSetDescription;

    public StringMember(long size, int paddingType, String paddingDescription, int charSet, String charSetDescription) {
        this.size = size;
        this.paddingType = paddingType;
        this.paddingDescription = paddingDescription;
        this.charSet = charSet;
        this.charSetDescription = charSetDescription;
    }

    @Override
    public String toString() {
        return "StringMember{" +
                "size=" + size +
                ", paddingType=" + paddingType +
                ", paddingDescription='" + paddingDescription + '\'' +
                ", charSet=" + charSet +
                ", charSetDescription='" + charSetDescription + '\'' +
                '}';
    }
}
