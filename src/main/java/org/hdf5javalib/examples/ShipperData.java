package org.hdf5javalib.examples;

import lombok.*;
import org.hdf5javalib.file.dataobject.message.datatype.FixedPointDatatype;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;

import java.math.BigInteger;

@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ShipperData {
    private BigInteger shipmentId;
    private String origCountry;
    private String origSlic;
    private BigInteger origSort;
    private String destCountry;
    private String destSlic;
    private BigInteger destIbi;
    private String destPostalCode;
    private String shipper;
    private BigInteger service;
    private BigInteger packageType;
    private BigInteger accessorials;
    private BigInteger pieces;
    private BigInteger weight;
    private BigInteger cube;
    private BigInteger committedTnt;
    private BigInteger committedDate;

//    private BigInteger shipmentId;
//    private String origCountry;
//    private String origSlic;
//    private Byte origSort;
//    private String destCountry;
//    private String destSlic;
//    private Byte destIbi;
//    private String destPostalCode;
//    private String shipper;
//    private Byte service;
//    private Byte packageType;
//    private Byte accessorials;
//    private Short pieces;
//    private Short weight;
//    private Integer cube;
//    private Byte committedTnt;
//    private Byte committedDate;
//
//    @Override
//    public String toString() {
//        return "ShipperData{" +
//                "shipmentId=" + shipmentId + "," +
//                "origCountry=" + origCountry + "," +
//                "origSlic=" + origSlic + "," +
//                "origSort=" + Byte.toUnsignedInt(origSort) + "," +
//                "destCountry=" + destCountry + "," +
//                "destSlic=" + destSlic + "," +
//                "destIbi=" + Byte.toUnsignedInt(destIbi) + "," +
//                "destPostalCode=" + destPostalCode + "," +
//                "shipper=" + shipper + "," +
//                "service=" + Byte.toUnsignedInt(service) + "," +
//                "packageType=" + Byte.toUnsignedInt(packageType) + "," +
//                "accessorials=" + Byte.toUnsignedInt(accessorials) + "," +
//                "pieces=" + Short.toUnsignedInt(pieces) + "," +
//                "weight=" + Short.toUnsignedInt(weight) + "," +
//                "cube=" + Integer.toUnsignedLong(cube) + "," +
//                "committedTnt=" + Byte.toUnsignedInt(committedTnt) + "," +
//                "committedDate=" + Byte.toUnsignedInt(committedDate) +
//                "}";
//    }
}
