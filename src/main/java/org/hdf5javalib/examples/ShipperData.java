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
    private Byte origSort;
    private String destCountry;
    private String destSlic;
    private Byte destIbi;
    private String destPostalCode;
    private String shipper;
    private Byte service;
    private Byte packageType;
    private Byte accessorials;
    private Short pieces;
    private Short weight;
    private Integer cube;
    private Byte committedTnt;
    private Byte committedDate;
}

//                                    (short) 8, (short) 0, (short) 64)),
//                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short) 2)),
//                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short) 5)),
//                                    (short) 1, (short) 0, (short) 8)),
//                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short) 2)),
//                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short) 5)),
//                                    (short) 1, (short) 0, (short) 8)),
//                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short) 9)),
//                            new StringDatatype(StringDatatype.createClassAndVersion(), stringBitSet, (short) 10)),
//                                    (short) 1, (short) 0, (short) 8)),
//                                    (short) 1, (short) 0, (short) 8)),
//                                    (short) 1, (short) 0, (short) 8)),
//                                    (short) 2, (short) 0, (short) 16)),
//                                    (short) 2, (short) 0, (short) 16)),
//                                    (short) 4, (short) 0, (short) 32)),
//                                    (short) 1, (short) 0, (short) 8)),
//                                    (short) 1, (short) 0, (short) 8))
