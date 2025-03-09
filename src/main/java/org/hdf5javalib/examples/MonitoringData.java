package org.hdf5javalib.examples;

import lombok.*;

import java.math.BigInteger;

@Data
public class MonitoringData {
    private String siteName;
    private Float airQualityIndex;
    private Double temperature;
    private Integer sampleCount;
}
