package org.hdf5javalib.examples;

import lombok.*;

import java.math.BigInteger;

@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MonitoringData {
    private String siteName;
    private Float airQualityIndex;
    private Double temperature;
    private BigInteger sampleCount;
}
