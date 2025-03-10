package org.hdf5javalib.examples;

import lombok.Data;

@Data
public class MonitoringData {
    private String siteName;
    private Float airQualityIndex;
    private Double temperature;
    private Integer sampleCount;
}
