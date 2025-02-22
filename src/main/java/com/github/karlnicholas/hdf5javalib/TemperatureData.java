package com.github.karlnicholas.hdf5javalib;

import lombok.Builder;
import lombok.Data;

import java.math.BigInteger;

@Data
@Builder
public class TemperatureData {
    private BigInteger temperature;
}
