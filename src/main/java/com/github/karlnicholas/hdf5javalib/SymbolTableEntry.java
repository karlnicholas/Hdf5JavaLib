package com.github.karlnicholas.hdf5javalib;

public class SymbolTableEntry {
    private String symbol;
    private String value;

    public SymbolTableEntry(String symbol, String value) {
        this.symbol = symbol;
        this.value = value;
    }

    public String getSymbol() { return symbol; }
    public String getValue() { return value; }

    @Override
    public String toString() {
        return "SymbolTableEntry{" + "symbol='" + symbol + "', value='" + value + "'}";
    }
}
