package org.hdf5javalib.redo.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utility class for reading CSV files containing numerical data into a list of lists of BigDecimal values.
 */
public class CsvReader {

    /**
     * Reads a CSV file from the resources folder and converts its contents into a list of lists of BigDecimal values.
     * Each row in the CSV file is represented as a {@code List<BigDecimal>}, and all rows are collected into a {@code List}.
     * The CSV file is expected to contain comma-separated numerical values.
     *
     * @param fileName the name of the CSV file (including path if in a subdirectory) located in the resources folder
     * @return a {@code List<List<BigDecimal>>} containing the parsed numerical data from the CSV file
     * @throws NumberFormatException if any value in the CSV file cannot be parsed as a valid number
     */
    public static List<List<BigDecimal>> readCsvFromFile(String fileName) {
        List<List<BigDecimal>> data = new ArrayList<>();

        // Load file from resources
        try (InputStream is = CsvReader.class.getClassLoader().getResourceAsStream(fileName);
             BufferedReader reader = new BufferedReader(new InputStreamReader(is))) {
            if (is == null) {
                throw new IOException("Resource file not found: " + fileName);
            }

            String line;
            while ((line = reader.readLine()) != null) {
                // Split line into values and convert to BigDecimal
                List<BigDecimal> row = Arrays.stream(line.split(","))
                        .map(String::trim)
                        .map(BigDecimal::new)
                        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
                data.add(row);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return data;
    }
}