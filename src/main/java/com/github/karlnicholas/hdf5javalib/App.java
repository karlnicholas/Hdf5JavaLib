package com.github.karlnicholas.hdf5javalib;

import java.io.File;
import java.io.IOException;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        try {
            String filePath = App.class.getResource("/ExportedNodeShips.h5").getFile();
            HdfReader reader = new HdfReader(filePath);
            reader.readFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
