package org.hdf5javalib.examples;

import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.hdf5javalib.HdfFileReader;
import org.hdf5javalib.dataclass.HdfFixedPoint;
import org.hdf5javalib.dataclass.HdfString;
import org.hdf5javalib.datasource.TypedDataSource;
import org.hdf5javalib.file.HdfDataSet;
import org.hdf5javalib.file.HdfFile;
import org.hdf5javalib.file.dataobject.message.DataspaceMessage;
import org.hdf5javalib.file.dataobject.message.datatype.StringDatatype;
import org.hdf5javalib.utils.HdfTestUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Hello world!
 *
 */
public class HdfSeparateTypesApp {
    public static void main(String[] args) {
        new HdfSeparateTypesApp().run();
    }

    private void run() {
        try {
            HdfFileReader reader = new HdfFileReader();
            String filePath = Objects.requireNonNull(HdfCompoundApp.class.getResource("/all_types_separate.h5")).getFile();
            try (FileInputStream fis = new FileInputStream(filePath)) {
                FileChannel channel = fis.getChannel();
                reader.readFile(channel);
                try ( HdfDataSet dataSet = reader.getDataset(channel, "fixed_point") ) {
                    displayData(channel, dataSet);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        tryHdfApiStrings("string_ascii_all.h5", this::writeAll, StringDatatype.createClassBitField(StringDatatype.PaddingType.SPACE_PAD, StringDatatype.CharacterSet.ASCII), 8);
    }

    private void displayData(FileChannel fileChannel, HdfDataSet dataSet) throws IOException {
        TypedDataSource<Integer> dataSource = new TypedDataSource<>(dataSet, fileChannel, dataSet.getDataAddress(), Integer.class);
        Integer allData = dataSource.readScalar();
        System.out.println("String stream = " + allData);
    }

}
