package com.lofitskyi.util

import org.apache.commons.csv.CSVParser
import static org.apache.commons.csv.CSVFormat.*

import java.nio.file.Paths

class CsvUtilApache implements CsvUtil{

    final static COLUMN_NAME = 'FIELDS'

    String path;

    CsvUtilApache() {
    }

    CsvUtilApache(String path) {
        this.path = path
    }

    @Override
    List<String> getRequestedCustomFields() {

        if (path == null) throw new IllegalArgumentException("You didn't pass argument: path is null")

        def fields = new ArrayList<>();

        Paths.get(path).withReader { reader ->
            CSVParser csv = new CSVParser(reader, DEFAULT.withHeader())

            for (record in csv.iterator()) {
                fields.add(record.get(COLUMN_NAME))
            }
        }

        fields
    }
}
