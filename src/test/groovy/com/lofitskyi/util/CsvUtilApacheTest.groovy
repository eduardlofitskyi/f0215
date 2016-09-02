package com.lofitskyi.util

import org.junit.Before
import org.junit.Test

import java.nio.file.NoSuchFileException

import static org.junit.Assert.assertEquals

class CsvUtilApacheTest {

    CsvUtilApache csv

    @Before
    public void setUp() throws Exception {
        csv = new CsvUtilApache()

    }

    @Test(expected = IllegalArgumentException)
    void shouldThrowExceptionWhenPassMissed(){
        csv.getRequestedCustomFields()
    }

    @Test(expected = NoSuchFileException)
    void shouldThrowExceptionWhenPassIllegalPath(){
        csv.path = "/not/real/path.csv"
        csv.getRequestedCustomFields()
    }

    @Test
    void shouldReturnCorrectCountOfCsvRows(){
        csv.path = "src/test/resources/test_custom_field.csv"
        def fields = csv.getRequestedCustomFields()
        assertEquals(3, fields.size())
        assertEquals("sex", fields.get(2))
    }
}
