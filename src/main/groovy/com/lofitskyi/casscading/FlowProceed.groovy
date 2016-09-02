package com.lofitskyi.casscading

import cascading.flow.FlowDef
import cascading.pipe.Pipe
import cascading.tap.Tap
import cascading.tuple.Fields
import com.lofitskyi.util.CsvUtil
import com.lofitskyi.util.CsvUtilApache

class FlowProceed {

    static final String[] STANDART_FIELDS = ["first_name", "last_name"]

    public static FlowDef customFieldsPrecessing(Tap<?, ?, ?> source, Tap<?, ?, ?> sink) {
        Pipe pipe = new Pipe("custom-field")

        Fields standard = new Fields(STANDART_FIELDS)
        Fields custom = new Fields(getCustomFields())

        Fields union = standard.append(custom)

//        pipe = new Retain(pipe, union);

        def flowDef = FlowDef.flowDef()
                .addSource(pipe, source)
                .addTail(pipe)
                .addSink(pipe, sink)

        flowDef
    }

    private static String[] getCustomFields() {
        CsvUtil csvUtil = new CsvUtilApache("src/test/resources/test_custom_field.csv")
        def fieldsStr = csvUtil.getRequestedCustomFields().toArray(new String[0])
        fieldsStr
    }
}
