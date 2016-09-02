package com.lofitskyi

import cascading.avro.local.PackedAvroScheme
import cascading.flow.Flow
import cascading.flow.FlowDef
import cascading.flow.FlowProcess
import cascading.flow.local.LocalFlowConnector
import cascading.operation.BaseOperation
import cascading.operation.Debug
import cascading.operation.Function
import cascading.operation.FunctionCall
import cascading.pipe.Each
import cascading.pipe.Pipe
import cascading.scheme.local.TextLine
import cascading.tap.Tap
import cascading.tap.local.FileTap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import com.truven.dataforge.masterpatientrecord.avro.pojo.MasterPatientRecord
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import java.lang.reflect.Method

class Main {
    public static void main(String[] args) throws IOException {
        def avroInputPath = "./src/main/resources/mpr.avro"
        def jsonOutPath = "output/MasterPatientRecord.json"

        Properties properties = new Properties()
        LocalFlowConnector flowConnector = new LocalFlowConnector(properties)

        Schema payerSchema = MasterPatientRecord.getClassSchema()

        PackedAvroScheme<MasterPatientRecord> packedAvroScheme = new PackedAvroScheme<MasterPatientRecord>(payerSchema)
        Tap<?,?,?> avroInTap = new FileTap(packedAvroScheme, avroInputPath)



        Pipe p = new Pipe("MasterPatientRecord Avro input")
        p = new Each(p, new Debug())


        Fields customFields = new Fields(FieldsDao.getFieldsForSpecificAco("any").toArray(new String[0]))
        p = new Each(p, new ReadCustomFieldFunction(customFields, FieldsDao.getFieldsForSpecificAco("any")), Fields.RESULTS)
        p = new Each(p, new Debug())

        Tap<?,?,?> outTap = new FileTap(new TextLine(new Fields("dasdasda")), jsonOutPath)

        FlowDef flowDef = FlowDef.flowDef()
                .setName("transform Avro to JSON")
                .addSource(p, avroInTap)
                .addTailSink(p, outTap)

        Flow<?> flow = flowConnector.connect(flowDef)
        flow.complete()
    }
}

class ReadCustomFieldFunction extends BaseOperation<MasterPatientRecord> implements Function<MasterPatientRecord> {

    List<String> customFields

    public ReadCustomFieldFunction(Fields fieldDeclaration, List<String> customFields){
        super(1,fieldDeclaration)
        this.customFields = customFields
    }

    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        MasterPatientRecord record = (MasterPatientRecord) functionCall.getArguments().getObject(0)
        Object o;

        Tuple tuple = new Tuple()
        customFields.each { field ->
            List<String> fieldPath = new LinkedList<>(field.split("\\.").toList())

            if (fieldPath.size() != 1) {
                Method method;
                Class arrayClass
                o = record
                fieldPath.each { property ->
                    method = o.getClass().getMethod("get", String.class)
                    GenericData.class.getClasses().each {
                        if (it.name.contains("Array"))
                            arrayClass = it
                    }
                    Method methodForList =  arrayClass.getMethod("get", int)
                    if (property != fieldPath.last()) {
                        if (property.contains("{first}")) { //\{\+?\d+\}
                            property = property.replace("{first}", "")
                            o = (method.invoke(o, property))
                            o = methodForList.invoke(o, 0)
                        } else
                            o = (method.invoke(o, property))

                    }
                }

                tuple.add(o.get(fieldPath.last))
            } else {
                tuple.add(record.get(field))
            }
        }

        functionCall.getOutputCollector().add(tuple)
    }
}

class FieldsDao{
    static List<String> getFieldsForSpecificAco(String aco){
        ["MasterPatientNumber",
         "RecordCreateDtm",
         "ClinicalPatientRecords{first}.CDAClientCode",
         "ClinicalPatientRecords{first}.ClinicalEncounters{first}.FacilityIdentifierRaw",
         "ClinicalPatientRecords{first}.ClinicalEncounters{first}.MedicalProfessionals{first}.LastNameRaw",
         "ClinicalPatientRecords{first}.ClinicalEncounters{first}.MedicalProfessionals{first}.FirstNameRaw"]
    }
}