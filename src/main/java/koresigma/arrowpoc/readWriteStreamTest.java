package koresigma.arrowpoc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.*;

public class readWriteStreamTest {
    private static BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

    private static Schema generateSchemaFromCSV(String pathname) throws IOException{
        Scanner sc = new Scanner(new File(pathname));
        String[] fieldNames = sc.nextLine().split(",");
        HashMap<String, ArrowType.Decimal> schemeValues = new HashMap<>();

        for (String fieldName: fieldNames){
            schemeValues.put(fieldName, new ArrowType.Decimal(100,5));
        }

        List<Field> allFields = new ArrayList<>();
        for (HashMap.Entry<String, ArrowType.Decimal> entry : schemeValues.entrySet()) {
            String fieldName = entry.getKey();
            FieldType fieldType = FieldType.nullable(entry.getValue());
            Field newField = new Field(fieldName, fieldType, Collections.emptyList());
            allFields.add(newField);
        }

        return new Schema(allFields);
    }


    private static ByteArrayOutputStream writeStream(Schema schema) throws IOException {
        int numBatches = 1;

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {
                writer.start();
                for (int i = 0; i < numBatches; i++) {
                    writer.writeBatch();
                }
                writer.end();
            }
            return out;
        }
    }

    private static ByteArrayInputStream readStream(byte[] out) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(out);
        try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
            Schema readSchema = reader.getVectorSchemaRoot().getSchema();
            while (reader.loadNextBatch()){
                System.out.println(reader.getVectorSchemaRoot().getRowCount());
                reader.loadNextBatch();
            }
        }

        return in;
    }

    private static byte[] putValueInPlasma(PlasmaClient plasmaClient, byte[] value){
        byte[] objectId = new byte[20];
        new Random().nextBytes(objectId);
        byte[] metaData = null;
        plasmaClient.put(objectId, value, metaData);

        return objectId;
    }

    private static byte[] getValueFromPlasma(PlasmaClient plasmaClient, byte[] objectId){
        int timeoutMs = 0;
        boolean isMetadata = false;

        return plasmaClient.get(objectId, timeoutMs, isMetadata);
    }

    public static void main(String[] args) throws IOException {
        System.loadLibrary("plasma_java");
        String pathName = "src/resources/data/million.csv";

        Schema schema = generateSchemaFromCSV(pathName);
        ByteArrayOutputStream out = writeStream(schema);

        PlasmaClient plasmaClient = new PlasmaClient("/tmp/store", "", 0);
        byte[] objectId = putValueInPlasma(plasmaClient, out.toByteArray());
        readStream(getValueFromPlasma(plasmaClient, objectId));
    }
}
