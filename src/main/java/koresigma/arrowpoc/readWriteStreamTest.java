package koresigma.arrowpoc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.pojo.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.channels.Channels;
import java.sql.SQLOutput;
import java.util.*;

public class readWriteStreamTest {
    private static BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

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

    private static void readStream(byte[] out) throws IOException{
        ByteArrayInputStream in = new ByteArrayInputStream(out);
        ArrowStreamReader reader = new ArrowStreamReader(in, allocator);

        for(int i = 0; i < reader.getVectorSchemaRoot().getFieldVectors().size(); i++){
            IntVector vector = (IntVector) reader.getVectorSchemaRoot().getFieldVectors().get(i);
            String fieldName = vector.getField().getName();
            reader.loadNextBatch();
            System.out.println(fieldName);
            String output = "\t\tIndex 0: ";
            output += vector.isNull(0) ? "null." : vector.get(0);
            System.out.println(output);
        }
    }

    private static ByteArrayOutputStream writeStreamFromCsv(String pathToFile) throws IOException{
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Scanner scanner = new Scanner(new File(pathToFile));
        String[] fieldNames = scanner.nextLine().split(",");
        String[] values = scanner.nextLine().replace("$", "").split(",");

        List<FieldVector> vectors = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        populateFieldsVectors(fieldNames, vectors, fields);

        Schema schema = new Schema(fields, null);
        VectorSchemaRoot root = new VectorSchemaRoot(schema, vectors, 0);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(os));
        writer.start();
        for(int i = 0; i < values.length; i++){
            String value = values[i];
            if(value.contains(".")){
                value = value.substring(0, value.indexOf("."));
            }
            writeVal(writer, (IntVector) vectors.get(i), root, value);
        }
        writer.end();
        return os;
    }

    private static void writeVal(ArrowWriter writer, IntVector vector, VectorSchemaRoot root, String value) throws IOException{
        try{
            vector.setSafe(0, Integer.parseInt(value));
        }catch (NumberFormatException nfe){
            vector.setNull(0);
        }
        vector.setValueCount(1);
        root.setRowCount(1);
        writer.writeBatch();
    }

    // writeStream helper method for better readability
    private static void populateFieldsVectors(String[] fieldNames, List<FieldVector> vectors, List<Field> fields){
        for (String fieldName: fieldNames) {
            IntVector vector = new IntVector(fieldName, allocator);
            vectors.add(vector);
            fields.add(vector.getField());
        }
    }

    public static void main(String[] args) throws IOException {
//        System.loadLibrary("plasma_java");
//        PlasmaClient plasmaClient = new PlasmaClient("/tmp/store", "", 0);
        String pathName = "src/main/resources/data/million.csv";
        readStream(writeStreamFromCsv(pathName).toByteArray());
//        ByteArrayOutputStream out = writeStream();
//        readStream(out.toByteArray());
//        byte[] objectId = putValueInPlasma(plasmaClient, out.toByteArray());
//        readStream(getValueFromPlasma(plasmaClient, objectId));

    }
}
