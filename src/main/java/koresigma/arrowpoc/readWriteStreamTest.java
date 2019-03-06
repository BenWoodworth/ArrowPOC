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
import java.nio.channels.Channels;
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // in writeStream() we need to create a vector for each header in the csv
    // for each header we need to write all the data in it's column in writeBatch()
    ////////////////////////////////////////////////////

    private static void readStream(byte[] out) throws IOException{
        ByteArrayInputStream in = new ByteArrayInputStream(out);
        ArrowStreamReader reader = new ArrowStreamReader(in, allocator);

        for(int i = 0; i < reader.getVectorSchemaRoot().getFieldVectors().size(); i++){
            IntVector vector = (IntVector) reader.getVectorSchemaRoot().getFieldVectors().get(i);
            while(reader.loadNextBatch()){
                for(int j = 0; j < vector.getValueCount(); j++){
                    String output = "Index " + j + " is ";
                    output += vector.isNull(j) ? "null." : vector.get(j);
                    System.out.println(output);
                }
            }
        }
    }

    private static ByteArrayOutputStream writeStreamFromCsv(String pathToFile) throws IOException{
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Scanner scanner = new Scanner(new File(pathToFile));
        String[] fieldNames = scanner.nextLine().split(",");
        List<FieldVector> vectors = new ArrayList<>();
        List<Field> fields = new ArrayList<>();

        for (String fieldName: fieldNames){
            IntVector vector = new IntVector(fieldName, allocator);
            vectors.add(vector);
            fields.add(vector.getField());
        }

        Schema schema = new Schema(fields, null);
        VectorSchemaRoot root = new VectorSchemaRoot(schema, vectors, 0);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(os));

        for(FieldVector vector: vectors){
            writeBatchData(writer, (IntVector) vector, root);
        }

        return os;
    }

    private static void writeBatchData(ArrowWriter writer, IntVector vector, VectorSchemaRoot root) throws IOException {
        writer.start();

        vector.setNull(0);
        vector.setSafe(1, 1);
        vector.setSafe(2, 2);
        vector.setNull(3);
        vector.setSafe(4, 1);
        vector.setValueCount(5);
        root.setRowCount(5);
        writer.writeBatch();

        vector.setNull(0);
        vector.setSafe(1, 1);
        vector.setSafe(2, 2);
        vector.setValueCount(3);
        root.setRowCount(3);
        writer.writeBatch();

        writer.end();
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
