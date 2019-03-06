package koresigma.arrowpoc;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.types.pojo.*;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
        IntVector vector = (IntVector) reader.getVectorSchemaRoot().getFieldVectors().get(0);

        while(reader.loadNextBatch()){
            for(int i = 0; i < vector.getValueCount(); i++){
                String output = "Index " + i + " is ";
                output += vector.isNull(i) ? "null." : vector.get(i);
                System.out.println(output);
            }
        }
    }

    private static ByteArrayOutputStream writeStream() throws IOException{
        ByteArrayOutputStream os = new ByteArrayOutputStream();

        IntVector vector = new IntVector("header1", allocator);
        Schema schema = new Schema(Collections.singletonList(vector.getField()), null);
        VectorSchemaRoot root = getNewVectorSchemaRoot(schema, vector);
        ArrowStreamWriter writer = getNewArrowStreamWriter(root, os);
        writeBatchData(writer, vector, root);

        return os;
    }

    // writeStream() helper function for better readability
    private static VectorSchemaRoot getNewVectorSchemaRoot(Schema schema, IntVector vector){
        return new VectorSchemaRoot(schema, Collections.singletonList(vector), vector.getValueCount());
    }

    // writeStream() helper function for better readability
    private static ArrowStreamWriter getNewArrowStreamWriter(VectorSchemaRoot root, ByteArrayOutputStream os){
        return new ArrowStreamWriter(root, null, Channels.newChannel(os));
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
//        String pathName = "src/main/resources/data/million.csv";
//        ByteArrayOutputStream out = writeStream();
//        readStream(out.toByteArray());
//        byte[] objectId = putValueInPlasma(plasmaClient, out.toByteArray());
//        readStream(getValueFromPlasma(plasmaClient, objectId));

        readStream(writeStream().toByteArray());
    }
}
