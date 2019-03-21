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

import java.io.*;
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

    private static void readStream(byte[] out) throws IOException{
        ByteArrayInputStream in = new ByteArrayInputStream(out);
        ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
        IntVector vector = (IntVector) reader.getVectorSchemaRoot().getFieldVectors().get(0);

        while(reader.loadNextBatch()) {
            for (int i = 0; i < vector.getValueCount(); i++) {
                if (vector.isNull(i)){
                    System.out.print("null ");
                }else{
                    System.out.print(vector.get(i) + " ");
                }
            }
            System.out.println();
        }
    }

    private static ByteArrayOutputStream writeStreamFromCsv(String pathToFile) throws IOException{
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        Scanner scanner = new Scanner(new File(pathToFile));
        scanner.nextLine();

        List<FieldVector> vectors = new ArrayList<>();
        List<Field> fields = new ArrayList<>();
        IntVector vector = new IntVector("csvtest", allocator);
        vectors.add(vector);
        fields.add(vector.getField());

        Schema schema = new Schema(fields, null);
        VectorSchemaRoot root = new VectorSchemaRoot(schema, vectors, 0);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(os));

        writer.start();
        while(scanner.hasNextLine()){
            String[] values = scanner.nextLine().replace("$", "").split(",");
            writeBatch(writer, vector, values, root);
        }
        writer.end();

        return os;
    }

    private static void writeBatch(ArrowWriter writer, IntVector vector, String[] values, VectorSchemaRoot root) throws IOException{
        for(int i = 0; i < values.length; i++){
            String value = values[i];

            if(value.contains(".")){
                value = value.substring(0, value.indexOf("."));
            }

            try{
                vector.setSafe(i, Integer.parseInt(value));
            }catch (NumberFormatException nfe){
                vector.setNull(i);
            }
        }

        vector.setValueCount(values.length);
        root.setRowCount(values.length);
        writer.writeBatch();
    }

    public static void main(String[] args) throws IOException {
        long start = System.nanoTime();
        System.loadLibrary("plasma_java");
        PlasmaClient plasmaClient = new PlasmaClient("/tmp/store", "", 0);
        String pathName = "src/main/resources/data/million.csv";
        //readStream(writeStreamFromCsv(pathName).toByteArray());
        ByteArrayOutputStream out = writeStreamFromCsv(pathName);
        readStream(out.toByteArray());
        byte[] objectId = putValueInPlasma(plasmaClient, out.toByteArray());
        readStream(getValueFromPlasma(plasmaClient, objectId));
        System.out.println(System.nanoTime() - start);

    }
}
