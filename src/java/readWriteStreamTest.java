import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import static java.util.Arrays.asList;

public class readWriteStreamTest {

    private static BufferAllocator allocator = getTestBufferAllocator();
    private static int numBatches = 1;

    public static Schema getTestScheme(){
        return new Schema(asList(new Field(
                "testField", FieldType.nullable(new ArrowType.Int(8, true)), Collections.<Field>emptyList())));
    }

    public static BufferAllocator getTestBufferAllocator(){
        return new RootAllocator(Integer.MAX_VALUE);
    }

    public static ByteArrayOutputStream writeStream() throws IOException {
        Schema schema = getTestScheme();

        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            root.getFieldVectors().get(0).allocateNew();
            TinyIntVector vector = (TinyIntVector) root.getFieldVectors().get(0);
            for (int i = 0; i < 16; i++) {
                vector.set(i, i < 8 ? 1 : 0, (byte) (i + 1));
            }
            vector.setValueCount(16);
            root.setRowCount(16);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            long bytesWritten = 0;
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

    public static ByteArrayInputStream readStream(ByteArrayOutputStream out) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
            Schema readSchema = reader.getVectorSchemaRoot().getSchema();
            System.out.println(readSchema.toJson());
        }

        return in;
    }

    public static void main(String[] args) throws IOException {
        ByteArrayOutputStream out = writeStream();
        ByteArrayInputStream in = readStream(out);

    }
}
