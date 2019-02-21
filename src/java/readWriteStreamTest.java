import data.TestDataMillion;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
    private static BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);

    private static Schema getTestSchema(){
        String fieldName = "testField";
        ArrowType.Int arrowType = new ArrowType.Int(8, true);
        Field newField = new Field(fieldName, FieldType.nullable(arrowType), Collections.emptyList());
        return new Schema(asList(newField));
    }

    private static ByteArrayOutputStream writeStream() throws IOException {
        int numBatches = 1;
        Schema schema = getTestSchema();

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

    private static ByteArrayInputStream readStream(ByteArrayOutputStream out) throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        try (ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
            Schema readSchema = reader.getVectorSchemaRoot().getSchema();
            System.out.println(readSchema.toJson());
        }

        return in;
    }

    public static void main(String[] args) throws IOException {
        ByteArrayOutputStream out = writeStream();
        readStream(out);
        //TestDataMillion.INSTANCE.getData().);
    }
}
