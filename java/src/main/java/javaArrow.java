import io.netty.buffer.ArrowBuf;
import koresigma.arrowpoc.ByteUtilsKt;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class javaArrow {

    static ArrowBuf buf(byte[] bytes) {
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ArrowBuf buffer = allocator.buffer(bytes.length);
        buffer.writeBytes(bytes);
        return buffer;
    }

    public static void main(String[] args) {
        System.loadLibrary("plasma_java");
        PlasmaClient client = new PlasmaClient("/tmp/store", "", 0);

        byte[] id = ByteUtilsKt.getHexBytes("0bf5ed24d1c95946f4ab1791f9238153e8b9147b");
        byte[] value = client.get(id, 0, false);
        ByteArrayInputStream in = new ByteArrayInputStream(value);
        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        ArrowStreamReader reader = new ArrowStreamReader(in, allocator);
        try{
            Schema readSchema = reader.getVectorSchemaRoot().getSchema();
            while(reader.loadNextBatch()){
                VectorSchemaRoot vsr = reader.getVectorSchemaRoot();
                System.out.println(vsr.contentToTSVString());
            }
        }catch (IOException e){
            e.printStackTrace();
        }
    }

}
