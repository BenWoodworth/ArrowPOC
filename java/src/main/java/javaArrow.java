import io.netty.buffer.ArrowBuf;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.arrow.memory.RootAllocator;

public class javaArrow {

    public static void main(String[] args) {
        System.loadLibrary("plasma_java");
        PlasmaClient client = new PlasmaClient("/tmp/store", "", 0);
        byte[] id = new byte[]{(byte) 0x44,
                (byte) 0x7c,
                (byte) 0x91,
                (byte) 0xee,
                (byte) 0xa0,
                (byte) 0x46,
                (byte) 0x8b,
                (byte) 0x57,
                (byte) 0x64,
                (byte) 0xab,
                (byte) 0x7f,
                (byte) 0x14,
                (byte) 0xd8,
                (byte) 0x67,
                (byte) 0x4f,
                (byte) 0xc1,
                (byte) 0x59,
                (byte) 0x27,
                (byte) 0xbb,
                (byte) 0x49
        };

        System.out.println(client.get(id, 0, false));
//
//        byte[] info = client.get(id, 0, false);
//        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
//        ArrowBuf buffer = allocator.buffer(info.length);
//        buffer.writeBytes(info);
//        System.out.println(buffer);


//        try {
//            javaDriver driver = new javaDriver();
//            ObjectStoreLink pLink = driver.getpLink();
//            System.out.println(plink);
//
//            String hello = "hello";
//            byte[] id1 = new byte[20];
//            Arrays.fill(id1, (byte) 1);
//            byte[] val1 = hello.getBytes();
//            byte[] meta1 = new byte[20];
//            Arrays.fill(meta1, (byte) 2);
//            pLink.put(id1, val1, meta1);
//            System.out.println(pLink.contains(id1));
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }

}
