import io.netty.buffer.ArrowBuf;
import koresigma.arrowpoc.ByteUtilsKt;
import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.arrow.memory.RootAllocator;

public class javaArrow {

    public static void main(String[] args) {
        System.loadLibrary("plasma_java");
        PlasmaClient client = new PlasmaClient("/tmp/store", "", 0);

        byte[] id = ByteUtilsKt.getHexBytes("447c91eea0468b5764ab7f14d8674fc15927bb49");
        byte[] value = client.get(id, 0, false);

        System.out.println(ByteUtilsKt.toHexString(value));

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
