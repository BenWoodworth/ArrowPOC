import org.apache.arrow.plasma.*;

public class arrow_test {

    private static void client(){
        PlasmaClient plasmaClient = new PlasmaClient("/tmp/plasma", "", 0);
        byte[] byteObject = null;


//        plasmaClient.put();
    }

    public static void main(String[] args){
        // Edit run configuration -> VM Arguments:
        // -Djava.library.path="~/.local/lib/miniconda3/lib/"
        System.out.println(System.getProperty("java.library.path"));

//        System.load("/home/ben/.local/lib/miniconda3/pkgs/arrow-cpp-0.11.1-py36h3bd774a_0/lib/libplasma.so.11.1.0");


        System.loadLibrary("libplasma");
//        System.loadLibrary("/home/ben/.local/lib/miniconda3/lib/libplasma.so.11.1.0");
//        System.load("/home/ben/.local/lib/miniconda3/lib/libarrow.so");
//        System.load("/home/ben/.local/lib/miniconda3/lib/libboost.so");

        client();
    }
}
