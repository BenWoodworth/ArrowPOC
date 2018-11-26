package koresigma.arrowpoc;

import org.apache.arrow.plasma.PlasmaClient;

public class ArrowPoc {

    public static void main(String[] args) {
        System.out.println(System.getProperty("java.library.path"));

        System.loadLibrary("plasma");

        client();
    }

    private static void client() {
        PlasmaClient plasmaClient = new PlasmaClient("/tmp/plasma", "", 0);
        byte[] byteObject = null;

        System.out.println("hello");
    }
}
