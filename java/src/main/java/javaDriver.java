import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.arrow.plasma.PlasmaClient;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class javaDriver {
    private String storeSuffix = "/tmp/store";
    private Process storeProcess;
    private int storePort;
    private ObjectStoreLink pLink;


    public javaDriver() {
        String plasmaStorePath = "plasma_store";
        this.startObjectStore(plasmaStorePath);
        System.loadLibrary("plasma_java");
        this.pLink = new PlasmaClient(this.getStoreAddress(), "", 0);
    }

    public ObjectStoreLink getpLink(){
        return this.pLink;
    }

    private Process startProcess(String[] cmd) {
        ProcessBuilder builder;
        List<String> newCmd = Arrays.stream(cmd).filter(s -> s.length() > 0).collect(Collectors.toList());
        builder = new ProcessBuilder(newCmd);
        builder.inheritIO();
        Process p = null;
        try {
            p = builder.start();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        System.out.println("Start process " + p.hashCode() + " OK, cmd = " + Arrays.toString(cmd).replace(',', ' '));
        return p;
    }

    private void startObjectStore(String plasmaStorePath) {
        int occupiedMemoryMB = 10;
        long memoryBytes = occupiedMemoryMB * 1000000;
        int numRetries = 10;
        Process p = null;
        while (numRetries-- > 0) {
            int currentPort = java.util.concurrent.ThreadLocalRandom.current().nextInt(0, 100000);
            String name = storeSuffix + currentPort;
            String cmd = plasmaStorePath + " -s " + name + " -m " + memoryBytes;

            p = startProcess(cmd.split(" "));

            if (p != null && p.isAlive()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (p.isAlive()) {
                    storePort = currentPort;
                    break;
                }
            }
        }


        if (p == null || !p.isAlive()) {
            throw new RuntimeException("Start object store failed ...");
        } else {
            storeProcess = p;
            System.out.println("Start object store success");
        }
    }

    private void cleanup() {
        if (storeProcess != null && killProcess(storeProcess)) {
            System.out.println("Kill plasma store process forcely");
        }
    }

    private static boolean killProcess(Process p) {
        if (p.isAlive()) {
            p.destroyForcibly();
            return true;
        } else {
            return false;
        }
    }

    private byte[] getArrayFilledWithValue(int arrayLength, byte val) {
        byte[] arr =  new byte[arrayLength];
        Arrays.fill(arr, val);
        return arr;
    }

    public String getStoreAddress() {
        return storeSuffix + storePort;
    }
}
