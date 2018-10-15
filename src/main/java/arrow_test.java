import org.apache.arrow.plasma.*;

public class arrow_test {

    private void client(){
        PlasmaClient plasmaClient = new PlasmaClient("/tmp/plasma", "", 0);
        byte[] byteObject = null;


        plasmaClient.put();
    }

    public static void main(String[] args){

    }
}
