package koresigma.arrowpoc;

import org.apache.arrow.plasma.PlasmaClient;

import java.nio.file.Paths;

public class ArrowPoc {

    // Install Miniconda for Python 3
    // To directory: ~/.local/lib/miniconda3/

    // Install Arrow libraries:
    // $ cd ~/.local/lib/miniconda3/bin/
    // $ ./conda install arrow-cpp=0.11.* -c conda-forge
    // $ ./conda install pyarrow=0.11.* -c conda-forge

    // In Intellij, edit run configuration:
    // VM Arguments: -Djava.library.path="/home/<USERNAME>/.local/lib/miniconda3/lib/"

    public static void main(String[] args) {
        System.out.println("Java libraries path: " + System.getProperty("java.library.path"));

        String[] libs = new String[]{
                "~/.local/lib/miniconda3/pkgs/icu-58.2-hfc679d8_0/lib/libicui18n.so.58",
                "~/.local/lib/miniconda3/pkgs/icu-58.2-hfc679d8_0/lib/libicudata.so.58",
                "~/.local/lib/miniconda3/pkgs/boost-cpp-1.68.0-h3a22d5f_0/lib/libboost_regex.so.1.68.0",
                "~/.local/lib/miniconda3/pkgs/boost-cpp-1.68.0-h3a22d5f_0/lib/libboost_filesystem.so.1.68.0",
                "~/.local/lib/miniconda3/pkgs/boost-cpp-1.68.0-h3a22d5f_0/lib/libboost_system.so.1.68.0",
                "~/.local/lib/miniconda3/pkgs/arrow-cpp-0.11.1-py36h3bd774a_0/lib/libplasma.so.11.1.0"
        };

        // Replace '~' in paths with home directory
        String homeDir = System.getProperty("user.home");
        for (int i = 0; i < libs.length; i++) {
            libs[i] = libs[i].replaceFirst("^~", homeDir);
        }

        for (String lib : libs) {
            String libAbsolutePath = Paths
                    .get(lib)
                    .toAbsolutePath()
                    .toString();

            System.load(libAbsolutePath);
        }

        client();
    }

    private static void client() {
        PlasmaClient plasmaClient = new PlasmaClient("/tmp/plasma", "", 0);
        byte[] byteObject = null;


//        plasmaClient.put();
    }
}
