package stephanLab;

import javax.swing.*;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) {
        try {
            Map<String, String> argArray = new HashMap<>();
            for (int i = 0; i < args.length; i++) {
                String key = "";
                String val = "True";
                if (args[i].startsWith("-")) {
                    if (args[i].contains("=")) {
                        key = args[i].substring(1, args[i].indexOf("="));
                        val = args[i].substring(args[i].indexOf("=") + 1);
                    } else {
                        key = args[i].substring(1);
                        if ((args.length - 1) != i) {
                            val = args[i + 1];
                        }
                    }
                }
                argArray.put(key, val);
            }
            if (!argArray.containsKey("nogui")) {
                JFrame frame = new JFrame("Ontojog");
                frame.setContentPane(new MainWindow().panelMain);
                frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
                frame.pack();
                frame.setVisible(true);
            } else {
                if(argArray.containsKey("action")){
                    if(argArray.get("action").equals("qc")){
                        OwlCompare comp = new OwlCompare();
                        comp.setSettings(argArray);
                        comp.run();

                    }
                    return;
                }
                if (argArray.containsKey("updateFile")) {
                    BaoImporter gen = new BaoImporter();
                    gen.setSettings(argArray);
                    gen.run();
                } else {
                    OwlGenerator gen = new OwlGenerator();
                    gen.setSettings(argArray);
                    gen.run();
                }
                if (!argArray.containsKey("noQC")) {
                    OwlCompare comp = new OwlCompare();
                    comp.setSettings(argArray);
                    comp.run();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
