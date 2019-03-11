
package com.stephanLab;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.base.Optional;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.NodeSet;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.OWLReasonerFactory;
import org.semanticweb.owlapi.reasoner.structural.StructuralReasonerFactory;
import com.google.common.base.Optional;

import javax.swing.text.InternationalFormatter;
import javax.swing.text.html.HTMLDocument;
import javax.swing.text.html.Option;
import java.awt.peer.SystemTrayPeer;
import java.io.*;
import java.net.URI;
import java.net.URLEncoder;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by John on 12/27/2016.
 */
public class OwlCompare implements Runnable {


    private Map<String, String> settings = new HashMap<>();
    private String version;
    FileWriter writer = null;

    File destDir = new File("ontology_modules");
    File settingsFile = null;

    public void setSettings( Map<String,String> args) {
        if (args.containsKey("out")) {
            destDir = new File(args.get("out"));
        }

        if (args.containsKey("settings")) {
            System.out.println("loading settings file " + args.get("settings"));
            settingsFile = new File(args.get("settings"));
        }
    }
    public void run() {

        try {
            System.out.println("loading Settings File");
            if(settingsFile != null)
                loadSettings(settingsFile);
        } catch (Exception e) {
            System.out.println("Settings exception occured" + e);
            System.exit(1);
        }

        try {
            File[] listOfFiles = destDir.listFiles();
            List<String> files = new ArrayList<String>();
            if (listOfFiles == null)
                throw new Exception("No files found");
            for (File f : listOfFiles) {
                if(Thread.currentThread().isInterrupted()){
                    return;
                }
                if (f.getName().startsWith(settings.get("ontologyShortName") + "_vocabulary"))
                    files.add(f.getName().substring(0, f.getName().indexOf(".owl")));
            }

            writer = new FileWriter("QCreport.txt");

            HashMap<String, HashMap<String, String>> onto1 = getURIs(files, destDir.getCanonicalPath(), true);
            Collection<String> onto1Keys = onto1.keySet();
            String version1 = version;


            Map<String, String> lastID = new HashMap<>();
            for (String URI : onto1.keySet()) {
                if(Thread.currentThread().isInterrupted()){
                    return;
                }
                if (URI.equals("Thing"))
                    continue;
                String fullID = URI.substring(URI.indexOf("_") + 1);
                Integer subID = Integer.parseInt(fullID.substring(3));
                String sSubID = fullID.substring(3);
                String groupID = fullID.substring(0, 3);
                if (!lastID.containsKey(groupID)) {
                    lastID.put(groupID, sSubID);
                } else if (subID > Integer.parseInt(lastID.get(groupID))) {
                    lastID.put(groupID, sSubID);
                }
            }

            if(Thread.currentThread().isInterrupted()){
                return;
            }

            List<String> keys = new ArrayList(lastID.keySet());
            Collections.sort(keys, new Comparator<String>() {

                public int compare(String o1, String o2) {
                    return Integer.parseInt(o1) - Integer.parseInt(o2);
                }
            });
            outputQC("");
            outputQC("");
            outputQC("");
            outputQC("Last used Ids in each group");
            outputQC("--------------------------------------");
            for (String key : keys) {
                if(Thread.currentThread().isInterrupted()){
                    return;
                }
                String GroupID = key;
                String lastSubID = lastID.get(key);
                if(onto1.get(settings.get("ontologyShortName").toUpperCase() + "_" + GroupID + lastSubID) != null)
                    outputQC(GroupID + lastSubID + "\t\t\t" + onto1.get(settings.get("ontologyShortName").toUpperCase() + "_" + GroupID + lastSubID).get("label"));
            }
            outputQC("--------------------------------------");
            outputQC("");
            outputQC("");
            String prefix = settings.get("ontologyURL") + "/";
            if (!settings.getOrDefault("vocabularyURL", "").equals("")) {
                prefix = prefix + settings.getOrDefault("vocabularyURL", "") + "/";
            }


            HashMap<String, HashMap<String, String>> onto2 = getURIs(files, prefix, false);
            Collection<String> onto2Keys = onto2.keySet();
            String version2 = version;
            if(Thread.currentThread().isInterrupted()){
                return;
            }
            List<String> onlyIn1 = new ArrayList<>(onto1.keySet());
            onlyIn1.removeAll(onto2Keys);
            Comparator<String> comparator = new Comparator<String>() {
                public int compare(String left, String right) {
                    if ((left.toLowerCase().contains(settings.get("ontologyShortName")) && !right.toLowerCase().contains(settings.get("ontologyShortName"))) || !left.contains("_"))
                        return 1;
                    else if ((!left.toLowerCase().contains(settings.get("ontologyShortName")) && right.toLowerCase().contains(settings.get("ontologyShortName"))) || !right.contains("_"))
                        return -1;
                    else {
                        return Integer.parseInt(left.substring(left.lastIndexOf("_") + 1)) - Integer.parseInt(right.substring(right.lastIndexOf("_") + 1));
                    }
                }
            };
            Collections.sort(onlyIn1, comparator);
            outputQC("Only in version " + version1);
            outputQC("--------------------------------------");
            for (String URI : onlyIn1) {
                HashMap<String, String> val = onto1.get(URI);
                StringBuilder s = new StringBuilder();
                int i = 0;
                if (val.get("label") != null) {
                    i = val.get("label").length();
                }
                for (; i < 60; i++) {
                    s.append(" ");
                }
                outputQC(URI + " " + val.get("label") + s.toString() + "\t" + val.get("module"));
            }
            if(Thread.currentThread().isInterrupted()){
                return;
            }
            outputQC("--------------------------------------");
            List<String> onlyIn2 = new ArrayList<String>(onto2Keys);
            onlyIn2.removeAll(onto1Keys);
            Collections.sort(onlyIn2, comparator);
            outputQC("Only in version " + version2);
            outputQC("--------------------------------------");
            for (String URI : onlyIn2) {
                HashMap<String, String> val = onto2.get(URI);

                StringBuilder s = new StringBuilder();
                int i = 0;
                if (val.get("label") != null) {
                    i = val.get("label").length();
                }
                for (; i < 60; i++) {
                    s.append(" ");
                }

                outputQC(URI + " " + val.get("label") + s.toString() + "\t" + val.get("module"));
            }
            outputQC("--------------------------------------");

            writer.close();
        }catch(Exception e){
            System.out.println(e);
        }

    }


    private void loadSettings(File settingsFile) throws Exception {
        char separator =',';
        CSVReader csvReader = null;

        //try and read the mapping file
        try {
            csvReader = new CSVReader(new FileReader(settingsFile), separator);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //make sure we can read the file
        String[] headerRow = csvReader.readNext();

        if (null == headerRow) {
            throw new FileNotFoundException(
                    "No columns defined in given CSV file." +
                            "Please check the CSV file format.");
        }
        while(null != headerRow)
        {
            settings.put(headerRow[0],headerRow[1]);
            headerRow = csvReader.readNext();
        }


        if(!settings.containsKey("ontologyURL")){
            throw new IllegalArgumentException("ontologyURL not present in settings file");
        }
    }


    private HashMap<String,HashMap<String,String>> getURIs(List<String> files,boolean outputDuplicates){
        return  getURIs(files,"",outputDuplicates);
    }



    private HashMap<String,HashMap<String,String>> getURIs(List<String> files, String prefix,boolean outputDuplicates){
        int totalClasses= 0;
        int duplicateURI = 0;
        Collection<String> URIs = new ArrayList<>();
        OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
        OWLDataFactory factory = manager.getOWLDataFactory();
        OWLReasonerFactory configuredReasoner = null;
        try {
            configuredReasoner = new StructuralReasonerFactory();
        }catch (Exception e){
            System.exit(0);
        }
        HashMap<String,Integer> duplicateAnnotations = new HashMap<>();

        List<String> columns = new ArrayList<>();
        HashMap<String,HashMap<String,String>> dataStorage = new  HashMap<>();
        for(String temp :files) {
            IRI DocumentIRI = null;
            if(prefix.contains("http")) {
                DocumentIRI = IRI.create(prefix + temp + ".owl");
            }else{
                DocumentIRI = IRI.create(new File(prefix+File.separator + temp + ".owl"));
            }
            try {
                OWLOntology currentOntology = manager.loadOntology(DocumentIRI);
                for(OWLAnnotation annotation :currentOntology.getAnnotations()){
                    if(annotation.getProperty().toString().equals("owl:versionInfo"))
                        version = annotation.getValue().toString();
                }
                OWLReasoner reasoner = configuredReasoner.createReasoner(currentOntology);
                Set<OWLClass> classes = currentOntology.getClassesInSignature();
                for (OWLClass aclass : classes) { // Get the annotations on the class that use the label property
                    Optional<String> IDOptional = aclass.getIRI().getRemainder();
                    if(IDOptional.isPresent()) {
                        String ID = IDOptional.get();
                        if (dataStorage.containsKey(ID) && !ID.equals("Thing")) {
                            System.out.println(ID + " Found Twice in " + temp + " and " + dataStorage.get(ID).get("module"));
                            duplicateURI++;
                        }
                        dataStorage.put(ID, new HashMap<String, String>());
                        URIs.add(ID);
                        dataStorage.get(ID).put("module", temp);
                        dataStorage.get(ID).put("resource", aclass.getIRI().getNamespace());
                        dataStorage.get(ID).put("ID", ID);
                        Iterator<OWLClass> itClass = reasoner.getSuperClasses(aclass, true).getFlattened().iterator();
                        if (itClass.hasNext()) {
                            Optional<String> parent = itClass.next().getIRI().getRemainder();
                            if (parent.isPresent()) {
                                dataStorage.get(ID).put("parentClass", parent.get());
                                dataStorage.get(ID).put("parentClassResource", reasoner.getSuperClasses(aclass, true).getFlattened().iterator().next().getIRI().getNamespace());
                            }
                        }

                        for (OWLOntology o : currentOntology.getImportsClosure()) {
                            Set<OWLAnnotationAssertionAxiom> a = o.getAnnotationAssertionAxioms(aclass.getIRI());
                            for (OWLAnnotationAssertionAxiom annotation : a) {
                                Optional<OWLLiteral> optionalV = annotation.getValue().asLiteral();
                                if(!optionalV.isPresent()){
                                    continue;
                                }
                                String value = optionalV.get().getLiteral();

                                Optional<String> optionaProp = annotation.getProperty().getIRI().getRemainder();
                                if(!optionaProp.isPresent()){
                                    continue;
                                }
                                String Property = optionaProp.get();
                                if (!duplicateAnnotations.containsKey(Property)) { //just for checking duplicates
                                    duplicateAnnotations.put(Property, 0);
                                }
                                if (dataStorage.get(ID).containsKey(Property)) {
                                    duplicateAnnotations.put(Property, duplicateAnnotations.get(Property) + 1);
                                    //System.out.println(ID + " already contains property '" + Property + "' with value '" + dataStorage.get(ID).get(Property) + "', new value '" + value + "'");
                                }
                                dataStorage.get(ID).put(Property, value);
                                if (!columns.contains(Property)) {
                                    columns.add(Property);
                                }
                                //System.out.println(Property+" = "+value);
                            }
                        }
                        totalClasses++;
                    }
                }
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
        if(outputDuplicates) {
            outputQC("------------------------");
            outputQC("Total Classes: " + totalClasses);
            outputQC("Duplicate Classes: " + duplicateURI);
            outputQC("Duplicate Count");
            for (Map.Entry<String, Integer> entry : duplicateAnnotations.entrySet()) {
                String key = entry.getKey();
                Integer map = entry.getValue();
                outputQC(key + " : " + map);
            }
        }
        return dataStorage;
    }

    private void outputQC(String message){
        System.out.println(message);
        try {
            writer.write(message + "\r\n");
        }catch( Exception e){
            System.out.println("Unable to write QC report");
            System.out.println(e);
        }
    }
}
