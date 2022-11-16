package stephanLab;

import au.com.bytecode.opencsv.CSVReader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by John on 11/4/2016.
 */
public class BaoImporter implements Runnable{

    private File annotationMapFile;

    private File settingsFile;

    private Connection conn = null;


    private String ontologyURL = "";


    private Map<String, String> settings = new HashMap<>();

    public void setSettings(Map<String,String> args){
        if (args.containsKey("updateFile")) {
            System.out.println("loading map file " + args.get("updateFile"));
            annotationMapFile = new File(args.get("updateFile"));
        }
        if (args.containsKey("settings")) {
            System.out.println("loading settings file " + args.get("settings"));
            settingsFile = new File(args.get("settings"));
        }
    }

    public void run(){


        settings.put("url","jdbc:mysql://127.0.0.1:3306/bao_text");
        settings.put("username","root");
        settings.put("password","pass");
        System.out.println("Connecting database...");
        this.conn = null;

        try {
            System.out.println("loading Settings File");
            loadSettings();
        } catch (Exception e) {
            System.out.println("Settings exception occured" + e);
            System.exit(1);
        }

        try {
            this.conn = DriverManager.getConnection(settings.get("url"), settings.get("username"), settings.get("password"));
            System.out.println("Database connected!");
        } catch (SQLException e) {
            System.out.println("SQL exception occured" + e);
            System.exit(1);
        }

        try {
            System.out.println("Adding New Classes");
            addRecords();

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void loadSettings() throws Exception {
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
        ontologyURL = settings.get("ontologyURL");
    }

    private void addRecords() throws Exception
    {

        //will rewrite later to load from a config file, currently holds a map of the id ranges and which file they go to
        char separator =',';
        CSVReader csvReader = null;
        Map<String, HashMap<String,String>> idMap = new HashMap<>();
        ArrayList<String> headers = new ArrayList<>();
        Map<String,String> moduleID = new HashMap<>();
        Map<String,Integer> nextID = new HashMap<>();
        moduleID.put("vocabulary_assay",        "001");
        nextID.put("001",0);
        moduleID.put("vocabulary_format",       "002");
        nextID.put("002",0);
        moduleID.put("vocabulary_biology",      "003");
        nextID.put("003",0);
        moduleID.put("vocabulary_method",       "004");
        nextID.put("004",0);
        moduleID.put("vocabulary_detection",    "005");
        nextID.put("005",0);
        moduleID.put("vocabulary_computational","006");
        nextID.put("006",0);
        moduleID.put("vocabulary_screenedentity","007");
        nextID.put("007",0);
        moduleID.put("vocabulary_result",       "008");
        nextID.put("008",0);
        moduleID.put("vocabulary_properties",   "009");
        nextID.put("009",0);
        moduleID.put("vocabulary_materialentity","010");
        nextID.put("010",0);
        moduleID.put("vocabulary_role",         "011");
        nextID.put("011",0);
        moduleID.put("vocabulary_quality",      "012");
        nextID.put("012",0);
        moduleID.put("vocabulary_function",     "013");
        nextID.put("013",0);
        moduleID.put("vocabulary_assaykit",     "014");
        nextID.put("014",0);
        moduleID.put("vocabulary_instrument",   "015");
        nextID.put("015",0);
        moduleID.put("vocabulary_people",       "016");
        nextID.put("016",0);
        moduleID.put("vocabulary_phenotype",    "017");
        nextID.put("017",0);
        moduleID.put("vocabulary_software" ,    "018");
        nextID.put("018",0);
        moduleID.put("vocabulary_unit",         "019");
        nextID.put("019",0);


        PreparedStatement sqlDB;
        ResultSet rs1;


        if(!settings.containsKey("labelColumn")&&!settings.containsKey("parentIDColumn")){
            throw new IllegalArgumentException("Settings file missing label or parent column");
        }
        //try and read the mapping file
        try {
            csvReader = new CSVReader(new FileReader(annotationMapFile), separator);
        } catch (Exception e) {
            e.printStackTrace();
        }

        //make sure we can read the file
        String[] row = csvReader.readNext();

        if (null == row) {
            throw new FileNotFoundException(
                    "No columns defined in given CSV file." +
                            "Please check the CSV file format.");
        }

        //get all entities from table and store the highest ID according to above groups
        sqlDB = conn.prepareStatement("SELECT module FROM entity WHERE label = ? ");
        rs1 = sqlDB.executeQuery("select module,substr(id from 5 for 3) as idGroup,max(substr(id from 8)) as nextID  from entity group by module,substr(id from 5 for 3);");
        while (rs1.next()) {
            if(!rs1.getString("idGroup").equals("000") && isNumeric(rs1.getString("idGroup"))){
                String next = rs1.getString("nextID");
                Integer id = Integer.parseInt(next);
                if(!next.contains("_") && (!nextID.containsKey(rs1.getString("idGroup"))||nextID.get(rs1.getString("idGroup")) < id)) {
                    nextID.put(rs1.getString("idGroup"),Integer.parseInt(next) );
                }
            }
        }
        int rowNum = 1;
        conn.setAutoCommit(false);
        FileWriter writer =  null;
        FileWriter writer2 =  null;
        FileWriter writer3 =  null;

        //import new terms and save information the a raw SQL file, a human readable file, and a BAE specific file
        try {
            writer = new FileWriter("sql-update.txt",true);
            writer2 = new FileWriter("sql-update-human.txt",true);
            writer3 = new FileWriter("sql-update-bae.txt",true);
            writer3.write("BAOTerm,BAETerm\r\n");
            while (null != row) {

                if (rowNum == 1) {
                    headers = new ArrayList<String>(Arrays.asList(row));
                } else {
                    if(row[0].equals("new term")) {
                        int parentIndex = headers.indexOf(settings.get("parentIDColumn"));
                        int parentColumnIndex = headers.indexOf(settings.get("parentLabelColumn"));
                        int lableIndex = headers.indexOf(settings.get("labelColumn"));
                        int newParentIndex = headers.indexOf(settings.get("newParentColumn"));
                        int defintionIndex = headers.indexOf(settings.get("descriptionColumn"));
                        int baeIndex = headers.indexOf(settings.get("baeColumn"));
                        String parentID = "";
                        String module = "";
                        if (row.length > parentIndex) {
                            parentID = row[parentIndex];
                            if (parentID.contains("#")) {
                                parentID = parentID.substring(parentID.lastIndexOf("#") + 1);
                            }
                            if (parentID.contains("\\")) {
                                parentID = parentID.substring(parentID.lastIndexOf("#") + 1);
                            }
                            sqlDB = conn.prepareStatement("SELECT module FROM entity WHERE id = ? ");
                            sqlDB.setString(1, row[parentIndex]);
                            System.out.println(row[parentColumnIndex]);
                            rs1 = sqlDB.executeQuery();
                            if (rs1.next()) {
                                module = rs1.getString("module");
                            } else {
                                if (idMap.containsKey(row[newParentIndex])) {
                                    module = idMap.get(row[newParentIndex]).get("module");
                                    parentID = idMap.get(row[newParentIndex]).get("id");
                                } else {
                                    System.out.println("Parent not found for "+row[lableIndex]);
                                    row = csvReader.readNext();
                                    continue;
                                }

                            }
                        }
                        Integer newSubID = nextID.get(moduleID.get(module)) + 1;
                        nextID.put(moduleID.get(module), newSubID);
                        String termID = "BAO_" + moduleID.get(module) + String.format("%4d", newSubID).replace(' ', '0');
                        writer.write("INSERT INTO `entity` VALUES ('" + termID + "', '" + row[lableIndex].replace("'", "\\'") + "', '" + module + "');\r\n");
                        writer2.write("Added class '" + row[lableIndex] + "' with ID <" + termID + "> as a child of <" + parentID + ">\r\n");
                        writer.write("INSERT INTO `entityinfo` (`entityid`,`property_label`,`property_type`,`value`) VALUES ('" + termID + "', 'subClassOf','rdf_property', '" + parentID + "');\r\n");
                        if (!row[defintionIndex].equals("")) {
                            writer.write("INSERT INTO `entityinfo` (`entityid`,`property_label`,`property_type`,`value`) VALUES ('" + termID + "', 'obo:IAO_0000115','annotation_property', '" + row[defintionIndex].replace("'", "\\'") + "');\r\n");
                        }
                        if (!row[baeIndex].equals("")) {
                            writer3.write(termID + "," + row[baeIndex] + "\r\n");
                        }

                        HashMap<String, String> newTerm = new HashMap<>();
                        newTerm.put("parent", parentID);
                        newTerm.put("module", module);
                        newTerm.put("id", termID);
                        idMap.put(row[lableIndex], newTerm);
                    }else if(row[0].equals("external term")) {
                        int parentIndex = headers.indexOf(settings.get("parentIDColumn"));
                        int lableIndex = headers.indexOf(settings.get("labelColumn"));
                        int externalColumn = headers.indexOf(settings.get("externalColumn"));
                        String parentID = "";
                        String module = "";
                        if (row.length > parentIndex) {
                            parentID = row[parentIndex];
                            if (parentID.contains("#")) {
                                parentID = parentID.substring(parentID.lastIndexOf("#") + 1);
                            }
                            if (parentID.contains("\\")) {
                                parentID = parentID.substring(parentID.lastIndexOf("#") + 1);
                            }
                        }
                        writer.write("INSERT INTO `external` VALUES ('" + row[externalColumn]  + "', '" + row[externalColumn].substring(row[externalColumn].lastIndexOf("/")+1,row[externalColumn].indexOf("_")) + "', '" +row[parentIndex]  + "',0);\r\n");
                        writer2.write("Added external class '" + row[lableIndex] + "' with ID <" + row[externalColumn] + "> as a child of <" + parentID + ">\r\n");
                        HashMap<String, String> newTerm = new HashMap<>();
                        newTerm.put("parent", parentID);
                        newTerm.put("module", module);
                        newTerm.put("id", row[externalColumn]);
                        idMap.put(row[lableIndex], newTerm);
                    }else{
                        row = csvReader.readNext();
                        continue;
                    }
                }
                rowNum++;
                row = csvReader.readNext();
            }
        }
        catch(Exception e){
            conn.rollback();
        }
        finally {
            if(writer != null) {
                writer.close();
            }
            if(writer2 != null) {
                writer2.close();
            }
            if(writer3 != null) {
                writer3.close();
            }
            conn.setAutoCommit(true);
        }
    }
    public boolean isNumeric(String s) {
        return s != null && s.matches("[-+]?\\d*\\.?\\d+");
    }
}
