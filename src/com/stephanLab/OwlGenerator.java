package com.stephanLab;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.Map.Entry;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringEscapeUtils;

import javax.swing.*;

/**
 * Created by John on 11/4/2016.
 */
public class OwlGenerator implements Runnable {

    private File destDir;
    private File dbFile;
    private File annotationMapFile;
    private File mapFile;
    private File settingsFile;
    private File reParent;
    private String RelationFile;
    private boolean cleanDB = false;
    private boolean cleanRelationships = false;
    private Connection conn = null;

    private boolean runAddtion = true;
    private boolean runAxioms = true;
    private boolean runGeneration = true;
    private boolean legacyMode = false;

    private Map<String, String> settings = new HashMap<>();

    private ArrayList<String> moduleBlacklist = new ArrayList<>();
    private ArrayList<String> moduleWhitelist = new ArrayList<>();


    private Map<String, String> modules = new HashMap<>();
    private Map<String, String> idHolder = new HashMap<>();
    private Map<String, Integer> classCounter = new HashMap<>();
    private ArrayList<HashMap<String,String>> info = new ArrayList<>();
    private Map<String, HashMap<String,Object>> relation = new HashMap<>();
    private Map<String, HashMap<String,Object>> equivalence = new HashMap<>();

    private String extraColumns ="";
    private String appendName = "";
    private String prependName = "";



    private ArrayList<HashMap<String,String>> infoG = new ArrayList<>();
    private Map<String, HashMap<String,Object>> relationG = new HashMap<>();
    private Map<String, HashMap<String,Object>> equivalenceG = new HashMap<>();
    private String extraColumnsG ="";
    private String appendNameG = "";
    private String prependNameG = "";

    private Map<String, HashMap<String,Object>> collectionMap = new HashMap<>();

    private String ontologyURL = "";
    private boolean inGroup = false;
    public JTextArea logWindow= null;

    public void setSettings( Map<String,String> args){
        if (args.containsKey("db")) {
            outputString("loading data file " + args.get("db"));
            dbFile = new File(args.get("db"));
        }
        if (args.containsKey("annomap")) {
            outputString("loading map file " + args.get("annomap"));
            annotationMapFile = new File(args.get("annomap"));
        }
        if (args.containsKey("map")) {
            outputString("loading map file " + args.get("map"));
            mapFile = new File(args.get("map"));
        }

        if (args.containsKey("settings")) {
            outputString("loading settings file " + args.get("settings"));
            settingsFile = new File(args.get("settings"));
        }

        if (args.containsKey("out")) {
            destDir = new File(args.get("out"));
        }

        if (args.containsKey("reparent")) {
            reParent = new File(args.get("reparent"));
        }

        if (args.containsKey("ignoreModules")) {
            moduleBlacklist = new ArrayList<String>(Arrays.asList(args.get("ignoreModules").split(",")));
        }

        if (args.containsKey("onlyModules")) {
            moduleWhitelist = new ArrayList<String>(Arrays.asList(args.get("onlyModules").split(",")));
        }

        if (args.containsKey("cleanDB")) {
            cleanDB = true;
        }
        if (args.containsKey("cleanRelationships")) {
            cleanRelationships = true;
        }

        if (args.containsKey("generate")) {
            runAddtion = false;
            runAxioms = false;
        }

        if (args.containsKey("add")) {
            runAxioms = false;
            runGeneration =false;
        }
        if (args.containsKey("legacy")) {
            legacyMode =true;
        }
    }

    public void run(){
        try {
            if (mapFile == null && legacyMode) {
                throw new IllegalArgumentException("-map not set as command line argument");
            }
            if (dbFile == null && legacyMode) {
                throw new IllegalArgumentException("-db not set as command line argument");
            }


            if (destDir == null) {
                destDir = new File("ontology_modules");
                if (!destDir.exists()) {
                    outputString("Create directory: \"ontology_modules\"");
                    boolean result = false;
                    try {
                        destDir.mkdir();
                        result = true;
                    } catch (SecurityException se) {
                    }
                    if (result) {
                        outputString("Directory created!");
                    }
                }
            }
            settings.put("url", "jdbc:mysql://127.0.0.1:3306/dto_text");
            settings.put("username", "root");
            settings.put("password", "pass");
            outputString("Connecting database...");
            this.conn = null;

            try {
                outputString("loading Settings File");
                loadSettings();
            } catch (Exception e) {
                outputString("Settings exception occured" + e);
                System.exit(1);
            }

            try {
                this.conn = DriverManager.getConnection(settings.get("url") + "?useUnicode=true&characterEncoding=utf-8", settings.get("username"), settings.get("password"));
                outputString("Database connected!");
            } catch (SQLException e) {
                outputString("SQL exception occured" + e);
                System.exit(1);
            }

            try {
                outputString("Loading Database");
                loadDatabase();

                if (runAxioms || legacyMode) {
                    outputString("Processing Mapping File");
                    processMapping();

                    if (reParent != null) {
                        outputString("Processing Reparent File");
                        processReparent();
                    }
                    outputString("Removing Redundant Axioms");
                    removeRedundant();


                    String statement = "INSERT INTO relationship (entityid,property,value,restriction) SELECT relationship3.entityid,relationship3.property,relationship3.`value`,relationship3.restriction FROM relationship3";
                    Statement sqlQ = conn.createStatement();
                    sqlQ.execute(statement);
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
            if (runGeneration || legacyMode) {
                try {
                    outputString("Generate Owl Modules");
                    generateModules();
                    generateExternalModules();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }

                try {
                    outputString("Generate Owl Axioms");
                    generateRelations();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(1);
                }
            }
        }
        catch(Exception e){
            System.out.println(e);
        }
    }

    private void outputString(String message){
        System.out.println(message);
        if(logWindow != null){
            logWindow.append(message+"\r\n");
        }
    }

    private void loadDatabase() throws Exception {
        Statement sqlQ = conn.createStatement();
        //Drop the ontology tables if they are currently there
        if (cleanDB)
            sqlQ.execute("DROP TABLE IF EXISTS entity  ;");
        if (cleanRelationships) {
            sqlQ.execute("DROP TABLE IF EXISTS entityinfo;");
            sqlQ.execute("DROP TABLE IF EXISTS relationship;");
            sqlQ.execute("DROP TABLE IF EXISTS equivalence;");
        }
        //add the ontology tables
        sqlQ.execute("create table IF NOT EXISTS entity(id varchar(255), label varchar(255), module varchar(255));");
        sqlQ.execute("create table IF NOT EXISTS entityinfo(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,entityid varchar(255), property_label varchar(255), property_type varchar(255), value varchar(255)); ");
        sqlQ.execute("create table IF NOT EXISTS relationship(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,entityid varchar(255), property varchar(255), value varchar(255), restriction varchar(255),collection int(255));");
        sqlQ.execute("create table IF NOT EXISTS equivalence(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,entityid varchar(255), property varchar(255), value varchar(255), restriction varchar(255),collection int(255));");
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

        RelationFile = settings.getOrDefault("relationFile","automated_axioms");
    }


    private void processMapping() throws Exception
    {
        String baseID = "";
        int SubClassLength = 1;
        char separator =',';
        CSVReader csvReader = null;

        Map<String, HashMap<String,String>> idMap = new HashMap<>();


        collectionMap.put("equivalence",new HashMap<>());
        collectionMap.get("equivalence").put("lastCollection","");
        collectionMap.get("equivalence").put("collectionNum",0);
        collectionMap.put("relation",new HashMap<>());
        collectionMap.get("relation").put("lastCollection","");
        collectionMap.get("relation").put("collectionNum",0);

        //try and read the mapping file
        try {
            csvReader = new CSVReader(new FileReader(mapFile), separator);
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
        PreparedStatement sqlQ;
        PreparedStatement sqlDB;
        PreparedStatement sqlDB2;

        ResultSet rs1;
        ResultSet rs2;

        String dataID = "";
        String ParentClassRow ="";
        String ClassIDRow ="";
        String ModuleRow ="";
        Boolean ParentDirect = false;
        //loop through the file and execute the mapping
        while(null != headerRow)
        {
            int firstReplacement = 0;
            int replacementLength;
            String id ="";
            if(headerRow[0].startsWith("##")){
                continue;
            }
            switch (headerRow[1].toLowerCase()){
                case "baseid": //set the base id system for the ontology
                    baseID = headerRow[2];
                    break;
                case "subclasslength": //set the length of the subclass ID
                    SubClassLength = Integer.parseInt(headerRow[2]);
                    break;
                case "setrelationfile": //set the length of the subclass ID
                    RelationFile = headerRow[2];
                    break;

                case "startgroup":
                    inGroup = true;
                    infoG.clear();
                    relationG.clear();
                    equivalenceG.clear();
                    extraColumnsG="";
                    appendNameG="";
                    prependNameG="";

                    break;
                case "endgroup":
                    inGroup = false;
                    infoG.clear();
                    relationG.clear();
                    equivalenceG.clear();
                    extraColumnsG="";
                    appendNameG="";
                    prependNameG="";
                    break;
                case "module": //set the module for a class
                    modules.put(headerRow[0],headerRow[2]);
                    break;
                case "prependname": //set the module for a class
                    if(inGroup) {
                        prependNameG = headerRow[2];
                    }else{
                        prependName = headerRow[2];
                    }
                    break;
                case "appendname":
                    if(inGroup) {
                        appendNameG = headerRow[2];
                    }else{
                        appendName = headerRow[2];
                    }
                    break;
                case "info":
                    HashMap<String,String> h = new HashMap<>();
                    h.put("column",headerRow[0]);
                    h.put("type",headerRow[3]);
                    h.put("label",headerRow[2]);
                    if(inGroup) {
                        infoG.add(h);
                        if (extraColumnsG.equals(""))
                            extraColumnsG += "`" + headerRow[0] + "`";
                        else
                            extraColumnsG += "," + "`" +headerRow[0] + "`";
                    }else{
                        info.add(h);
                        if (extraColumns.equals(""))
                            extraColumns += "`" + headerRow[0] + "`";
                        else
                            extraColumns += "," + "`" +headerRow[0] + "`";
                    }
                    break;
                case "overrideparentclass":
                    ParentClassRow =headerRow[0];
                    ParentDirect = false;
                    break;
                case "overrideparentclassdirect":
                    ParentClassRow =headerRow[0];
                    ParentDirect = true;
                    break;
                case "overrideid":
                    ClassIDRow =headerRow[0];
                    break;
                case "overridemodule":
                    ModuleRow =headerRow[0];
                    break;
                case "mainclass":
                    if(!modules.containsKey(headerRow[0])){
                        throw  new Exception("Module must be set before setting a main class");
                    }

                    firstReplacement = baseID.indexOf('?');
                    replacementLength = headerRow[2].length();
                    id = baseID.substring(0,firstReplacement)+headerRow[2]+baseID.substring(firstReplacement+replacementLength);
                    idHolder.put(headerRow[0],id);
                    classCounter.put(headerRow[0],1);

                    sqlDB = conn.prepareStatement("select id from entity where label = ? ");
                    sqlDB.setString(1,headerRow[0]);
                    rs1 = sqlDB.executeQuery();
                    if(rs1.next()){
                        outputString(headerRow[0] + " already present");
                    }else {
                        sqlQ = conn.prepareStatement("insert into entity (id, label, module) values (?,?,?);");
                        sqlQ.setString(1,id.replace('?', '0'));
                        sqlQ.setString(2,headerRow[0]);
                        sqlQ.setString(3,modules.get(headerRow[0]));
                        sqlQ.executeUpdate();
                    }
                    ParentClassRow ="";
                    break;
                case "entity":
                    sqlDB = conn.prepareStatement("select id from entity where label = ?");
                    sqlDB.setString(1,headerRow[0]);
                    rs1 = sqlDB.executeQuery(); //make sure we only add each entity to the database once with one ID
                    if(rs1.next()){
                        outputString(headerRow[0] + " already present");
                    }else {
                        sqlQ = conn.prepareStatement("insert into entity (id, label, module) values (?,?,'entities');");
                        sqlQ.setString(1,headerRow[2]);
                        sqlQ.setString(2,headerRow[0]);
                        sqlQ.executeUpdate();
                    }
                    break;
                case "subclass":
                    if(!modules.containsKey(headerRow[2])&& ModuleRow.equals("")){
                        throw  new Exception("Module must be set for main class before using subclass");
                    }
                    id = idHolder.get(headerRow[2]);
                    firstReplacement = id.indexOf('?');
                    id = id.substring(0,firstReplacement)+String.format("%0"+SubClassLength+"d",classCounter.get(headerRow[2]))+id.substring(firstReplacement+SubClassLength);
                    classCounter.put(headerRow[2],classCounter.get(headerRow[2])+1);
                    modules.put(headerRow[0],modules.get(headerRow[2])); //increment the subclass count for the parent module
                    idHolder.put(headerRow[0],id); //store the id for the class
                    classCounter.put(headerRow[0],1); //create own subclass counter
                    sqlDB = conn.prepareStatement("select id from entity where label = ?");
                    sqlDB.setString(1,headerRow[0]);
                    rs1 = sqlDB.executeQuery();
                    if(rs1.next()){
                        outputString(headerRow[0] + " already present");
                        id = rs1.getString(1);
                    }else {
                        sqlQ = conn.prepareStatement("insert into entity (id, label, module) values (?,?,?);");
                        sqlQ.setString(1,id.replace('?', '0'));
                        sqlQ.setString(2,headerRow[0]);
                        sqlQ.setString(3,modules.get(headerRow[0]));
                        sqlQ.executeUpdate();
                        id = id.replace('?','0');
                    }
                    sqlQ = conn.prepareStatement("insert into entityinfo (entityid, property_label, property_type, value) values(?,'subClassOf','rdf_property',?)");
                    sqlQ.setString(1,id);
                    sqlQ.setString(2,idHolder.get(headerRow[2]).replace('?','0'));
                    sqlQ.executeUpdate();
                    ParentClassRow =""; //Reset the ParentClass Variable to prevent problems later on
                    break;
                case "class": //adds all rows with non-null headerRow[0] as a subclass of headerRow[2]
                    if(!modules.containsKey(headerRow[2])&& ModuleRow.equals("")){
                        throw  new Exception("Module must be set for main class before using subclass");
                    }
                    int counter = 0;
                    if(ClassIDRow.equals("")) {
                        id = idHolder.get(headerRow[2]);
                        firstReplacement = id.indexOf('?');
                        for (int i = 0; i < id.length(); i++) { //get how much we need to pad
                            if (id.charAt(i) == '?') {
                                counter++;
                            }
                        }
                    }
                    String query = "select "+headerRow[0];
                    int ClassIDRowNum = 2;
                    int ModuleRowNum = 2;
                    if(!ParentClassRow.equals("")){
                        query+=",`"+ParentClassRow+"`"; //get the ID for the current entity from the data table for later
                        ClassIDRowNum++;
                        ModuleRowNum++;
                    }
                    if(!ClassIDRow.equals("")){
                        query+=",`"+ClassIDRow+"`"; //get the ID for the current entity from the data table for later
                        ModuleRowNum++;
                    }
                    if(!ModuleRow.equals("")){
                        query+=",`"+ModuleRow+"`"; //get the ID for the current entity from the data table for later
                    }
                    query += ",id FROM data_table WHERE "+headerRow[0]+" != \"\" GROUP BY "+headerRow[0]+";";
                    sqlDB = conn.prepareStatement(query);
                    rs2 = sqlDB.executeQuery(); //get the ID for the current entity from the data table for later
                    while(rs2.next()) {
                        dataID = rs2.getString("id");
                        String module = modules.get(headerRow[2]);
                        if(!ModuleRow.equals("")){
                            module = rs2.getString(ModuleRowNum);
                        }
                        if(!ClassIDRow.equals("")) {
                            id = rs2.getString(ClassIDRowNum);
                            String idString = id.replaceAll("[\\D]", "");
                            if(!idString.equals("")) {
                                int nextID = Integer.parseInt(idString);
                                if (!classCounter.containsKey(headerRow[2]) ||classCounter.get(headerRow[2]) < nextID) {
                                    classCounter.put(headerRow[2], nextID);
                                }
                            }
                        }else{
                            classCounter.put(headerRow[2], classCounter.get(headerRow[2]) + 1);
                            id = id.substring(0, firstReplacement) + String.format("%0" + counter + "d", classCounter.get(headerRow[2])) + id.substring(firstReplacement + counter);
                        }
                        sqlDB2 = conn.prepareStatement("select id from entity where label = ?");
                        sqlDB2.setString(1,prependName + prependNameG + rs2.getString(headerRow[0]) + appendName + appendNameG);
                        rs1 = sqlDB2.executeQuery(); //make sure we only add each entity to the database once with one ID
                        if(rs1.next()){
                            outputString(prependName + prependNameG + rs2.getString(headerRow[0]) + appendName + appendNameG + " already present");
                            id = rs1.getString(1);
                        }else {
                            sqlQ = conn.prepareStatement("insert into entity (id, label, module) values (?,?,?);");
                            sqlQ.setString(1,id);
                            sqlQ.setString(2,prependName + prependNameG + rs2.getString(headerRow[0]) + appendName + appendNameG);
                            sqlQ.setString(3,module);
                            sqlQ.executeUpdate();

                            if(!idMap.containsKey(dataID)){
                                idMap.put(dataID,new HashMap<>());
                            }
                            HashMap iMap = idMap.get(dataID);
                            iMap.put(prependName + prependNameG + rs2.getString(headerRow[0]) + appendName + appendNameG,id);
                        }
                        String parent =headerRow[2];
                        String parentID;
                        if(!ParentClassRow.equals("")){
                            if(!ParentDirect) {
                                rs1 = sqlDB2.executeQuery("select id from entity where label ='" + rs2.getString(2) + "'"); //make sure we only add each entity to the database once with one ID
                                rs1.next();
                                parentID = rs1.getString(1);
                            }else{
                                parentID = rs2.getString(2);
                            }
                        }else{
                            parentID = idHolder.get(parent);
                        }
                        if(parentID!=null) {
                            sqlQ = conn.prepareStatement("insert into entityinfo (entityid, property_label, property_type, value) values(?,'subClassOf','rdf_property',?)");
                            sqlQ.setString(1, id);
                            sqlQ.setString(2, parentID.replace('?', '0'));
                            sqlQ.executeUpdate();
                        }

                        addInfo(id,dataID);
                    }
                    ParentClassRow =""; //Reset the ParentClass Variable to prevent problems later on
                    break;
                case "staticclass":
                    if(!modules.containsKey(headerRow[2])){
                        throw  new Exception("Module must be set for main class before using subclass");
                    }
                    id = idHolder.get(headerRow[2]);
                    firstReplacement = id.indexOf('?');
                    counter = 0;
                    for( int i=0; i<id.length(); i++ ) { //get how much we need to pad
                        if( id.charAt(i) == '?' ) {
                            counter++;
                        }
                    }
                    classCounter.put(headerRow[2], classCounter.get(headerRow[2]) + 1);
                    id = id.substring(0, firstReplacement) + String.format("%0" + counter + "d", classCounter.get(headerRow[2])) + id.substring(firstReplacement + counter);
                    sqlDB = conn.prepareStatement("select id from entity where label = ?");
                    sqlDB.setString(1,headerRow[0]);
                    rs1 = sqlDB.executeQuery(); //make sure we only add each entity to the database once with one ID
                    if(rs1.next()){
                        outputString(headerRow[0] + " already present");
                        id = rs1.getString(1);
                    }else {
                        sqlQ = conn.prepareStatement("insert into entity (id, label, module) values (?,?,?);");
                        sqlQ.setString(1,id);
                        sqlQ.setString(2,headerRow[0]);
                        sqlQ.setString(3,modules.get(headerRow[2]));
                        sqlQ.executeUpdate();
                    }
                    sqlQ = conn.prepareStatement("insert into entityinfo (entityid, property_label, property_type, value) values(?,'subClassOf','rdf_property',?)");
                    sqlQ.setString(1,id);
                    sqlQ.setString(2,idHolder.get(headerRow[2]).replace('?','0'));
                    sqlQ.executeUpdate();
                    ParentClassRow =""; //Reset the ParentClass Variable to prevent problems later on
                    break;
            }
            headerRow = csvReader.readNext();
        }

        //try and read the mapping file
        try {
            csvReader = new CSVReader(new FileReader(mapFile), separator);
        } catch (Exception e) {
            e.printStackTrace();
        }

        headerRow = csvReader.readNext();
        while(null != headerRow)
        {
            if(headerRow[0].startsWith("##")){
                continue;
            }
            int firstReplacement;
            int replacementLength;
            String id;
            HashMap<String,Object> m;
            switch (headerRow[1].toLowerCase()){
                case "startgroup":
                    inGroup = true;
                    relationG.clear();
                    equivalenceG.clear();
                    extraColumnsG="";
                    appendNameG="";
                    prependNameG="";

                    break;
                case "endgroup":
                    inGroup = false;
                    relationG.clear();
                    equivalenceG.clear();
                    extraColumnsG="";
                    appendNameG="";
                    prependNameG="";
                    break;
                case "prependname": //set the module for a class
                    if(inGroup) {
                        prependNameG = headerRow[2];
                    }else{
                        prependName = headerRow[2];
                    }
                    break;
                case "appendname":
                    if(inGroup) {
                        appendNameG = headerRow[2];
                    }else{
                        appendName = headerRow[2];
                    }
                    break;
                case "equivalencestatic":
                    if(inGroup) {
                        if(!equivalenceG.containsKey(headerRow[0])){
                            equivalenceG.put(headerRow[0],new HashMap<>());
                        }
                        m = equivalenceG.get(headerRow[0]);
                    }else{
                        if(! equivalence.containsKey(headerRow[0])){
                            equivalence.put(headerRow[0],new HashMap<>());
                        }
                        m =  equivalence.get(headerRow[0]);
                    }
                    m.put("static",true);
                    m.put("property",headerRow[2]);
                    m.put("restriction",headerRow[3]);
                    sqlDB = conn.prepareStatement("select id from entity where label = ? ");
                    sqlDB.setString(1,headerRow[0]);
                    rs2 = sqlDB.executeQuery();
                    if(rs2.next()){
                        m.put("id",rs2.getString(1));
                    }else{
                        m.put("id",headerRow[0]);
                    }
                    break;
                case "equivalencecondition":
                    if(inGroup) {
                        if(! equivalenceG.containsKey(headerRow[0])){
                            equivalenceG.put(headerRow[0],new HashMap<>());
                        }
                        m =  equivalenceG.get(headerRow[0]);
                    }else{
                        if(! equivalence.containsKey(headerRow[0])){
                            equivalence.put(headerRow[0],new HashMap<>());
                        }
                        m = equivalence.get(headerRow[0]);
                    }

                    if(!m.containsKey("conditions")){
                        m.put("conditions",new HashMap<String,ArrayList<String>>());
                    }
                    @SuppressWarnings("unchecked")
                    HashMap<String,ArrayList<String>> h = (HashMap<String,ArrayList<String>>)m.get("conditions");
                    if(!h.containsKey(headerRow[2])){
                        h.put(headerRow[2],new ArrayList<>());
                    }
                    h.get(headerRow[2]).add(headerRow[3]);
                    break;
                case "equivalencedirect":
                    if(inGroup) {
                        if(! equivalenceG.containsKey(headerRow[0])){
                            equivalenceG.put(headerRow[0],new HashMap<>());
                        }
                        m =  equivalenceG.get(headerRow[0]);
                    }else{
                        if(! equivalence.containsKey(headerRow[0])){
                            equivalence.put(headerRow[0],new HashMap<>());
                        }
                        m =  equivalence.get(headerRow[0]);
                    }
                    m.put("column",headerRow[0]);
                    m.put("restriction",headerRow[3]);
                    m.put("property",headerRow[2]);
                    m.put("direct",true);
                    break;
                case "equivalence":
                    if(inGroup) {
                        if(! equivalenceG.containsKey(headerRow[0])){
                            equivalenceG.put(headerRow[0],new HashMap<>());
                        }
                        m = equivalenceG.get(headerRow[0]);
                    }else{
                        if(!equivalence.containsKey(headerRow[0])){
                            equivalence.put(headerRow[0],new HashMap<>());
                        }
                        m = equivalence.get(headerRow[0]);
                    }
                    m.put("column",headerRow[0]);
                    m.put("restriction",headerRow[3]);
                    m.put("property",headerRow[2]);
                    break;
                case "equivalencecolumn":
                    if(inGroup) {
                        if(!equivalenceG.containsKey(headerRow[0])){
                            equivalenceG.put(headerRow[0],new HashMap<>());
                        }
                        m = equivalenceG.get(headerRow[0]);
                    }else{
                        if(!equivalence.containsKey(headerRow[0])){
                            equivalence.put(headerRow[0],new HashMap<>());
                        }
                        m = equivalence.get(headerRow[0]);
                    }
                    m.put("column",headerRow[2]);
                    break;
                case "equivalencecollection":
                    if(inGroup) {
                        if(!equivalenceG.containsKey(headerRow[0])){
                            equivalenceG.put(headerRow[0],new HashMap<>());
                        }
                        m = equivalenceG.get(headerRow[0]);
                    }else{
                        if(!equivalence.containsKey(headerRow[0])){
                            equivalence.put(headerRow[0],new HashMap<>());
                        }
                        m = equivalence.get(headerRow[0]);
                    }
                    m.put("collection",headerRow[2]);
                    break;
                case "relationshipstatic":
                    if(inGroup) {
                        if(!relationG.containsKey(headerRow[0])){
                            relationG.put(headerRow[0],new HashMap<>());
                        }
                        m = relationG.get(headerRow[0]);
                    }else{
                        if(!relation.containsKey(headerRow[0])){
                            relation.put(headerRow[0],new HashMap<>());
                        }
                        m = relation.get(headerRow[0]);
                    }
                    m.put("static",true);
                    m.put("property",headerRow[2]);
                    m.put("restriction",headerRow[3]);
                    sqlDB2 = conn.prepareStatement("select id from entity where label = ?");
                    sqlDB2.setString(1,headerRow[0]);
                    rs2 = sqlDB2.executeQuery();
                    if(rs2.next()){
                        m.put("id",rs2.getString(1));
                    }else{
                        m.put("id",headerRow[0]);
                    }
                    break;
                case "relationshipcondition":
                    if(inGroup) {
                        if(!relationG.containsKey(headerRow[0])){
                            relationG.put(headerRow[0],new HashMap<>());
                        }
                        m = relationG.get(headerRow[0]);
                    }else{
                        if(!relation.containsKey(headerRow[0])){
                            relation.put(headerRow[0],new HashMap<>());
                        }
                        m = relation.get(headerRow[0]);
                    }

                    if(!m.containsKey("conditions")){
                        m.put("conditions",new HashMap<String,ArrayList<String>>());
                    }
                    @SuppressWarnings("unchecked")
                    HashMap<String,ArrayList<String>> h1 = (HashMap<String,ArrayList<String>>)m.get("conditions");
                    if(!h1.containsKey(headerRow[2])){
                        h1.put(headerRow[2],new ArrayList<String>());
                    }
                    h1.get(headerRow[2]).add(headerRow[3]);
                    break;
                case "relationshipdirect":
                    if(inGroup) {
                        if(!relationG.containsKey(headerRow[0])){
                            relationG.put(headerRow[0],new HashMap<>());
                        }
                        m = relationG.get(headerRow[0]);
                    }else{
                        if(!relation.containsKey(headerRow[0])){
                            relation.put(headerRow[0],new HashMap<>());
                        }
                        m = relation.get(headerRow[0]);
                    }
                    m.put("column",headerRow[0]);
                    m.put("restriction",headerRow[3]);
                    m.put("property",headerRow[2]);
                    m.put("direct",true);
                    break;
                case "relationship":
                    if(inGroup) {
                        if(!relationG.containsKey(headerRow[0])){
                            relationG.put(headerRow[0],new HashMap<>());
                        }
                        m = relationG.get(headerRow[0]);
                    }else{
                        if(!relation.containsKey(headerRow[0])){
                            relation.put(headerRow[0],new HashMap<>());
                        }
                        m = relation.get(headerRow[0]);
                    }
                    m.put("column",headerRow[0]);
                    m.put("restriction",headerRow[3]);
                    m.put("property",headerRow[2]);
                    break;
                case "relationshipcolumn":
                    if(inGroup) {
                        if(!relationG.containsKey(headerRow[0])){
                            relationG.put(headerRow[0],new HashMap<>());
                        }
                        m = relationG.get(headerRow[0]);
                    }else{
                        if(!relation.containsKey(headerRow[0])){
                            relation.put(headerRow[0],new HashMap<>());
                        }
                        m = relation.get(headerRow[0]);
                    }
                    m.put("column",headerRow[2]);
                    break;
                case "relationshipcollection":
                    if(inGroup) {
                        if(!relationG.containsKey(headerRow[0])){
                            relationG.put(headerRow[0],new HashMap<>());
                        }
                        m = relationG.get(headerRow[0]);
                    }else{
                        if(!relation.containsKey(headerRow[0])){
                            relation.put(headerRow[0],new HashMap<>());
                        }
                        m = relation.get(headerRow[0]);
                    }
                    m.put("collection",headerRow[2]);
                    break;
                case "mainclass":
                    if(!modules.containsKey(headerRow[0])){
                        throw  new Exception("Module must be set before setting a main class");
                    }
                    firstReplacement = baseID.indexOf('?');
                    replacementLength = headerRow[2].length();
                    id = baseID.substring(0,firstReplacement)+headerRow[2]+baseID.substring(firstReplacement+replacementLength);
                    idHolder.put(headerRow[0],id);
                    classCounter.put(headerRow[0],1);
                    addRelations(id.replace('?','0'),dataID);
                    addRelations(id.replace('?','0'),dataID,true);
                    break;
                case "entity":
                    break;
                case "subclass":
                    if(!modules.containsKey(headerRow[2])){
                        throw  new Exception("Module must be set for main class before using subclass");
                    }
                    id = idHolder.get(headerRow[2]);
                    firstReplacement = id.indexOf('?');
                    id = id.substring(0,firstReplacement)+String.format("%0"+SubClassLength+"d",classCounter.get(headerRow[2]))+id.substring(firstReplacement+SubClassLength);
                    classCounter.put(headerRow[2],classCounter.get(headerRow[2])+1);
                    modules.put(headerRow[0],modules.get(headerRow[2])); //increment the subclass count for the parent module
                    idHolder.put(headerRow[0],id); //store the id for the class
                    classCounter.put(headerRow[0],1); //create own subclass counter
                    addRelations(id.replace('?','0'),dataID);
                    addRelations(id.replace('?','0'),dataID,true);
                    break;
                case "class": //adds all rows with non-null headerRow[0] as a subclass of headerRow[2]
                    int counter = 0;
                    id = idHolder.get(headerRow[2]);
                    firstReplacement = id.indexOf('?');
                    counter = 0;
                    for( int i=0; i<id.length(); i++ ) { //get how much we need to pad
                        if( id.charAt(i) == '?' ) {
                            counter++;
                        }
                    }
                    String query = "select "+headerRow[0];
                    int ClassIDRowNum = 2;
                    int ModuleRowNum = 2;
                    if(!ParentClassRow.equals("")){
                        query+=",`"+ParentClassRow+"`"; //get the ID for the current entity from the data table for later
                        ClassIDRowNum++;
                        ModuleRowNum++;
                    }
                    if(!ClassIDRow.equals("")){
                        query+=",`"+ClassIDRow+"`"; //get the ID for the current entity from the data table for later
                        ModuleRowNum++;
                    }
                    if(!ModuleRow.equals("")){
                        query+=",`"+ModuleRow+"`"; //get the ID for the current entity from the data table for later
                    }
                    query += ",id FROM data_table WHERE "+headerRow[0]+" != \"\" GROUP BY "+headerRow[0]+";";
                    sqlDB = conn.prepareStatement(query);
                    rs1 = sqlDB.executeQuery(); //get the ID for the current entity from the data table for later
                    while(rs1.next()) {
                        String name =prependName + prependNameG + rs1.getString(headerRow[0]) + appendName + appendNameG;
                        dataID = rs1.getString("id");
                        id = idMap.get(dataID).get(name);
                        collectionMap.get("equivalence").put("lastCollection","");
                        collectionMap.get("relation").put("lastCollection","");
                        addRelations(id,dataID);
                        addRelations(id,dataID,true);
                    }
                    break;
                case "staticclass": //adds all rows with non-null headerRow[0] as a subclass of headerRow[2]
                    if(!modules.containsKey(headerRow[2])){
                        throw  new Exception("Module must be set for main class before using subclass");
                    }
                    counter = 0;
                    id = idHolder.get(headerRow[2]);
                    firstReplacement = id.indexOf('?');
                    for( int i=0; i<id.length(); i++ ) { //get how much we need to pad
                        if( id.charAt(i) == '?' ) {
                            counter++;
                        }
                    }
                    id = id.substring(0,firstReplacement)+String.format("%0"+counter+"d",classCounter.get(headerRow[2]))+id.substring(firstReplacement+SubClassLength);
                    classCounter.put(headerRow[2],classCounter.get(headerRow[2])+1);
                    addRelations(id,dataID);
                    addRelations(id,dataID,true);
                    break;
            }
            headerRow = csvReader.readNext();
        }
    }
    private void addInfo(String id,String dataID) throws Exception{
        ResultSet rs2;
        PreparedStatement sqlQ;
        Statement sqlDB2 = conn.createStatement();
        String columns = extraColumns;
        if (inGroup && !extraColumnsG.equals(""))
            if (columns.equals(""))
                columns += extraColumnsG;
            else
                columns += "," + extraColumnsG;

        if (!dataID.equals("") && info.size() >0) { //check to make sure this dataID is not empty before attempting to query it
            for (HashMap<String,String> entry : info) { // Enter all globally set info label
                String label = entry.get("label");
                String type = entry.get("type");
                rs2 = sqlDB2.executeQuery("select " + columns + " FROM data_table WHERE id = '" + dataID + "';");
                if (rs2.next()) {
                    String value = rs2.getString(entry.get("column"));
                    if (value != null && !value.equals("")) {
                        sqlQ = conn.prepareStatement("insert into entityinfo (entityid, property_label, property_type, value) values(?,?,?,?)");
                        sqlQ.setString(1, id);
                        sqlQ.setString(2, label);
                        sqlQ.setString(3, type);
                        sqlQ.setString(4, value);
                        sqlQ.executeUpdate();
                    }
                }

            }
        }
        if(inGroup) {
            if (!dataID.equals("") && infoG.size() > 0) {
                for (HashMap<String,String> entry : infoG) { // Enter all globally set info label
                    String label = entry.get("label");
                    String type = entry.get("type");
                    rs2 = sqlDB2.executeQuery("select " + columns + " FROM data_table WHERE id = '" + dataID + "';");
                    if (rs2.next()) {
                        String value = rs2.getString(entry.get("column"));
                        if (value != null && !value.equals("")) {
                            sqlQ = conn.prepareStatement("insert into entityinfo (entityid, property_label, property_type, value) values(?,?,?,?)");
                            sqlQ.setString(1, id);
                            sqlQ.setString(2, label);
                            sqlQ.setString(3, type);
                            sqlQ.setString(4, value);
                            sqlQ.executeUpdate();
                        }
                    }
                }
            }
        }
    }

    private void addRelations(String id,String dataID) throws Exception{
        addRelations(id,dataID,false);
    }

    private void addRelations(String id,String dataID,boolean equivalence) throws Exception{
        ResultSet rs2;
        PreparedStatement sqlQ;
        Statement sqlDB2 = conn.createStatement();
        Map<String, HashMap<String,Object>> hashMap;
        Map<String, HashMap<String,Object>> hashMapG;
        String tableName = "";
        HashMap<String,Object> collectionArray;
        if(equivalence) { //equivelence and relations share most of the same code, just different variables
            hashMap = this.equivalence;
            hashMapG = equivalenceG;
            tableName = "equivalence";
            collectionArray = collectionMap.get("equivalence");
        }else {
            hashMap = relation;
            hashMapG = relationG;
            tableName = "relationship";
            collectionArray = collectionMap.get("relation");
        }

        Object[] keys = hashMap.keySet().toArray();
        Arrays.sort(keys);
        for(Object key : keys) {
            String property = (String)key;
            HashMap<String,Object> map = hashMap.get(key);
            boolean match = true;
            if(map.containsKey("conditions") && !dataID.equals("")){//check if current relationship had conditions
                @SuppressWarnings("unchecked")
                HashMap<String,ArrayList<String>> conditions = (HashMap<String,ArrayList<String>>)map.get("conditions");
                for (Entry<String, ArrayList<String>> condition : conditions.entrySet()) {
                    String condKey = condition.getKey();
                    ArrayList<String> cond = condition.getValue();
                    for (String s : cond) {
                        rs2 = sqlDB2.executeQuery("select `"+condKey+"` FROM data_table WHERE id = '"+dataID+"' AND `"+condKey+"` = '"+s+"';");
                        if(!rs2.last()){
                            match=false;
                            break ;
                        }
                    }
                }
            }
            if(match) {
                String value ="";
                String collection = null;
                if (map.containsKey("static")) {
                    value = (String)map.get("id");
                }else if(map.containsKey("column") && !dataID.equals("")){
                    String insertID ="";
                    rs2 = sqlDB2.executeQuery("select `" + map.get("column") + "` FROM data_table WHERE id = '" + dataID + "';");
                    if (rs2.next()) {
                        if(!map.containsKey("direct")) { // If we did not defined that this is a direct relation ship then get id from entity table
                            ResultSet rs3 = sqlDB2.executeQuery("select distinct e.id from entity e INNER JOIN data_table d on d.`" + map.get("column") + "` = e.label WHERE d.`" + map.get("column") + "` = '" + rs2.getString(1) + "'");
                            if (rs3.next()) {
                                insertID = rs3.getString("id");
                            }
                        }else {
                            insertID = rs2.getString(1);
                        }
                    }
                    if(insertID != null && !insertID.equals("")){
                        value = insertID;
                    }
                }
                if(map.containsKey("collection")){
                    if(!map.get("collection").equals(collectionArray.get("lastCollection"))){
                        collectionArray.put("collectionNum",(int)collectionArray.get("collectionNum")+1);
                        collectionArray.put("lastCollection",(String)map.get("collection"));
                    }
                    collection = collectionArray.get("collectionNum").toString();
                }
                if(!value.equals("")){
                    sqlQ = conn.prepareStatement("insert into `"+tableName+"` (entityid, property, value, restriction,collection) values(?,?,?,?,?)");
                    sqlQ.setString(1,id);
                    sqlQ.setString(2,map.get("property").toString());
                    sqlQ.setString(3,value);
                    sqlQ.setString(4,map.get("restriction").toString());
                    if(collection == null || collection.isEmpty())
                        sqlQ.setNull(5,java.sql.Types.INTEGER);
                    else
                        sqlQ.setInt(5,Integer.parseInt(collection));
                    sqlQ.executeUpdate();
                }
            }
        }
        if(inGroup) {
            Object[] keysG = hashMapG.keySet().toArray();
            Arrays.sort(keysG);
            for(Object key : keysG) {
                HashMap<String,Object> map = hashMapG.get(key);
                boolean match = true;
                if(map.containsKey("conditions") && !dataID.equals("")){//check if current relationship had conditions
                    @SuppressWarnings("unchecked")
                    HashMap<String,ArrayList<String>> conditions = (HashMap<String,ArrayList<String>>)map.get("conditions");
                    for (Entry<String, ArrayList<String>> condition : conditions.entrySet()) {
                        String condKey = condition.getKey();
                        ArrayList<String> cond = condition.getValue();
                        for (String s : cond) {
                            rs2 = sqlDB2.executeQuery("select `"+condKey+"` FROM data_table WHERE id = '"+dataID+"' AND `"+condKey+"` = '"+s+"';");
                            if(!rs2.last()){
                                match=false;
                                break ;
                            }
                        }
                    }
                }
                if(match) {
                    String value ="";
                    String collection = null;
                    if (map.containsKey("static")) {
                        value = (String)map.get("id");
                    }else if(map.containsKey("column") && !dataID.equals("")){
                        String insertID ="";
                        rs2 = sqlDB2.executeQuery("select `" + map.get("column") + "` FROM data_table WHERE id = '" + dataID + "';");
                        if (rs2.next()) {
                            if(!map.containsKey("direct")) { // If we did not defined that this is a direct relation ship then get id from entity table
                                ResultSet rs3 = sqlDB2.executeQuery("select distinct e.id from entity e INNER JOIN data_table d on d.`" + map.get("column") + "` = e.label WHERE d.`" + map.get("column") + "` = '" + rs2.getString(1) + "'");
                                if (rs3.next()) {
                                    insertID = rs3.getString("id");
                                }
                            }else {
                                insertID = rs2.getString(1);
                            }
                        }
                        if(insertID != null && !insertID.equals("")){
                            value = insertID;
                        }
                    }
                    if(map.containsKey("collection")){
                        if(!map.get("collection").equals(collectionArray.get("lastCollection"))){
                            collectionArray.put("collectionNum",(int)collectionArray.get("collectionNum")+1);
                            collectionArray.put("lastCollection",(String)map.get("collection"));
                        }
                        collection = collectionArray.get("collectionNum").toString();
                    }
                    if(!value.equals("")){
                        sqlQ = conn.prepareStatement("insert into `"+tableName+"` (entityid, property, value, restriction,collection) values(?,?,?,?,?)");
                        sqlQ.setString(1,id);
                        sqlQ.setString(2,map.get("property").toString());
                        sqlQ.setString(3,value);
                        sqlQ.setString(4,map.get("restriction").toString());
                        if(collection == null || collection.isEmpty())
                            sqlQ.setNull(5,java.sql.Types.INTEGER);
                        else
                            sqlQ.setInt(5,Integer.parseInt(collection));
                        sqlQ.executeUpdate();
                    }
                }
            }
        }
    }


    private void processReparent() throws Exception {
        CSVReader csvReader = null;
        char seprator =',';

        //try and read the reparent file file
        try {
            csvReader = new CSVReader(new FileReader(reParent), seprator);
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
        Statement sqlGetId1 = conn.createStatement();
        Statement sqlGetId2 = conn.createStatement();
        Statement sqlReParent = conn.createStatement();

        //loop through reparent file
        while(null != headerRow) {
            ResultSet rs1 = sqlGetId1.executeQuery("select `id`,`module` FROM entity WHERE label = '" + headerRow[1] + "';"); //get the child
            ResultSet rs2 = sqlGetId2.executeQuery("select `id` FROM entity WHERE label = '" + headerRow[0] + "';"); //get the new parent
            if(rs1.next()){
                if(!rs2.next()) {
                    outputString(headerRow[0] +" entity does not exist, skipping");
                }else{
                    sqlReParent.execute("UPDATE entityinfo SET value = '" + rs2.getString(1) + "' WHERE entityid = '" + rs1.getString(1) + "' AND property_label = 'subClassOf';"); //reparent child
                }
            }
            headerRow = csvReader.readNext();
        }
    }

    private void removeRedundant() throws Exception {

    }

    private void outputRelation(BufferedWriter buffer, ResultSet relations,ArrayList<String> ignore, ResultSet collectionResource) throws Exception{
        String Resource ="";
        String propResource ="";
        String ExtraTab ="";
        if(relations.getString("property") == null)
            return;
        if (relations.getString("collection") != null && collectionResource != null) {
            buffer.write("\t\t\t<owl:Class>\n");
            buffer.write("\t\t\t\t<owl:intersectionOf rdf:parseType=\"Collection\">\n");
            while(collectionResource.next()){
                if (collectionResource.getString("value").contains("DTO")) {
                    Resource = ontologyURL+settings.getOrDefault("idSeperator","/");
                } else if (collectionResource.getString("value").toLowerCase().startsWith("http")) {
                    Resource = "";
                }else{
                    Resource = "http://purl.obolibrary.org/obo/";
                }

                if (collectionResource.getString("property").contains("DTO")) {
                    propResource = ontologyURL+settings.getOrDefault("idSeperator","/");
                } else if (collectionResource.getString("property").toLowerCase().startsWith("http")) {
                    propResource = "";
                }else{
                    propResource = "http://purl.obolibrary.org/obo/";
                }

                ignore.add(collectionResource.getString("id"));
                if (collectionResource.getString("restriction").equals("none")){
                    buffer.write("\t\t\t\t\t<rdf:Description rdf:about=\"" + Resource + relations.getString("value") + "\"/>\n");
                }else {
                    //if (collectionResource.getString("restriction").equals("not")) {
                    //    buffer.write("\t\t\t\t\t<owl:complementOf>\n");
                    //    ExtraTab = "\t";
                    //}
                    buffer.write("\t\t\t\t\t" + ExtraTab + "<owl:Restriction>\n");
                    buffer.write("\t\t\t\t\t\t" + ExtraTab + "<owl:onProperty rdf:resource=\"" + propResource + collectionResource.getString("property") + "\"/>\n");
                    if (collectionResource.getString("restriction").equals("some")) {
                        buffer.write("\t\t\t\t\t\t" + ExtraTab + "<owl:someValuesFrom rdf:resource=\"" + Resource + collectionResource.getString("value") + "\"/>\n");
                    } else if (collectionResource.getString("restriction").equals("only")) {
                        buffer.write("\t\t\t\t\t\t" + ExtraTab + "<owl:allValuesFrom rdf:resource=\"" + Resource + collectionResource.getString("value") + "\"/>\n");
                    } else if (collectionResource.getString("restriction").equals("value")) {
                        buffer.write("\t\t\t\t\t\t" + ExtraTab + "<owl:hasValue rdf:datatype=\"http://www.w3.org/2001/XMLSchema#integer\">" + StringEscapeUtils.escapeXml11(collectionResource.getString("value")) + "</owl:hasValue>\n");
                    } else if (collectionResource.getString("restriction").equals("not")) {
                        buffer.write("\t\t\t\t\t\t" + ExtraTab + "<owl:someValuesFrom rdf:resource=\"" + Resource + collectionResource.getString("value") + "\"/>\n");
                    } else {
                        outputString("Unknown Restriction `" + collectionResource.getString("restriction") + "`");
                    }
                    buffer.write("\t\t\t\t\t" + ExtraTab + "</owl:Restriction>\n");
                    //if (collectionResource.getString("restriction").equals("not")) {
                    //    buffer.write("\t\t\t\t\t</owl:complementOf>\n");
                    //}
                }
                ExtraTab ="";
            }
            buffer.write("\t\t\t\t</owl:intersectionOf>\n");
            buffer.write("\t\t\t</owl:Class>\n");
        }else {
            if (relations.getString("value").toLowerCase().contains(settings.getOrDefault("ontologyShortName","DTO").toLowerCase())) {
                Resource = ontologyURL+settings.getOrDefault("idSeperator","/");
            } else if (relations.getString("value").toLowerCase().toLowerCase().startsWith("http")) {
                Resource = "";
            }else{
                Resource = "http://purl.obolibrary.org/obo/";
            }

            if (relations.getString("property").toLowerCase().contains(settings.getOrDefault("ontologyShortName","DTO").toLowerCase())) {
                propResource = ontologyURL+settings.getOrDefault("idSeperator","/");
            } else if (relations.getString("property").toLowerCase().toLowerCase().startsWith("http")) {
                propResource = "";
            }else{
                propResource = "http://purl.obolibrary.org/obo/";
            }


            if (relations.getString("restriction").equals("none")){
                buffer.write("\t\t\t<rdf:Description rdf:about=\"" + Resource + relations.getString("value") + "\"/>\n");
            }else {
                if (relations.getString("restriction").startsWith("not")) {
                    buffer.write("\t\t\t<owl:Class>\n");
                    buffer.write("\t\t\t\t<owl:complementOf>\n");
                    ExtraTab = "\t\t";
                }
                buffer.write("\t\t\t" + ExtraTab + "<owl:Restriction>\n");
                buffer.write("\t\t\t\t" + ExtraTab + "<owl:onProperty rdf:resource=\"" + propResource + relations.getString("property") + "\"/>\n");
                if (relations.getString("restriction").endsWith("some")) {
                    buffer.write("\t\t\t\t" + ExtraTab + "<owl:someValuesFrom rdf:resource=\"" + Resource + relations.getString("value") + "\"/>\n");
                } else if (relations.getString("restriction").endsWith("only")) {
                    buffer.write("\t\t\t\t" + ExtraTab + "<owl:allValuesFrom rdf:resource=\"" + Resource + relations.getString("value") + "\"/>\n");
                } else if (relations.getString("restriction").endsWith("value")) {
                    buffer.write("\t\t\t\t" + ExtraTab + "<owl:hasValue rdf:datatype=\"http://www.w3.org/2001/XMLSchema#integer\">" + StringEscapeUtils.escapeXml11(relations.getString("value")) + "</owl:hasValue>\n");
                } else {
                    outputString("Unknown Restriction `" + relations.getString("restriction") + "`");
                }
                buffer.write("\t\t\t" + ExtraTab + "</owl:Restriction>\n");
                if (relations.getString("restriction").startsWith("not")) {
                    buffer.write("\t\t\t\t</owl:complementOf>\n");
                    buffer.write("\t\t\t</owl:Class>\n");
                }
            }
            ExtraTab ="";
        }
    }

    private void generateRelations()  throws Exception {
        String ontologyShortName = settings.getOrDefault("ontologyShortName","")+"_";

        String moduleName = RelationFile;

        try {
            Statement stmt1 = conn.createStatement();
            Statement stmt2 = conn.createStatement();
            Statement stmt3 = conn.createStatement();

            try {
                ResultSet rs1 = stmt1.executeQuery("select distinct e.id,e.module  from entity e order by id");

                ResultSet rs2;

                ResultSet rs3 = null;
                rs2 = stmt2.executeQuery("select count(*) FROM equivalence");
                rs2.next();
                int equivCount = rs2.getInt(1);
                int equivComplete = 0;
                float equivLastOutput = 9;
                outputString(equivCount + " total equivalence axioms !");

                rs2 = stmt2.executeQuery("select count(*) FROM relationship");
                rs2.next();
                int relationCount = rs2.getInt(1);
                int relationComplete = 0;
                float relationLastOutput = 9;
                outputString(relationCount + " total relationship axioms !");

                File file;
                if(RelationFile == null ||RelationFile.equals("") ) {
                    file = new File(destDir + "/"+ontologyShortName+"automated_axioms.owl");
                }else{
                    file = new File(destDir + "/"+ontologyShortName+RelationFile+".owl");
                }
                if (!file.exists()) {file.createNewFile();}
                FileWriter fw = new FileWriter(file.getAbsoluteFile());
                BufferedWriter bw = new BufferedWriter(fw);
                bw.write(generateOwlHeader(moduleName));
                bw.write("\n");


                while(rs1.next()){
                    String module = rs1.getString("module");
                    if(!moduleBlacklist.isEmpty() && moduleBlacklist.contains(module)){
                        continue;
                    }

                    if(!moduleWhitelist.isEmpty() && !moduleWhitelist.contains(module)){
                        continue;
                    }
                    boolean hasStarted = false;
                    rs2 = stmt2.executeQuery("select * FROM equivalence r where r.entityid=\"" + rs1.getString("id") + "\" order by id,r.property");

                    ArrayList<String> ignore = new ArrayList<>();

                    while(rs2.next()) {
                        if (ignore.contains(rs2.getString("id")))
                            continue;
                        if (rs2.getString("collection") != null)
                            rs3 = stmt3.executeQuery("select * FROM equivalence r where r.collection=\"" + rs2.getString("collection") + "\" order by id,r.property");
                        if(!hasStarted){
                            bw.write("\t<owl:Class rdf:about=\""+ontologyURL+settings.getOrDefault("idSeperator","/")+  rs1.getString("id") + "\">\n");
                            hasStarted = true;
                        }
                        bw.write("\t\t<owl:equivalentClass>\n");
                        outputRelation(bw,rs2,ignore,rs3);
                        equivComplete++;
                        bw.write("\t\t</owl:equivalentClass>\n");
                    }
                    float percentDoneEquiv = 0;
                    if(equivCount>0){
                        percentDoneEquiv =(float)equivComplete/equivCount;
                    }

                    rs2 = stmt2.executeQuery("select * FROM relationship r where r.entityid=\"" + rs1.getString("id") + "\" order by id,r.property");
                    rs3 = null;
                    ignore.clear();
                    while(rs2.next()) {
                        if (ignore.contains(rs2.getString("id")))
                            continue;
                        if (rs2.getString("collection") != null)
                            rs3 = stmt3.executeQuery("select * FROM relationship r where r.collection=\"" + rs2.getString("collection") + "\" order by id,r.property");
                        if(!hasStarted){
                            bw.write("\t<owl:Class rdf:about=\""+ontologyURL+settings.getOrDefault("idSeperator","/") + rs1.getString("id") + "\">\n");
                            hasStarted = true;
                        }
                        bw.write("\t\t<rdfs:subClassOf>\n");
                        outputRelation(bw,rs2,ignore,rs3);
                        bw.write("\t\t</rdfs:subClassOf>\n");
                        relationComplete++;
                    }
                    DecimalFormat df = new DecimalFormat("#.#");
                    df.setRoundingMode(RoundingMode.CEILING);
                    float percentDoneRelationship = 0;
                    if(relationCount>0){
                        percentDoneRelationship =(float)relationComplete/relationCount;
                    }
                    if(Float.parseFloat(df.format(percentDoneEquiv)) > equivLastOutput ||Float.parseFloat(df.format(percentDoneRelationship)) > relationLastOutput) {
                        outputString((percentDoneRelationship*100)+"% completed relationship " +(percentDoneEquiv*100)+"% completed equivelence ");
                        equivLastOutput = Float.parseFloat(df.format(percentDoneEquiv));
                        relationLastOutput = Float.parseFloat(df.format(percentDoneRelationship));
                    }
                    if(hasStarted)
                        bw.write("\t</owl:Class>\n\n");
                }



                rs1 = stmt1.executeQuery("select distinct e.entityid as id from relationship e where not exists( select id from entity where id = e.entityid) order by id,e.property");

                while(rs1.next()){

                    boolean hasStarted = false;
                    rs2 = stmt2.executeQuery("select * FROM equivalence r where r.entityid=\"" + rs1.getString("id") + "\" order by id,r.property");

                    ArrayList<String> ignore = new ArrayList<>();

                    while(rs2.next()) {
                        if (ignore.contains(rs2.getString("id")))
                            continue;
                        if (rs2.getString("collection") != null)
                            rs3 = stmt3.executeQuery("select * FROM equivalence r where r.collection=\"" + rs2.getString("collection") + "\" order by id,r.property");
                        if(!hasStarted){
                            bw.write("\t<owl:Class rdf:about=\""+ontologyURL+settings.getOrDefault("idSeperator","/")+  rs1.getString("id") + "\">\n");
                            hasStarted = true;
                        }
                        bw.write("\t\t<owl:equivalentClass>\n");
                        outputRelation(bw,rs2,ignore,rs3);
                        equivComplete++;
                        bw.write("\t\t</owl:equivalentClass>\n");
                    }

                    rs2 = stmt2.executeQuery("select * FROM relationship r where r.entityid=\"" + rs1.getString("id") + "\" order by id,r.property");
                    rs3 = null;
                    ignore.clear();
                    while(rs2.next()) {
                        if (ignore.contains(rs2.getString("id")))
                            continue;
                        if (rs2.getString("collection") != null)
                            rs3 = stmt3.executeQuery("select * FROM relationship r where r.collection=\"" + rs2.getString("collection") + "\" order by id,r.property");
                        if(!hasStarted){
                            bw.write("\t<owl:Class rdf:about=\""+ontologyURL+settings.getOrDefault("idSeperator","/") + rs1.getString("id") + "\">\n");
                            hasStarted = true;
                        }
                        bw.write("\t\t<rdfs:subClassOf>\n");
                        outputRelation(bw,rs2,ignore,rs3);
                        bw.write("\t\t</rdfs:subClassOf>\n");
                        relationComplete++;
                    }
                    if(hasStarted)
                        bw.write("\t</owl:Class>\n\n");
                }

                bw.write("</rdf:RDF>");
                bw.close();
                outputString("Module \"" + file.getName() + "\" was generated successfully!");
            }
            catch (IOException e) {
                e.printStackTrace();}
        }
        catch (SQLException e) {
            throw new IllegalStateException("Cannot connect the database!", e);}
        outputString("DONE!" );

    }

    private void generateExternalModules() throws Exception{
        if(Thread.currentThread().isInterrupted()){
            return;
        }
        String ontologyShortName = settings.getOrDefault("ontologyShortName","");
        try {
            Statement stmt = conn.createStatement();
            Statement stmt2 = conn.createStatement();
            try {

                ResultSet rs = stmt.executeQuery("select distinct module from external where module is not null order by id");
                while (rs.next()) {
                    if(Thread.currentThread().isInterrupted()){
                        return;
                    }
                    String moduleName = rs.getString("module");
                    if(!moduleBlacklist.isEmpty() && moduleBlacklist.contains(moduleName)){
                        outputString("Module \"" + rs.getString("module") + "\" is on blacklist!");
                        continue;
                    }

                    if(!moduleWhitelist.isEmpty() && !moduleWhitelist.contains(moduleName)){
                        outputString("Module \"" + rs.getString("module") + "\" is not on whitelist!");
                        continue;
                    }

                    File file = new File(destDir + "/" +ontologyShortName+"_"+rs.getString("module") + "_import.txt");
                    if (!file.exists()) {
                        file.createNewFile();
                    }


                    FileWriter fw = new FileWriter(file.getAbsoluteFile());
                    BufferedWriter bw = new BufferedWriter(fw);

                    //write the header


                    bw.write("[URI of the OWL(RDF/XML) output file]\n");
                    bw.write(ontologyURL+"/external/"+ontologyShortName.toUpperCase()+"_"+moduleName+"_import.owl\n");
                    bw.write("\n");
                    bw.write("[Source ontology]\n");
                    bw.write(moduleName+"\n");
                    bw.write("\n");
                    bw.write("[Low level source term URIs]\n");

                    //get external sources
                    ResultSet rs2 = stmt2.executeQuery("SELECT id from external WHERE module ='" + rs.getString("module") + "' AND (is_top_level IS NULL or is_top_level = 0)");
                    while (rs2.next()) {
                        String annotation = rs2.getString(1);
                        if (annotation != null) {
                            bw.write(annotation + "\n");
                        }
                    }
                    bw.write("\r\n\r\n\r\n");


                    bw.write("[Top level source term URIs and target direct superclass URIs]\n");
                    rs2 = stmt2.executeQuery("SELECT id,super_class from external WHERE module ='" + rs.getString("module") + "' AND (super_class IS NOT NULL OR (is_top_level IS NOT NULL AND is_top_level != 0))");
                    while (rs2.next()) {
                        if(Thread.currentThread().isInterrupted()){
                            return;
                        }
                        String annotation = rs2.getString(1);
                        String super_class = rs2.getString(2);
                        if (annotation != null) {
                            bw.write(annotation + "\n");
                            if (super_class != null && !super_class.equals("0")) {
                                bw.write("subClassOf "+super_class + "\n");
                            }
                        }
                    }
                    bw.write("\r\n\r\n\r\n");


                    bw.write(generateOwlHeader("external_"+moduleName));
                    bw.write("\n");
                    bw.close();
                    outputString("Module \"external_" + rs.getString("module") + "\" was generated successfully!");

                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            System.out.println(e);
            throw new IllegalStateException("Cannot connect the database!", e);
        }
        outputString("DONE!");
    }

    /**
     * Generate Modules
     *
     * Description: Generates the individual vocabulary files that makes a particular ontology based on information in the database.
     * @throws Exception
     */
    private void generateModules() throws Exception  {
        if(Thread.currentThread().isInterrupted()){
            return;
        }
        String ontologyShortName = settings.getOrDefault("ontologyShortName","");

        try {
            Statement stmt = conn.createStatement();
            Statement stmt2 = conn.createStatement();
            Statement stmt3 = conn.createStatement();
            try {

                // Get all current modules in the database
                ResultSet rs = stmt.executeQuery("select distinct module from entity where module is not null order by id");
                while (rs.next()) {
                    if(Thread.currentThread().isInterrupted()){
                        return;
                    }
                    //get the curent module if its in the blacklist ignore, or if whitelist is set and not in whitelist
                    String moduleName = rs.getString("module");
                    if(!moduleBlacklist.isEmpty() && moduleBlacklist.contains(moduleName)){
                        outputString("Module \"" + rs.getString("module") + "\" is on blacklist!");
                        continue;
                    }

                    if(!moduleWhitelist.isEmpty() && !moduleWhitelist.contains(moduleName)){
                        outputString("Module \"" + rs.getString("module") + "\" is not on whitelist!");
                        continue;
                    }

                    // create the module file
                    File file = new File(destDir + "/" +ontologyShortName+"_"+rs.getString("module") + ".owl");
                    if (!file.exists()) {
                        file.createNewFile();
                    }


                    FileWriter fw = new FileWriter(file.getAbsoluteFile());
                    BufferedWriter bw = new BufferedWriter(fw);


                    //write the header based on the template file if it exsists
                    bw.write(generateOwlHeader(moduleName));
                    bw.write("\n");


                    boolean wroteAnnotations =false;
                    boolean wroteClasses = false;



                    //get annotations not from external sources
                    ResultSet rs2 = stmt2.executeQuery("SELECT distinct entityinfo.property_label from entity INNER JOIN entityinfo ON entityinfo.entityid= entity.id WHERE module ='" + rs.getString("module") + "' AND entityinfo.property_type='annotation_property' order by entity.id,entityinfo.property_label");
                    while (rs2.next()) {
                        if(Thread.currentThread().isInterrupted()){
                            return;
                        }
                        String annotation = rs2.getString(1);
                        if(!annotation.contains(":")) {
                            //if this is the first annotation then put the annotation header
                            if(!wroteAnnotations){
                                bw.write("    <!--\n");
                                bw.write("    ///////////////////////////////////////////////////////////////////////////////////////\n");
                                bw.write("    //\n");
                                bw.write("    // Annotation properties\n");
                                bw.write("    //\n");
                                bw.write("    ///////////////////////////////////////////////////////////////////////////////////////\n");
                                bw.write("     -->\n\n");
                                wroteAnnotations = true;
                            }
                            bw.write("\t<!-- " + ontologyURL + "/" + annotation + " -->\n");
                            bw.write("\n");
                            bw.write("\t<owl:AnnotationProperty rdf:about=\"" + ontologyURL + "/" + annotation + "\"/>\r\n");
                        }
                    }

                    //if we wrote annotations add in file padding
                    if(wroteAnnotations){
                        bw.write("\r\n\r\n\r\n");
                    }



                    //write classes
                    rs2 = stmt2.executeQuery("SELECT id,label FROM entity WHERE module ='" + rs.getString("module") + "' order by id");
                    while (rs2.next()) {
                        if(Thread.currentThread().isInterrupted()){
                            return;
                        }
                        //if the class doesn't start with the ontology address igore it
                        if(!rs2.getString("id").toLowerCase().startsWith(settings.getOrDefault("ontologyShortName","DTO").toLowerCase())) {
                            continue;
                        }

                        //if we have a class and have not yet written the class header, write the class header
                        if(!wroteClasses) {
                            bw.write("    <!--\n");
                            bw.write("    ///////////////////////////////////////////////////////////////////////////////////////\n");
                            bw.write("    //\n");
                            bw.write("    // Classes\n");
                            bw.write("    //\n");
                            bw.write("    ///////////////////////////////////////////////////////////////////////////////////////\n");
                            bw.write("     -->\n\n");
                            wroteClasses=true;
                        }


                        if(!rs.getString("module").equals("entities")) {
                            bw.write("\t<!-- "+ontologyURL+settings.getOrDefault("idSeperator","/")+rs2.getString("id")+" -->\n");
                            bw.write("\n");
                            bw.write("\t<owl:Class rdf:about=\""+ontologyURL+settings.getOrDefault("idSeperator","/")+ rs2.getString("id") + "\">");
                        }else{
                            bw.write("\t<owl:ObjectProperty rdf:about=\""+ontologyURL+settings.getOrDefault("idSeperator","/")+ rs2.getString("id") + "\">");
                        }
                        bw.write("\n");


                        //write its label
                        bw.write("\t\t<rdfs:label rdf:datatype=\"&xsd;string\">" + StringEscapeUtils.escapeXml11(rs2.getString("label")) + "</rdfs:label>");
                        bw.write("\n");


                        //write all its annotation properties
                        ResultSet rs3 = stmt3.executeQuery("SELECT * FROM entityinfo where entityid ='" + rs2.getString("id") + "' order by entityid");
                        while (rs3.next()) {
                            if (rs3.getString("property_type").contains("annotation_property")) {
                                String label = rs3.getString("property_label");
                                String value = rs3.getString("value");
                                if(!label.contains(":") && !ontologyShortName.isEmpty()){
                                    label=ontologyShortName+":"+label;
                                }
                                bw.write("\t\t<" + label + ">" + StringEscapeUtils.escapeXml11(value) + "</" + label + ">");
                                bw.write("\n");
                            }
                            if (rs3.getString("property_type").contains("rdf_property")) {
                                String value = rs3.getString("value");
                                if(value.contains("http")){
                                    bw.write("\t\t<rdfs:subClassOf rdf:resource=\"" + StringEscapeUtils.escapeXml11(value) + "\"/>");
                                }else{
                                    bw.write("\t\t<rdfs:subClassOf rdf:resource=\""+ontologyURL+settings.getOrDefault("idSeperator","/")+ StringEscapeUtils.escapeXml11(value) + "\"/>");
                                }
                                bw.write("\n");
                            }
                        }
                        if(!rs.getString("module").equals("entities")) {
                            bw.write("\t</owl:Class>\n\n");
                        }else{
                            bw.write("\t</owl:ObjectProperty>\n\n");
                        }
                    }
                    bw.write("</rdf:RDF>");
                    bw.close();
                    outputString("Module \"" + rs.getString("module") + "\" was generated successfully!");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot connect the database!", e);
        }
        outputString("DONE!");
    }

    /**
     *
     * @param ModuleName: Name of the module we are currently getting the template header for
     * @return  template: Template string containing the nessicary headers
     * @throws Exception
     */
    private String generateOwlHeader(String ModuleName) throws Exception{
        String ontologyShortName = settings.getOrDefault("ontologyShortName","generic");
        String header = "";

        //list of accepted encodings
        List<Charset> encodings = new ArrayList<>();
        encodings.add(StandardCharsets.UTF_8);
        encodings.add(StandardCharsets.US_ASCII);
        encodings.add(StandardCharsets.ISO_8859_1);

        //if any templates exists and one matches your module name then use that
        if(Files.exists(Paths.get("templates"))) {
            if (Files.exists(Paths.get("templates/" + ModuleName + ".owl"))) {
                List<String> Lines;
                for(Charset c: encodings){
                    try {
                        Lines = Files.readAllLines(Paths.get("templates/" + ModuleName + ".owl"), c);
                        for (String s : Lines)
                        {
                            header += s + "\r\n";
                        }
                        break;
                    }catch(Exception e){
                        continue;
                    }
                }
            }

            //if a custom template does not exists then use the generic if it exists
            else if(Files.exists(Paths.get("templates/generic.owl"))){
                boolean running = true;
                List<String> Lines;
                for(Charset c: encodings){
                    try {
                        Lines = Files.readAllLines(Paths.get("templates/generic.owl"), c);
                        for (String s : Lines)
                        {
                            header += s + "\r\n";
                        }
                        break;
                    }catch(Exception e){
                        continue;
                    }
                }
            }
        }

        //remove ending tag if it exists
        header = header.replace("</rdf:RDF>","");

        //if a template did not exists attempt to build a custom template from the entity, rdf, and ontology header files if present
        if(header.isEmpty()){
            header += "<?xml version=\"1.0\"?>\r\n";
            Path entityHeader = Paths.get("entityHeader.txt");
            if(Files.exists(entityHeader)){
                byte[] encoded = Files.readAllBytes(entityHeader);
                header+= new String(encoded, Charset.defaultCharset());
            }

            header += "\n<rdf:RDF xmlns=\""+ontologyURL+"/" +ontologyShortName+"_"+ ModuleName + "#\"\n"
                    +"\txml:base=\""+ontologyURL+"/" +ontologyShortName+"_"+ ModuleName + "\"\n";

            Path rdfHeader = Paths.get("rdfHeader.txt");
            if(Files.exists(rdfHeader)) {
                byte[] encoded = Files.readAllBytes(rdfHeader);
                header += new String(encoded, Charset.defaultCharset());
            }
            header +=">\n";


            Path ontologyHeader = Paths.get("ontologyHeader.txt");
            if(Files.exists(ontologyHeader)){
                byte[] encoded = Files.readAllBytes(ontologyHeader);
                header+= new String(encoded, Charset.defaultCharset());
            }
        }

        //replace ontology template variables
        header = header.replace("$ontologyFile$",ontologyURL+"/" +ontologyShortName+"_"+ ModuleName + ".owl");
        String dateFormat = settings.getOrDefault("dateFormat","d MMM yyyy");
        SimpleDateFormat date = new SimpleDateFormat(dateFormat);
        header = header.replace("$date$",date.format(new Date()));


        return header;
    }

}
