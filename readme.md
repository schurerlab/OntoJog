# OntoJog

OntoJog is a java based program to automatically generate owl files from delimited text files, database tables, or directly from Ontofox.

After compiling, the main use of Ontojog is to generate or QC an ontology using OntoJog’s swing based GUI . It’s designed to allow for rapid development and iteration of ontologies via the use of a pseudo triples store in a mysql relational database keeping.  The database tracks classes  terms, definitions, external annotations and cross references, and simple axioms as well and assigns and tracks IDs to prevent conflicts. OntoJob also supports the generation to ontology modules. Edits to the database can be made quickly and by multiple users. Keeping the data in a single structured relational database facilitates systematic development, building, and QC of ontology modules.

Ontojog has been used to develop Drug Target Ontology and us also used to maintain both Drug Target Ontology and BioAssay Ontology.

# Required Libraries

commons-lang3-3.5
mysql-connector-java-5.1.13-bin
owlapi-distribution-4.1.3

# Settings

Currently the GUI and commandline versions take a settings file to determine where the ontology's data resides and how to generate the ontology.

Currently valid values are

    username,user

    password,pass

    ontologyURL,httpwww.testontology.orgtest

    ontologyShortName,test

    dateFormat,d MMM yyyy

    url,jdbcmysql127.0.0.13306test

    idSeperator,#

    vocabularyURL,

dateFormat is based on the Java SimpleDateFormat, more information can be found at httpsdocs.oracle.comjavase7docsapijavatextSimpleDateFormat.html

If there is a separate directory at the ontology url (for vocabulary files vs axioms files in case of modular construction), vocabularyURL can be set to that directory for QC purposes.

Everything below this is for future extension

    labelColumn,term

    parentIDColumn,parent

    descriptionColumn,definition

    newParentColumn,newSuper

    parentLabelColumn,newParent

    externalColumn,isExternal

# Template Files

OntoJog also supports template files for ontology generation. Template files located in the templates directory, are used to generate the headers of the ontology vocabulary files. If there is a .owl template file (in the templates directory) matching an ontology vocabulary or module file that template will be used. Otherwise it will be attempted to use generic.owl as template. If no template directory is found or no matching template file name is found it will be attempted to load entities from entityHeader.txt in the templates directory. If that fails, a generic owl header consisting of module name and information in the rdfHeader.txt file will be used if that file exists and if not, the ontologyHeader.txt file will be used. If none of these files exists a generic owl header will be placed on the top of all files. Example files are included.

# Using Ontojog

Ontojog can be used via its GUI or strictly commandline

If `-nogui` is not passed via commandline then OntoJog will start by default in GUI mode. The user will be presented with a simplified GUI allowing to generate or QC an ontology using the settings file. One can simply setup the settings file and provide it along with an output directory and hit generate. Ontojog will then connect to the specified database and attempt to generate the ontology. After generation one can then QC to get the change list as well as as the QCreport.txt file.

If `-nogui` is passed   then `-settings=file` must be passed to denote where the settings file is. Ontojog will generate all files and then run QC unless `-noQC` is passed and export results to QCreport.txt. This report will contain any duplicate IDs found as well as all terms added to or removed from the last released version (based on config) and the current output file.

# Axioms

Not all axioms are supported by OntoJog or its format. Simple axioms are supported. Highly nested axioms can cause problems. However, simple nested axioms (one level) and Not axioms are supported. All axioms when generated will be written in the file automated_axioms.owl.