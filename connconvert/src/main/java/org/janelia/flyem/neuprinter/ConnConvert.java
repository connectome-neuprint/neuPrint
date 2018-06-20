package org.janelia.flyem.neuprinter;

import java.io.BufferedReader;
import java.io.FileReader;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.io.File;
import java.util.logging.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.janelia.flyem.neuprinter.db.DbConfig;
import org.janelia.flyem.neuprinter.json.JsonUtils;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.janelia.flyem.neuprinter.model.SortBodyByNumberOfSynapses;


public class ConnConvert {


    @Parameters(separators = "=")
    public static class ConverterParameters {

        @Parameter(
                names = "--dbProperties",
                description = "Properties file containing database information (required)",
                required = true)
        public String dbProperties;

        @Parameter(
                names = "--loadNeurons",
                description = "Indicates that data from neurons json should be loaded to database (omit to skip)",
                required = false,
                arity = 0)
        public boolean loadNeurons;

        @Parameter(
                names = "--loadSynapses",
                description = "Indicates that data from synapses json should be loaded to database (omit to skip)",
                required = false,
                arity = 0)
        public boolean loadSynapses;

        @Parameter(
                names = "--doAll",
                description = "Indicates that both Neurons and Synapses jsons should be loaded and all database features added",
                required = false,
                arity = 0)
        public boolean doAll;

        @Parameter(
                names = "--prepDatabase",
                description = "Indicates that database constraints and indexes should be setup (omit to skip)",
                required = false,
                arity = 0)
        public boolean prepDatabase;

        @Parameter(
                names = "--addConnectsTo",
                description = "Indicates that ConnectsTo relations should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addConnectsTo;

        @Parameter(
                names = "--addSynapses",
                description = "Indicates that synapse nodes should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addSynapses;

        @Parameter(
                names = "--addSynapsesTo",
                description = "Indicates that SynapsesTo relations should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addSynapsesTo;

        @Parameter(
                names = "--addNeuronRois",
                description = "Indicates that neuron ROI labels should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addNeuronRois;


        @Parameter(
                names = "--addSynapseSets",
                description = "Indicates that synapse set nodes should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addSynapseSets;

        @Parameter(
                names = "--addNeuronParts",
                description = "Indicates that neuron parts nodes should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addNeuronParts;

        @Parameter(
                names = "--addSkeletons",
                description = "Indicates that skeleton nodes should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addSkeletons;

        @Parameter(
                names = "--neuronJson",
                description = "JSON file containing neuron data to import",
                required = false)
        public String neuronJson;

        @Parameter(
                names = "--datasetLabel",
                description = "Dataset value for all nodes (required)",
                required = true)
        public String datasetLabel;

        @Parameter(
                names = "--bigThreshold",
                description = "Total number of synapses for a body must be greater than this value for the body to be considered \"Big\" and be given an sId (must be an integer; default is 10)",
                required = false)
        public Integer bigThreshold;

        @Parameter(
                names = "--synapseJson",
                description = "JSON file containing body synapse data to import",
                required = false)
        public String synapseJson;

        @Parameter(
                names = "--skeletonDirectory",
                description = "Path to directory containing skeleton files for this dataset",
                required = false)
        public String skeletonDirectory;

        @Parameter(
                names = "--createLog",
                description = "Indicates that log file should be created (omit to skip)",
                required = false,
                arity = 0)
        public boolean createLog;

        @Parameter(
                names = "--editMode",
                description = "Indicates that neuprinter is being used in edit mode to alter data in an existing database (omit to skip).",
                required = false,
                arity = 0)
        public boolean editMode;

        @Parameter(
                names = "--help",
                help = true)
        public boolean help;

        public DbConfig getDbConfig() {
            return (dbProperties == null) ? null : DbConfig.fromFile(new File(dbProperties));
        }

        @Override
        public String toString() {
            return JsonUtils.GSON.toJson(this);
        }
    }


    private static List<Neuron> neuronList;
    private static List<BodyWithSynapses> bodyList;
    private static String dataset;


    private static List<Neuron> readNeuronsJson(String filepath) {

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            neuronList = Neuron.fromJson(reader);
            LOG.info("Number of neurons: " + neuronList.size());
        } catch (Exception e) {
            e.printStackTrace();
        }


//        try (JsonReader reader = new JsonReader(new FileReader(filepath)) ) {
//            neuronList = new ArrayList<>();
//            reader.beginArray();
//            int i = 0;
//            while (reader.hasNext() && i < 50000) {
//
//                Neuron neuron = Neuron.fromJsonSingleObject(reader);
//                //System.out.println("added neuron " + neuron.getId());
//
//                neuronList.add(neuron);
//                i++;
//            }
//
//
//        }
//
        return neuronList;

    }

    private static List<BodyWithSynapses> readSynapsesJson(String filepath) {

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            bodyList = BodyWithSynapses.fromJson(reader);
            LOG.info("Number of bodies with synapses: " + bodyList.size());
        } catch (Exception e) {
            e.printStackTrace();
        }


//        try (JsonReader reader = new JsonReader(new FileReader(filepath)) ) {
//            Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
//            bodyList = new ArrayList<>();
//            reader.beginArray();
//            int i = 0;
//            //Stopwatch timer_bws = Stopwatch.createStarted();
//            while (reader.hasNext() && i < 2400000) {
//
//                BodyWithSynapses bws = gson.fromJson(reader, BodyWithSynapses.class);
//                //System.out.println("added neuron " + neuron.getId());
//
//                bodyList.add(bws);
//                i++;
//                //System.out.println(timer_bws);
//
//            }
//
//
//        }
        return bodyList;
    }

    private static void setDatasetName(String neuronFilePath, String synapseFilePath) {
        String patternNeurons = ".*/(.*?)_Neurons.*";
        Pattern rN = Pattern.compile(patternNeurons);
        Matcher mN = rN.matcher(neuronFilePath);
        String patternSynapses = ".*/(.*?)_Synapses.*";
        Pattern rS = Pattern.compile(patternSynapses);
        Matcher mS = rS.matcher(synapseFilePath);
        mN.matches();
        mS.matches();

        try {
            if (mS.group(1).equals(mN.group(1))) {
                dataset = mS.group(1);
            } else {
                LOG.log(Level.INFO, "Check that input files are from the same dataset.");
                System.exit(1);
            }
        } catch (IllegalStateException ise) {
            LOG.log(Level.INFO, "Check input file names.", ise);
            System.exit(1);
        }
    }

    public static Long setSkeletonAssociatedBodyId(String swcFilePath) {

        String patternSurroundingId = ".*/(.*?).swc";
        Pattern r = Pattern.compile(patternSurroundingId);
        Matcher mR = r.matcher(swcFilePath);
        mR.matches();
        Long associatedBodyId = Long.parseLong(mR.group(1));
        return associatedBodyId;

    }


    public static void main(String[] args) throws Exception {
        final ConverterParameters parameters = new ConverterParameters();
        final JCommander jCommander = new JCommander(parameters);
        jCommander.setProgramName("java -cp neuprinter.jar " + ConnConvert.class.getName());


        boolean parseFailed = true;
        try {
            jCommander.parse(args);
            parseFailed = false;
        } catch (final ParameterException pe) {
            JCommander.getConsole().println("\nERROR: failed to parse command line arguments\n\n" + pe.getMessage());
        } catch (final Throwable t) {
            LOG.log(Level.INFO, "failed to parse command line arguments", t);
        }

        if (parameters.help || parseFailed) {
            JCommander.getConsole().println("");
            jCommander.usage();
            System.exit(1);
        }

        LOG.info("running with parameters: " + parameters);

        dataset = parameters.datasetLabel;


        if (parameters.createLog) {

            FileHandler fh;
            try {
                String timeStamp = new SimpleDateFormat("yyyy-MM-dd_HH").format(new java.util.Date());
                fh = new FileHandler("connconvertmainlog_" + timeStamp + ".log");
                fh.setFormatter(new SimpleFormatter());
                LOG.addHandler(fh);

                //LOG.setUseParentHandlers(false);

            } catch (SecurityException e) {
                e.printStackTrace();
            }

        }


//        if ((parameters.datasetLabel != null)) {
//            String patternNeurons = ".*/(.*?)_Neurons.*";
//            Pattern rN = Pattern.compile(patternNeurons);
//            Matcher mN = rN.matcher(parameters.neuronJson);
//            String patternSynapses = ".*/(.*?)_Synapses.*";
//            Pattern rS = Pattern.compile(patternSynapses);
//            Matcher mS = rS.matcher(parameters.synapseJson);
//            mN.matches();
//            mS.matches();
//            // TODO: ask user if it's okay to continue if the dataset names seem different
//            if (mN.group(1).equals(mS.group(1))) {
//
//
//            } else {
////                Scanner input = new Scanner( System.in );
////                String userInput = null;
////                while (userInput == null ) {
////                    System.out.print("Input file names do not appear to be from the same dataset. Okay to proceed? (y/n) ");
////                    userInput = input.nextLine();
////                    if (!userInput.equals("y") && !userInput.equals("n")) {
////                        System.out.println("Incorrect response, please respond with y or n. ");
////                    } else if (userInput.equals("y")) {
////
////
////                    }
//                    System.out.print("Input file names do not appear to be from the same dataset.");
//                    System.exit(1);
//                }
//        } else {
//            setDatasetName(parameters.neuronJson, parameters.synapseJson);
//        }

        LOG.info("Dataset is: " + dataset);


        if (parameters.loadNeurons || parameters.doAll) {

            // read in the neurons data
            Stopwatch timer2 = Stopwatch.createStarted();
            neuronList = readNeuronsJson(parameters.neuronJson);
            LOG.info("Reading in neurons json took: " + timer2.stop());
            timer2.reset();
            //write it to the database
            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                if (parameters.prepDatabase || parameters.doAll) {
                    neo4jImporter.prepDatabase(dataset);
                }

                Stopwatch timer = Stopwatch.createStarted();
                neo4jImporter.addNeurons(dataset, neuronList);
                LOG.info("Loading all Neuron nodes took: " + timer.stop());
                timer.reset();
            }
        }


        if (parameters.loadSynapses || parameters.doAll) {

            Stopwatch timer = Stopwatch.createStarted();
            SynapseMapper mapper = new SynapseMapper();
            bodyList = mapper.loadAndMapBodies(parameters.synapseJson);
            LOG.info("Number of bodies with synapses: " + bodyList.size());
            LOG.info("Reading in synapse json took: " + timer.stop());
            timer.reset();

            //TODO: do I need to worry about duplicates here?
            HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();


            //can now sort bodyList by synapse count for sId use
            bodyList.sort(new SortBodyByNumberOfSynapses());


            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {


                if (parameters.prepDatabase && !(parameters.loadNeurons || parameters.doAll)) {
                    neo4jImporter.prepDatabase(dataset);
                }

                if (parameters.addConnectsTo || parameters.doAll) {
                    timer.start();
                    if (parameters.bigThreshold != null) {
                        neo4jImporter.addConnectsTo(dataset, bodyList, parameters.bigThreshold);
                    } else {
                        neo4jImporter.addConnectsTo(dataset, bodyList, 10);
                    }
                    LOG.info("Loading all ConnectsTo took: " + timer.stop());
                    timer.reset();
                }

                if (parameters.addSynapses || parameters.doAll) {
                    timer.start();
                    neo4jImporter.addSynapsesWithRois(dataset, bodyList);
                    LOG.info("Loading all Synapses took: " + timer.stop());
                    timer.reset();
                }

                if (parameters.addSynapsesTo || parameters.doAll) {
                    timer.start();
                    neo4jImporter.addSynapsesTo(dataset, preToPost);
                    LOG.info("Loading all SynapsesTo took: " + timer.stop());
                    timer.reset();
                }

                if (parameters.addNeuronRois || parameters.doAll) {
                    timer.start();
                    neo4jImporter.addNeuronRois(dataset, bodyList);
                    LOG.info("Loading all Neuron ROI labels took: " + timer.stop());
                    timer.reset();
                }

                if (parameters.addSynapseSets || parameters.doAll) {
                    timer.start();
                    neo4jImporter.addSynapseSets(dataset, bodyList);
                    LOG.info("Loading SynapseSets took: " + timer.stop());
                    timer.reset();
                }


            }

            if (parameters.addNeuronParts || parameters.doAll) {

                timer.start();
                for (BodyWithSynapses bws : bodyList) {
                    bws.setNeuronParts();

                }
                LOG.info("Setting neuron parts took: " + timer.stop());


                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                    timer.start();
                    neo4jImporter.addNeuronParts(dataset, bodyList);
                    LOG.info("Loading all NeuronParts took: " + timer.stop());
                    timer.reset();
                }
            }


        }


        if (parameters.addSkeletons) {

            File folder = new File(parameters.skeletonDirectory);
            File[] listOfSwcFiles = folder.listFiles((dir,name) -> name.toLowerCase().endsWith(".swc"));
            List<Skeleton> skeletonList = new ArrayList<>();

            LOG.info("Reading in " + listOfSwcFiles.length + " swc files.");

            for (File swcFile : listOfSwcFiles) {
                String filepath = swcFile.getAbsolutePath();
                Long associatedBodyId = setSkeletonAssociatedBodyId(filepath);
                Skeleton skeleton = new Skeleton();

                try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
                    skeleton.fromSwc(reader, associatedBodyId);
                    skeletonList.add(skeleton);
                    //LOG.info("Loaded skeleton associated with bodyId " + associatedBodyId + " and size " + skeleton.getSkelNodeList().size());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                if (parameters.prepDatabase && !(parameters.loadNeurons || parameters.doAll || parameters.loadSynapses)) {
                    neo4jImporter.prepDatabase(dataset);
                }

                Stopwatch timer = Stopwatch.createStarted();
                neo4jImporter.addSkeletonNodes(dataset, skeletonList);
                LOG.info("Loading all Skeleton nodes took: " + timer.stop());
                timer.reset();
            }


        }


        if (parameters.editMode) {

//            final HashMap<String,NeuronTypeTree> neuronTypeTreeMap = NeuronTypeTree.readTypeTree("mb6_cell_types.csv");
//
//            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
//
//                neo4jImporter.prepDatabase(dataset);
//
//                neo4jImporter.addCellTypeTree(dataset,neuronTypeTreeMap);
//
//
//            }

//            Stopwatch timer2 = Stopwatch.createStarted();
//            neuronList = readNeuronsJson(parameters.neuronJson);
//            LOG.info("Reading in neurons json took: " + timer2.stop());
//            timer2.reset();

//            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
//                neo4jImporter.prepDatabase(dataset);
//            }

//            try (Neo4jEditor neo4jEditor = new Neo4jEditor(parameters.getDbConfig())) {


//                List<Skeleton> mbonSkeletons = new ArrayList<>();
//                for (Skeleton skeleton : skeletonList) {
//                    if (skeleton.getAssociatedBodyId()==1661302 || skeleton.getAssociatedBodyId()==1190582) {
//                        mbonSkeletons.add(skeleton);
//                    }
//                }
//
//                Stopwatch timer = Stopwatch.createStarted();
//                neo4jEditor.updateSkelNodesRowNumber(dataset, mbonSkeletons);
//                LOG.info("Updating skelnodes took: " + timer.stop());
//                timer.reset();
//
//                timer.start();
//                neo4jEditor.linkAllSkelNodesToSkeleton(dataset, mbonSkeletons);
//                LOG.info("Adding links to skeletons took: " + timer.stop());
//                timer.reset();

//                timer2.start();
//                neo4jEditor.updateNeuronProperties(dataset, neuronList);
//                LOG.info("Updating all Neuron nodes took: " + timer2.stop());
//                timer2.reset();

//            }

        }
    }

    private static final Logger LOG = Logger.getLogger("ConnConvert.class");

}





