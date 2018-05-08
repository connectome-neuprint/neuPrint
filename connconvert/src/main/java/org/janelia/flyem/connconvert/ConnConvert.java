package org.janelia.flyem.connconvert;

import java.io.BufferedReader;
import java.io.FileReader;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.FieldNamingPolicy;


import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.io.File;
import java.util.Arrays;
import java.util.logging.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.stream.JsonReader;
import org.janelia.flyem.connconvert.db.DbConfig;
import org.janelia.flyem.connconvert.json.JsonUtils;
import org.janelia.flyem.connconvert.model.Neuron;


// TODO: Add ROI information using column names from neurons file?
// FIB25 names often include column info (7 columns)  - pnas paper.
public class ConnConvert {


    @Parameters(separators = "=")
    public static class ConverterParameters {

        @Parameter(
                names = "--dbProperties",
                description = "Properties file containing database information (omit to print statements to stdout)",
                required = false)
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
                names = "--doAll"
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
                names = "--addSizeId",
                description = "Indicates that neuron size-based ID should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addSizeId;

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
                names = "--neuronJson",
                description = "JSON file containing neuron data to import",
                required = false)
        public String neuronJson;

        @Parameter(
                names = "--neuronDataset",
                description = "Dataset value for all neurons (if not specified will read from file name)",
                required = false)
        public String neuronDataset;

        @Parameter(
                names = "--synapseJson",
                description = "JSON file containing body synapse data to import",
                required = false)
        public String synapseJson;

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


//
//    public void testLoadNeuronsWithJSON() throws Exception {
//        try (Session session = driver.session()) {
//            for (Neuron neuron : neurons) {
//                try (Transaction tx = session.beginTransaction()) {
//                    String stringQuery = "CALL apoc.load.json(\"/Users/neubarthn/Downloads/fib25_neo4j_inputs/fib25_Neurons.json\") \n" +
//                            "YIELD value AS neurons \n" +
//                            //"UNWIND neurons AS test \n" +
//                            "RETURN neurons";
//                    tx.run(stringQuery);
//                    tx.success();
//
//                }
//            }
//        }
//    }
//
//    public void testSynapseLoad() throws Exception {
//        try (Session session = driver.session()) {
//            for (int i=0 ; i <= 100 ; i++) {
//                if (bodyList.get(i).getBodyId() != 304654117 || !dataset.equals("mb6v2")) {
//                    for (Synapse synapse : bodyList.get(i).getSynapseSet()) {
//
//
//                        if (synapse.getType().equals("pre")) {
//
//                            //StatementResult test=null;
//                            try (Transaction tx = session.beginTransaction()) {
//                                tx.run("CREATE (s:Synapse:PreSyn {datasetLocation:$datasetLocation}) " +
//                                                "SET s.location = $location," +
//                                                " s.datasetLocation = $datasetLocation," +
//                                                " s.confidence = $confidence," +
//                                                " s.type = $type," +
//                                                " s.x=$x," +
//                                                " s.y=$y," +
//                                                " s.z=$z \n" +
//                                                " WITH s \n" +
//                                                " CALL apoc.create.addLabels(id(s),[$dataset]) YIELD node \n" +
//                                                " RETURN node",
//                                        parameters("location", synapse.getLocationString(),
//                                                "datasetLocation", dataset + ":" + synapse.getLocationString(),
//                                                "confidence", synapse.getConfidence(),
//                                                "type", synapse.getType(),
//                                                "x", synapse.getLocation().get(0),
//                                                "y", synapse.getLocation().get(1),
//                                                "z", synapse.getLocation().get(2),
//                                                "dataset", dataset));
//                                //LOG.info(test.summary().toString());
//                                tx.success();
//                            } catch (ClientException ce) {
//                                ce.printStackTrace();
//                            }
//                            //LOG.info("Loading Synapse node with CREATE+apoc.create.addLabels took: " + timer.stop());
//
//                            //timer.start();
//                            try (Transaction tx = session.beginTransaction()) {
//                                String stringQuery = "CREATE (s:Synapse:PreSyn {datasetLocation:$datasetLocation}) " +
//                                        "SET s.location = $location," +
//                                        " s.datasetLocation = $datasetLocation," +
//                                        " s.confidence = $confidence," +
//                                        " s.type = $type," +
//                                        " s.x=$x," +
//                                        " s.y=$y," +
//                                        " s.z=$z," +
//                                        " s:" + dataset ;
//                                tx.run(stringQuery,
//                                        parameters("location", synapse.getLocationString(),
//                                                "datasetLocation", dataset + ":" + synapse.getLocationString(),
//                                                "confidence", synapse.getConfidence(),
//                                                "type", synapse.getType(),
//                                                "x", synapse.getLocation().get(0),
//                                                "y", synapse.getLocation().get(1),
//                                                "z", synapse.getLocation().get(2)));
//                                tx.success();
//                            } catch (ClientException ce) {
//                                System.out.println("Synapse already present.");
//                            }
//                            //LOG.info("Loading Synapse node with just CREATE+stringbuild took: " + timer.stop());
//                        }
//
//                    }
//                }
//            }
//        }
//    }
//


    private static List<Neuron> readNeuronsJson(String filepath) {

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            neuronList = Neuron.fromJson(reader);
            System.out.println("Number of neurons: " + neuronList.size());
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
            System.out.println("Number of bodies with synapses: " + bodyList.size());
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

    private static void setDatasetName(String neuronFilePath,String synapseFilePath) {
        String patternNeurons = ".*inputs/(.*?)_Neurons.*";
        Pattern rN = Pattern.compile(patternNeurons);
        Matcher mN = rN.matcher(neuronFilePath);
        String patternSynapses = ".*inputs/(.*?)_Synapses.*";
        Pattern rS = Pattern.compile(patternSynapses);
        Matcher mS = rS.matcher(synapseFilePath);
        mN.matches();
        mS.matches();

        try {
            if (mS.group(1).equals(mN.group(1))) {
                dataset = mS.group(1);
            } else {
                LOG.log(Level.INFO,"Check that input files are from the same dataset.");
                System.exit(1);
            }
        } catch (IllegalStateException ise) {
            LOG.log(Level.INFO,"Check input file names.",ise);
            System.exit(1);
        }
    }


    public static void main(String[] args) throws Exception {
        final ConverterParameters parameters = new ConverterParameters();
        final JCommander jCommander = new JCommander(parameters);
        jCommander.setProgramName("java -cp converter.jar " + ConnConvert.class.getName());

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


        if ((parameters.neuronDataset != null)) {
            String patternNeurons = ".*/(.*?)_Neurons.*";
            Pattern rN = Pattern.compile(patternNeurons);
            Matcher mN = rN.matcher(parameters.neuronJson);
            String patternSynapses = ".*/(.*?)_Synapses.*";
            Pattern rS = Pattern.compile(patternSynapses);
            Matcher mS = rS.matcher(parameters.synapseJson);
            mN.matches();
            mS.matches();
            // TODO: ask user if it's okay to continue if the dataset names seem different
            if (mN.group(1).equals(mS.group(1))) {
                dataset = parameters.neuronDataset;

            } else {
                LOG.log(Level.INFO, "Check that input files are from the same dataset.");
                System.exit(1);
            }
        } else {
            setDatasetName(parameters.neuronJson, parameters.synapseJson);
        }

        System.out.println("Dataset is: " + dataset);


        if (parameters.loadNeurons || parameters.doAll) {

            // read in the neurons data
            Stopwatch timer2 = Stopwatch.createStarted();
            neuronList = readNeuronsJson(parameters.neuronJson);
            System.out.println(timer2.stop());
            timer2.reset();
            //write it to the database
            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                if (parameters.prepDatabase || parameters.doAll) {
                    neo4jImporter.prepDatabase();
                }

                Stopwatch timer = Stopwatch.createStarted();
                neo4jImporter.addNeurons(dataset, neuronList);
                LOG.info("Loading all Neuron nodes took: " + timer.stop());
                timer.reset();
            }
        }


        if (parameters.loadSynapses || parameters.doAll) {

            Stopwatch timer = Stopwatch.createStarted();
            bodyList = readSynapsesJson(parameters.synapseJson);
            LOG.info("Loading all synapse data took: " + timer.stop());
            timer.reset();

            //create a new hashmap for storing: body>pre, pre>post; post>body
            HashMap<String, Long> preToBody = new HashMap<>();
            HashMap<String, Long> postToBody = new HashMap<>();
            HashMap<String, List<String>> preToPost = new HashMap<>();
            timer.start();
            for (BodyWithSynapses bws : bodyList) {
                List<String> preLocs = bws.getPreLocations();
                List<String> postLocs = bws.getPostLocations();

                if (!preLocs.isEmpty()) {
                    for (String loc : preLocs) {
                        preToBody.put(loc, bws.getBodyId());

                    }
                }
                if (!postLocs.isEmpty()) {
                    for (String loc : postLocs) {
                        postToBody.put(loc, bws.getBodyId());
                    }
                }
            }

            LOG.info("hashmaps took : " + timer.stop());
            timer.reset();
            timer.start();
            for (BodyWithSynapses bws : bodyList) {
                bws.setConnectsTo(postToBody);
                bws.setSynapseCounts();
                preToPost.putAll(bws.getPreToPostForBody());
            }

            LOG.info("setting features took : " + timer.stop());
            timer.reset();

            // in mb6 bodyId 304654117 has a synapse classified as both pre and post
            // System.out.println(postToBody.get("4305:5400:11380"));
            // System.out.println(preToBody.get("4305:5400:11380"));

            //can now sort bodyList by synapse count
            timer.start();
            bodyList.sort(new SortBodyByNumberOfSynapses());
            LOG.info("sorting by synapses took : " + timer.stop());
            timer.reset();



            //logging
        FileHandler fh;
        try {

            fh = new FileHandler("hemitestload.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);

            //LOG.setUseParentHandlers(false);

        } catch (SecurityException e) {
            e.printStackTrace();
        }


            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                if ((parameters.prepDatabase || parameters.doAll) && !parameters.loadNeurons) {
                    neo4jImporter.prepDatabase();
                }

                if (parameters.addConnectsTo || parameters.doAll) {
                    timer.start();
                    neo4jImporter.addConnectsTo(dataset, bodyList);
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

                if (parameters.addSizeId || parameters.doAll) {
                    timer.start();
                    neo4jImporter.addSizeId(dataset, bodyList);
                    LOG.info("Adding all sIds took: " + timer.stop());
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
    }

    private static final Logger LOG = Logger.getLogger("ConnConvert.class");

}





