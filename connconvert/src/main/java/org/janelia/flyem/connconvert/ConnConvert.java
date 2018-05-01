package org.janelia.flyem.connconvert;

import java.io.BufferedReader;
import java.io.FileReader;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.FieldNamingPolicy;

import java.util.List;
import java.util.HashMap;
import java.io.File;
import java.util.Arrays;
import java.util.logging.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.janelia.flyem.connconvert.db.DbConfig;
import org.janelia.flyem.connconvert.json.JsonUtils;


// TODO: Add ROI information using column names from neurons file?
// FIB25 names often include column info (7 columns)  - pnas paper.
public class ConnConvert {


    public static class ConverterParameters {

        @Parameter(
                names = "--dbProperties",
                description = "Properties file containing database information (omit to print statements to stdout)",
                required = false)
        public String dbProperties;

        @Parameter(
                names = "--prepDatabase",
                description = "Indicates that database constraints and indexes should be setup (omit to skip)",
                required = false,
                arity = 0)
        public boolean prepDatabase;

        @Parameter(
                names = "--neuronJson",
                description = "JSON file containing neuron data to import",
                required = false)
        public String neuronJson;

        @Parameter(
                names = "--neuronDataSet",
                description = "Data set value for all neurons",
                required = false)
        public String neuronDataSet;

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

        //TODO: what is this for?
        @Override
        public String toString() {
            return JsonUtils.GSON.toJson(this);
        }
    }


    private static List<Neuron> neurons;
    private static List<BodyWithSynapses> bodies;
    private static String dataset;

    private ConnConvert() {
    }


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
//                if (bodies.get(i).getBodyId() != 304654117 || !dataset.equals("mb6v2")) {
//                    for (Synapse synapse : bodies.get(i).getSynapseSet()) {
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


    private static List<Neuron> readNeuronsJson(String filepath) throws Exception {
        Neuron[] neuronsArray;
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
            neuronsArray = gson.fromJson(reader, Neuron[].class);
            neurons = Arrays.asList(neuronsArray);
            //System.out.println("Object mode: " + neurons[0]);
            System.out.println("Number of neurons: " + neurons.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return neurons;
    }

    private static List<BodyWithSynapses> readSynapsesJson(String filepath) throws Exception {
        BodyWithSynapses[] bodiesArray;
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
            bodiesArray = gson.fromJson(reader, BodyWithSynapses[].class);
            bodies = Arrays.asList(bodiesArray);
            //System.out.println("Object mode: " + bodies[0]);
            System.out.println("Number of bodies with synapses: " + bodies.size());
            //System.out.println(bodies[0].synapseSet.get(2).getConnectionLocationStrings().get(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bodies;
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



        //String filepath = properties.getProperty("fib25neurons");
        //String filepath2 = properties.getProperty("fib25synapses");
        //String filepath = properties.getProperty("mb6neurons");
        //String filepath2 = properties.getProperty("mb6synapses");

        //String filepath = "/Users/neubarthn/Downloads/fib25_neo4j_inputs/fib25_Neurons.json";
        //String filepath2 = "/Users/neubarthn/Downloads/fib25_neo4j_inputs/fib25_Synapses_with_rois.json";

        String filepath = "/Users/neubarthn/Downloads/mb6_neo4j_inputs/mb6_Neurons.json";
        String filepath2 = "/Users/neubarthn/Downloads/mb6_neo4j_inputs/mb6_Synapses.json";

        //read dataset name
        String patternNeurons = ".*inputs/(.*?)_Neurons.*";
        Pattern rN = Pattern.compile(patternNeurons);
        Matcher mN = rN.matcher(filepath);
        String patternSynapses = ".*inputs/(.*?)_Synapses.*";
        Pattern rS = Pattern.compile(patternSynapses);
        Matcher mS = rS.matcher(filepath2);
        mN.matches();
        mS.matches();

        try {
            if (mS.group(1).equals(mN.group(1))) {
                dataset = mS.group(1);
            }
        } catch (IllegalStateException ise) {
            System.out.println("Check input file names.");
            return;
        }

        System.out.println("Dataset is: " + dataset);
        if (dataset.equals("mb6")) {
            dataset = "mb6v2";
        }


        neurons = readNeuronsJson(filepath);
        bodies = readSynapsesJson(filepath2);


        //sorting the neurons by size
        //Collections.sort(neurons,new SortNeuronBySize());
        //System.out.println(neurons.get(0));

        //create a new hashmap for storing: body>pre, pre>post; post>body
        HashMap<String, Integer> preToBody = new HashMap<>();
        HashMap<String, Integer> postToBody = new HashMap<>();
        HashMap<String, List<String>> preToPost = new HashMap<>();

        for (BodyWithSynapses bws : bodies) {
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
        for (BodyWithSynapses bws : bodies) {
            bws.setNeuronParts();
            bws.setConnectsTo(postToBody);
            bws.setConnectsFrom(preToBody);
            bws.setSynapseCounts();
            preToPost.putAll(bws.getPreToPostForBody());

        }

        // in mb6 bodyId 304654117 has a synapse classified as both pre and post
        // System.out.println(postToBody.get("4305:5400:11380"));
        // System.out.println(preToBody.get("4305:5400:11380"));

        //can now sort bodies by synapse count
        bodies.sort(new SortBodyByNumberOfSynapses());

        //System.out.println(bodies[3].connectsTo);
        //System.out.println(bodies[3].connectsFrom);
        //List<Integer> temploc = new ArrayList<Integer>() {{
        //    add(4657);
        //    add(2648);
        //    add(1509);
        //}};
        //System.out.println(preToPost.get(temploc));
        //System.out.println(preToPost.keySet());
        //System.out.println(bodies[0].getSynapseSet().get(0));
        // start upload to database


        //logging
        FileHandler fh;
        try {


            fh = new FileHandler("/Users/neubarthn/Documents/GitHub/ConnectomeJSONtoNeo4j/connconvert/logs/neo4jload.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);

            //LOG.setUseParentHandlers(false);


        } catch (SecurityException e) {
            e.printStackTrace();
        }


        //try(ConnConvert connConvert = new ConnConvert(uri,user,password)) {
        // uncomment to add different features to database
        //connConvert.prepDatabase(); //ran 7:30
        //connConvert.addNeurons(); //ran 7:30
        //connConvert.addConnectsTo(); // ran 10PM
        //connConvert.addSynapses();
        //connConvert.addSynapsesTo(preToPost);
        //connConvert.addRois();
        //connConvert.addNeuronParts();
        //connConvert.addSizeId(); //
        //connConvert.addSynapseSets(); //

        //connConvert.testSynapseLoad();
        //connConvert.testLoadNeuronsWithJSON();
        //}

        ConnConvert connConvert = new ConnConvert();


        try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
            neo4jImporter.prepDatabase();

            Stopwatch timer = Stopwatch.createStarted();
            neo4jImporter.addNeurons(dataset, neurons);
            LOG.info("Loading all Neuron nodes took: " + timer.stop());

            timer.start();
            neo4jImporter.addConnectsTo(dataset, bodies);
            LOG.info("Loading all ConnectsTo took: " + timer.stop());

            timer.start();
            neo4jImporter.addSynapses(dataset, bodies);
            LOG.info("Loading all Synapses took: " + timer.stop());

            timer.start();
            neo4jImporter.addSynapsesTo(dataset, preToPost);
            LOG.info("Loading all SynapsesTo took: " + timer.stop());

            timer.start();
            neo4jImporter.addRois(dataset, bodies);
            LOG.info("Loading all ROI labels took: " + timer.stop());

            timer.start();
            neo4jImporter.addNeuronParts(dataset, bodies);
            LOG.info("Loading all NeuronParts took: " + timer.stop());

            timer.start();
            neo4jImporter.addSizeId(dataset, bodies);
            LOG.info("Adding all sIds took: " + timer.stop());

            timer.start();
            neo4jImporter.addSynapseSets(dataset, bodies);
            LOG.info("Loading SynapseSets took: " + timer.stop());


        }


    }

    private static final Logger LOG = Logger.getLogger("ConnConvert.class");


}





