package connconvert;


import java.io.BufferedReader;
import java.io.FileReader;

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.FieldNamingPolicy;

import java.util.List;
import java.util.HashMap;
import java.io.File;
import java.util.Collections;
import java.util.Arrays;
import java.util.logging.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import connconvert.db.DbConfig;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import static org.neo4j.driver.v1.Values.parameters;

// TODO: Add ROI information using column names from neurons file?
// FIB25 names often include column info (7 columns)  - pnas paper.
public class ConnConvert {

    private static Neuron[] neuronsArray;
    private static List<Neuron> neurons;
    private static BodyWithSynapses[] bodiesArray;
    private static List<BodyWithSynapses> bodies;
    private static String dataset;

    private ConnConvert () {
    }

    public DbConfig getDbConfig() {
        String configPath = new File("").getAbsolutePath();
        configPath = configPath.concat("/connconvert.properties");
        return (configPath == null) ? null : DbConfig.fromFile(new File(configPath));
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
//    public void addRois() {
//        try (Session session = driver.session()) {
//            for (BodyWithSynapses bws : bodies) {
//                for (Synapse synapse : bws.getSynapseSet()) {
//                    List<String> roiList = synapse.getRois();
//                    try (Transaction tx = session.beginTransaction()) {
//                        tx.run("MERGE (s:Synapse {datasetLocation:$datasetLocation}) ON CREATE SET s.location = $location, s.datasetLocation=$datasetLocation \n" +
//                                        "WITH s \n" +
//                                        "CALL apoc.create.addLabels(id(s),$rois) YIELD node \n" +
//                                        "RETURN node",
//                                parameters("location", synapse.getLocationString(),
//                                        "datasetLocation",dataset+":"+synapse.getLocationString(),
//                                        "rois", roiList));
//                        tx.success();
//                    }
//                    try (Transaction tx = session.beginTransaction()) {
//                        tx.run("MERGE (n:Neuron {datasetBodyId:$datasetBodyId}) ON CREATE SET n.bodyId = $bodyId, n.datasetBodyId=$datasetBodyId \n" +
//                                        "WITH n \n" +
//                                        "CALL apoc.create.addLabels(id(n),$rois) YIELD node \n" +
//                                        "RETURN node",
//                                parameters("bodyId", bws.getBodyId(),
//                                        "datasetBodyId",dataset+":"+bws.getBodyId(),
//                                        "rois", roiList));
//                        tx.success();
//                    }
//                }
//
//            }
//        }
//        System.out.println("ROI labels added to Synapses and Neurons.");
//    }
//
//
//
//    public void addSynapsesTo(HashMap<String,List<String>> preToPost) throws Exception {
//        try (Session session = driver.session()) {
//            for (String preLoc : preToPost.keySet()) {
//                for (String postLoc : preToPost.get(preLoc)) {
//                    try (Transaction tx = session.beginTransaction()) {
//
//                        tx.run("MERGE (s:Synapse {datasetLocation:$datasetPreLocation}) ON CREATE SET s.location = $prelocation, s.datasetLocation=$datasetPreLocation,s:createdforsynapsesto \n" +
//                                        "MERGE (t:Synapse {datasetLocation:$datasetPostLocation}) ON CREATE SET t.location = $postlocation, t.datasetLocation=$datasetPostLocation, t:createdforsynapsesto \n" +
//                                        "MERGE (s)-[:SynapsesTo]->(t) \n",
//                                parameters("prelocation", preLoc,
//                                        "datasetPreLocation",dataset+":"+preLoc,
//                                        "datasetPostLocation",dataset+":"+postLoc,
//                                        "postlocation", postLoc));
//                        tx.success();
//
//
//                    }
//                }
//            }
//            System.out.println("SynapsesTo relations added.");
//        }
//    }
////
//    public void addNeuronParts() throws Exception {
//        try (Session session = driver.session()) {
//                for (BodyWithSynapses bws: bodies) {
//                    for (NeuronPart np : bws.getNeuronParts()) {
//                        try(Transaction tx = session.beginTransaction()) {
//                        // create neuronpart node that points to neuron with partof relation
//                            String neuronPartId = dataset+":"+bws.getBodyId()+":"+np.getRoi();
//                        tx.run("MERGE (n:Neuron {datasetBodyId:$datasetBodyId}) ON CREATE SET n.bodyId=$bodyId, n.datasetBodyId=$datasetBodyId, n:createdforneuronpart \n"+
//                                        "MERGE (p:NeuronPart {neuronPartId:$neuronPartId}) ON CREATE SET p.neuronPartId = $neuronPartId, p.pre=$pre, p.post=$post, p.size=$size \n"+
//                                        "MERGE (p)-[:PartOf]->(n) \n" +
//                                        "WITH p \n" +
//                                        "CALL apoc.create.addLabels(id(p),[$roi, $dataset]) YIELD node \n" +
//                                        "RETURN node",
//                                parameters("bodyId",bws.getBodyId(),
//                                        "roi",np.getRoi(),
//                                        "dataset",dataset,
//                                        "neuronPartId",neuronPartId,
//                                        "datasetBodyId",dataset+":"+bws.getBodyId(),
//                                        "pre",np.getPre(),
//                                        "post",np.getPost(),
//                                        "size",np.getPre()+np.getPost()));
//                        tx.success();
//                    }
//                }
//            }
//        }
//        System.out.println("NeuronPart nodes added with PartOf relationships.");
//    }
//
//    public void addSizeId() throws Exception {
//        int sId = 0;
//        try (Session session = driver.session()) {
//            for (BodyWithSynapses bws : bodies) {
//                try (Transaction tx = session.beginTransaction()) {
//                    // bodies should be sorted in descending order by number of synapses, so can create id starting at 0
//
//
//                    tx.run("MERGE (n:Neuron {datasetBodyId:$datasetBodyId}) ON CREATE SET n.bodyId=$bodyId, n.datasetBodyId=$datasetBodyId, n:createdforsid \n" +
//                                    "SET n.sId=$sId",
//                            parameters("bodyId", bws.getBodyId(),
//                                    "datasetBodyId",dataset+":"+ bws.getBodyId(),
//                                    "sId", sId));
//                    sId++;
//                    tx.success();
//
//                }
//            }
//        }
//        System.out.println("Added sId to neurons in this dataset.");
//    }
//
//
//    public void addSynapseSets() throws Exception {
//        try (Session session = driver.session()) {
//            for (BodyWithSynapses bws : bodies) {
//
//                try (Transaction tx = session.beginTransaction()) {
//                    tx.run("MERGE (n:Neuron {datasetBodyId:$datasetBodyId}) ON CREATE SET n.bodyId=$bodyId, n.datasetBodyId=$datasetBodyId \n" +
//                            "MERGE (s:SynapseSet {datasetBodyId:$datasetBodyId}) ON CREATE SET s.datasetBodyId=$datasetBodyId \n" +
//                            "MERGE (n)-[:Contains]->(s) \n" +
//                                    "WITH s \n" +
//                                    "CALL apoc.create.addLabels(id(s),[$dataset]) YIELD node \n" +
//                                    "RETURN node",
//                            parameters("bodyId",bws.getBodyId(),
//                                    "datasetBodyId",dataset+":"+bws.getBodyId(),
//                                    "dataset",dataset));
//                    tx.success();
//                }
//                for (Synapse synapse : bws.getSynapseSet()) {
//                    try (Transaction tx = session.beginTransaction()) {
//                        tx.run("MERGE (s:Synapse {datasetLocation:$datasetLocation}) ON CREATE SET s.location=$location, s.datasetLocation=$datasetLocation \n"+
//                                "MERGE (t:SynapseSet {datasetBodyId:$datasetBodyId}) ON CREATE SET t.bodyId=$datasetBodyId \n" +
//                                "MERGE (t)-[:Contains]->(s) \n" +
//                                        "WITH t \n" +
//                                        "CALL apoc.create.addLabels(id(t),[$dataset]) YIELD node \n" +
//                                        "RETURN node",
//                                parameters("location", synapse.getLocationString(),
//                                        "datasetLocation",dataset+":"+synapse.getLocationString(),
//                                        "bodyId", bws.getBodyId(),
//                                        "datasetBodyId",dataset+":"+bws.getBodyId(),
//                                        "dataset",dataset));
//                        tx.success();
//                    }
//                }
//            }
//        }
//        System.out.println("SynapseSet nodes with Contains relations added.");
//    }
//



    public static List<Neuron> readNeuronsJson(String filepath) throws Exception{
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

    public static List<BodyWithSynapses> readSynapsesJson(String filepath) throws Exception{
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

        //String filepath = properties.getProperty("fib25neurons");
        //String filepath2 = properties.getProperty("fib25synapses");
        //String filepath = properties.getProperty("mb6neurons");
        //String filepath2 = properties.getProperty("mb6synapses");

        String filepath = "/Users/neubarthn/Downloads/fib25_neo4j_inputs/fib25_Neurons.json";
        String filepath2 = "/Users/neubarthn/Downloads/fib25_neo4j_inputs/fib25_Synapses_with_rois.json";

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
        HashMap<String,List<String>> preToPost = new HashMap<>();

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
        Collections.sort(bodies,new SortBodyByNumberOfSynapses());


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
        DbConfig dbConfig = connConvert.getDbConfig();

        try(Neo4jImporter neo4jImporter = new Neo4jImporter(dbConfig)){
            neo4jImporter.prepDatabase();

            Stopwatch timer = Stopwatch.createStarted();
            String testLabel = "speedtest";
            //neo4jImporter.addNeurons(dataset,neurons);
            LOG.info("Loading all Neuron nodes took: " + timer.stop());

            timer.start();
            //neo4jImporter.addConnectsTo(dataset,bodies);
            LOG.info("Loading all ConnectsTo took: " + timer.stop());

            timer.start();
            //neo4jImporter.addSynapses(dataset,bodies);
            LOG.info("Loading all Synapses took: " + timer.stop());

            timer.start();
            neo4jImporter.addSynapsesTo(dataset,preToPost);
            LOG.info("Loading all SynapsesTo took: " + timer.stop());

        }



    }

    private static final Logger LOG = Logger.getLogger("ConnConvert.class");



}





