package connconvert;


import java.io.BufferedReader;
import java.io.FileReader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.FieldNamingPolicy;

import java.util.List;
import java.util.HashMap;


import org.neo4j.driver.v1.*;
import static org.neo4j.driver.v1.Values.parameters;


public class ConnConvert implements AutoCloseable {
    private final Driver driver;
    public static Neuron[] neurons;
    public static BodyWithSynapses[] bodies;

    public ConnConvert (String uri, String user, String password) {
        driver = GraphDatabase.driver(uri,AuthTokens.basic(user, password));
    }

    @Override
    public void close() throws Exception {
        driver.close();
        System.out.println("Driver closed.");
    }

    public void addNeurons() throws Exception {

        try (Session session = driver.session()) {
            for (Neuron neuron : neurons) {
                try (Transaction tx = session.beginTransaction()) {
                    // TODO: Index name and status
                    // have already set
                    // CREATE CONSTRAINT ON (n:Neuron) ASSERT n.bodyId IS UNIQUE
                    // CREATE INDEX ON :Neuron(bodyId)
                    tx.run("MERGE (n:Neuron {bodyId:$bodyId}) " +
                                    "ON CREATE SET n.bodyId = $bodyId," +
                                    " n.name = $name," +
                                    " n.type = $type," +
                                    " n.status = $status," +
                                    " n.size = $size, " +
                                    "n:fib25",
                            parameters("bodyId", neuron.getId(),
                                    "name", neuron.getName(),
                                    "type", neuron.getType(),
                                    "status", neuron.getStatus(),
                                    "size", neuron.getSize()));


                    tx.success();

                }

            }
            System.out.println("Added neurons.");
        }

    }

    public void addConnectsTo() throws Exception {
        try (Session session = driver.session()) {
            for (BodyWithSynapses bws : bodies) {
                for (Integer postsynapticBodyId : bws.connectsTo.keySet()) {
                    try (Transaction tx = session.beginTransaction()) {
                        // TODO: Incorporate confidence values for ConnectsTo
                        tx.run("MERGE (n:Neuron {bodyId:$bodyId1}) ON CREATE SET n.bodyId = $bodyId1, n:fib25, n:notinneurons \n" +
                                        "MERGE (m:Neuron {bodyId:$bodyId2}) ON CREATE SET m.bodyId = $bodyId2, m:fib25, m:notinneurons \n" +
                                        "MERGE (n)-[:ConnectsTo{weight:$weight}]->(m) \n",
                                parameters("bodyId1", bws.getBodyId(),
                                        "bodyId2", postsynapticBodyId,
                                        "weight", bws.connectsTo.get(postsynapticBodyId)));

                        tx.success();

                    }
                    try (Transaction tx = session.beginTransaction()) {
                        tx.run("MATCH (n:Neuron {bodyId:$bodyId1} ) SET n.pre = $pre, n.post = $post",
                                parameters("bodyId1", bws.getBodyId(),
                                        "pre", bws.getPre(),
                                        "post", bws.getPost()));
                        tx.success();

                    }

                }

                for (Integer presynapticBodyId : bws.connectsFrom.keySet()) {
                    try (Transaction tx = session.beginTransaction()) {
                        tx.run("MATCH (n:Neuron {bodyId:$bodyId1} ) SET n.pre = $pre, n.post = $post",
                                parameters("bodyId1", bws.getBodyId(),
                                        "pre", bws.getPre(),
                                        "post", bws.getPost()));
                        tx.success();

                    }

                }
            }
            System.out.println("Added ConnectsTo relations.");
            System.out.println("Added pre and post counts.");
        }
    }

    public void addSynapses() throws Exception {
        try (Session session = driver.session()) {
            for (BodyWithSynapses bws : bodies) {
                for (Synapse synapse : bws.getSynapseSet()) {
                    try (Transaction tx = session.beginTransaction()) {
                        // have already set
                        // CREATE CONSTRAINT ON (s:Synapse) ASSERT s.location IS UNIQUE
                        if (synapse.getType().equals("pre")) {
                        tx.run("MERGE (s:Synapse:PreSyn {location:$location}) " +
                                        "ON CREATE SET s.location = $location," +
                                        " s.confidence = $confidence," +
                                        " s.type = $type",
                                parameters("location", synapse.getLocationString(),
                                        "confidence", synapse.getConfidence(),
                                        "type", synapse.getType()));
                        tx.success();
                        } else if (synapse.getType().equals("post")) {
                            tx.run("MERGE (s:Synapse:PostSyn {location:$location}) " +
                                    "ON CREATE SET s.location = $location," +
                                    " s.confidence = $confidence," +
                                    " s.type = $type",
                                    parameters("location", synapse.getLocationString(),
                                            "confidence", synapse.getConfidence(),
                                            "type", synapse.getType()));
                            tx.success();

                        }
                    }
                }
            }
            System.out.println("Synapse nodes added.");
        }

    }


    public static Neuron[] readNeuronsJson(String filepath) throws Exception{
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
            neurons = gson.fromJson(reader, Neuron[].class);
            //System.out.println("Object mode: " + neurons[0]);
            System.out.println("Number of neurons: " + neurons.length);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return neurons;
    }

    public static BodyWithSynapses[] readSynapsesJson(String filepath) throws Exception{
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
            bodies = gson.fromJson(reader, BodyWithSynapses[].class);
            //System.out.println("Object mode: " + bodies[0]);
            System.out.println("Number of bodies with synapses: " + bodies.length);
            //System.out.println(bodies[0].synapseSet.get(2).getConnectionLocationStrings().get(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bodies;
    }


    public static void main(String[] args) throws Exception {
        String filepath = "/Users/neubarthn/Downloads/fib25_neo4j_inputs/fib25_Neurons.json";
        String filepath2 = "/Users/neubarthn/Downloads/fib25_neo4j_inputs/fib25_Synapses.json";

        Neuron[] neurons = readNeuronsJson(filepath);
        BodyWithSynapses[] bodies = readSynapsesJson(filepath2);

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
            bws.setConnectsTo(postToBody);
            bws.setConnectsFrom(preToBody);
            bws.setSynapseCounts();
            preToPost.putAll(bws.getPreToPostForBody());
        }

        //System.out.println(bodies[3].connectsTo);
        //System.out.println(bodies[3].connectsFrom);
        //System.out.println(preToPost.get("4657:2648:1509"));
        //System.out.println(preToPost.keySet());

        // start upload to database

        String uri = "bolt://localhost:7687";
        String user = "neo4j";
        String password = "n304j";


        try(ConnConvert connConvert = new ConnConvert(uri,user,password)) {
            // uncomment to add different features to database
            // connConvert.addNeurons();
            // connConvert.addConnectsTo();
            // connConvert.addSynapses();

        }

    }





}





