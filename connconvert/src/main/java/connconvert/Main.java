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


public class Main implements AutoCloseable {
    private Driver driver;
    public static Neuron[] neurons;
    public static BodyWithSynapses[] bodies;


    public static void main(String[] args) throws Exception {
        Main main = new Main();
        String filename = "/Users/neubarthn/Downloads/fib25_neo4j_inputs/fib25_Neurons.json";


        try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
            Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
            neurons = gson.fromJson(reader, Neuron[].class);
            System.out.println("Object mode: " + neurons[0]);
            System.out.println("Number of neurons: " + neurons.length);

        } catch (Exception e) {
            e.printStackTrace();

        }


        String filename2 = "/Users/neubarthn/Downloads/fib25_neo4j_inputs/fib25_Synapses.json";


        try (BufferedReader reader = new BufferedReader(new FileReader(filename2))) {
            Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
            bodies = gson.fromJson(reader, BodyWithSynapses[].class);
            //System.out.println("Object mode: " + bodies[0]);

            System.out.println("Number of bodies with synapses: " + bodies.length);

            //System.out.println(bodies[0].synapseSet.get(2).getConnectionLocationStrings().get(0));

        } catch (Exception e) {
            e.printStackTrace();

        }


        // create pre to body, post to body, pre to post list
        //create a new hashmap for storing: body>pre, pre>post; post>body
        HashMap<String, Integer> preToBody = new HashMap<>();
        HashMap<String, Integer> postToBody = new HashMap<>();


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
            bws.setSynapseCounts();
        }

        //System.out.println(bodies[3].connectsTo);

        // start upload to database
        main.start();

    }

    @Override
    public void close() throws Exception {
        driver.close();
        System.out.println("Driver closed.");
    }

    public void start() throws Exception {
        //System.out.printf("Input file name is: %s \n", filename);


        String uri = "bolt://localhost:7687";
        String user = "neo4j";
        String password = "n304j";
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));


        try (Session session = driver.session()) {
            for (Neuron neuron : neurons) {
                try (Transaction tx = session.beginTransaction()) {

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
                    System.out.print("Added neurons.");
                }

            }



            for (BodyWithSynapses bws : bodies) {
                for (Integer postsynapticBodyId : bws.connectsTo.keySet()) {
                    try (Transaction tx = session.beginTransaction()) {

                        // have already set
                        // CREATE CONSTRAINT ON (n:Neuron) ASSERT n.bodyId IS UNIQUE
                        // CREATE INDEX ON :Neuron(bodyId)
                        //tx.run("MERGE (n:Neuron {bodyId:$bodyId1}) ON CREATE SET n.bodyId = $bodyId1, n:fib25, n:notinneurons \n" +
                        //               "MERGE (m:Neuron {bodyId:$bodyId2}) ON CREATE SET m.bodyId = $bodyId2, n:fib25, n:notinneurons \n" +
                        //                "MERGE (n)-[:ConnectsTo{weight:$weight}]->(m) \n",
                        tx.run("MATCH (n:Neuron),(m:Neuron)\n" +
                                        "WHERE n.bodyId = $bodyId1 AND m.bodyId = $bodyId2 \n" +
                                        "CREATE (n)-[:ConnectsTo{weight:$weight}]->(m)",
                                parameters("bodyId1", bws.getBodyId(),
                                        "bodyId2", postsynapticBodyId,
                                        "weight", bws.connectsTo.get(postsynapticBodyId)));

                        tx.success();
                        System.out.println("Added ConnectsTo relations.");
                    }
                    try (Transaction tx = session.beginTransaction()) {
                        tx.run("MATCH (n:Neuron {bodyId:$bodyId1} ) SET n.pre = $pre, n.post = $post",
                                parameters("bodyId1", bws.getBodyId(),
                                        "pre", bws.getPre(),
                                        "post", bws.getPost()));
                        tx.success();
                        System.out.println("Added pre and post counts.");
                    }

                }
            }


        }
        driver.close();
    }
}





