package org.janelia.flyem.connconvert;

import org.janelia.flyem.connconvert.db.DbConfig;

import org.janelia.flyem.connconvert.db.DbTransactionBatch;
import org.janelia.flyem.connconvert.db.StdOutTransactionBatch;
import org.janelia.flyem.connconvert.db.TransactionBatch;
import org.janelia.flyem.connconvert.model.*;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jImporter implements AutoCloseable {

    private final Driver driver;
    private final int statementsPerTransaction;

    public Neo4jImporter(final DbConfig dbConfig) {

        if (dbConfig == null) {

            this.driver = null;
            this.statementsPerTransaction = 1;

        } else {

            this.driver = GraphDatabase.driver(dbConfig.getUri(),
                    AuthTokens.basic(dbConfig.getUser(),
                            dbConfig.getPassword()));
            this.statementsPerTransaction = dbConfig.getStatementsPerTransaction();

        }

    }

    @Override
    public void close() {
        driver.close();
        System.out.println("Driver closed.");
    }

    private TransactionBatch getBatch() {
        final TransactionBatch batch;
        if (driver == null) {
            batch = new StdOutTransactionBatch();
        } else {
            batch = new DbTransactionBatch(driver.session(), statementsPerTransaction);
        }
        return batch;
    }

    public void prepDatabase(String dataset) {

        LOG.info("prepDatabase: entry");

        final String[] prepTextArray = {
                "CREATE CONSTRAINT ON (n:"+ dataset +") ASSERT n.bodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (n:" + dataset +") ASSERT n.sId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:SynapseSet) ASSERT s.datasetBodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:Synapse) ASSERT s.datasetLocation IS UNIQUE",
                "CREATE CONSTRAINT ON (p:NeuronPart) ASSERT p.neuronPartId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:SkelNode) ASSERT s.skelNodeId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:Skeleton) ASSERT s.skeletonId IS UNIQUE",
                "CREATE INDEX ON :Neuron(status)",
                "CREATE INDEX ON :Synapse(x)",
                "CREATE INDEX ON :Synapse(y)",
                "CREATE INDEX ON :Synapse(z)",
                "CREATE INDEX ON :Synapse(location)"
        };

        for (final String prepText : prepTextArray) {
            try (final TransactionBatch batch = getBatch()) {
                batch.addStatement(new Statement(prepText));
                batch.writeTransaction();
            }
        }

        LOG.info("prepDatabase: exit");

    }

    //TODO: arbitrary properties
    public void addNeurons(final String dataset,
                           final List<Neuron> neuronList) {

        final String neuronText = "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) " +
                "ON CREATE SET n.bodyId = $bodyId," +
                " n.name = $name," +
                " n.type = $type," +
                " n.status = $status," +
                " n.size = $size," +
                " n.somaLocation = $somaLocation," +
                " n.somaRadius = $somaRadius \n" +
                " WITH n \n" +
                " CALL apoc.create.addLabels(id(n),$rois) YIELD node \n" +
                " RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (final Neuron neuron : neuronList) {
                batch.addStatement(
                        new Statement(neuronText,
                                parameters("bodyId", neuron.getId(),
                                        "name", neuron.getName(),
                                        "type", neuron.getNeuronType(),
                                        "status", neuron.getStatus(),
                                        "size", neuron.getSize(),
                                        "somaLocation", neuron.getSomaLocation(),
                                        "somaRadius", neuron.getSomaRadius(),
                                        "rois", neuron.getRois()))
                );
            }
            batch.writeTransaction();
        }

    }

    public void addConnectsTo(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addConnectsTo: entry");

        final String connectsToText =
                "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId1}) ON CREATE SET n.bodyId = $bodyId1 \n" +
                        "MERGE (m:Neuron:" + dataset + " {bodyId:$bodyId2}) ON CREATE SET m.bodyId = $bodyId2 \n" +
                        "MERGE (n)-[:ConnectsTo{weight:$weight}]->(m)";
        // TODO: rois added here?
        final String terminalCountText = "MATCH (n:Neuron:" + dataset + " {bodyId:$bodyId} ) SET n.pre = $pre, n.post = $post, n.sId=$sId";

        final String terminalCountTextWithoutSId = "MATCH (n:Neuron:" + dataset + " {bodyId:$bodyId} ) SET n.pre = $pre, n.post = $post";

        int sId = 0;

        try (final TransactionBatch batch = getBatch()) {
            for (final BodyWithSynapses bws : bodyList) {
                for (final Long postsynapticBodyId : bws.getConnectsTo().keySet()) {
                    batch.addStatement(
                            new Statement(connectsToText,
                                    parameters("bodyId1", bws.getBodyId(),
                                            "bodyId2", postsynapticBodyId,
                                            "weight", bws.getConnectsTo().get(postsynapticBodyId)))
                    );
                }
                if (bws.getNumberOfPostSynapses()+bws.getNumberOfPreSynapses() > 10) {
                    batch.addStatement(
                            new Statement(terminalCountText,
                                    parameters("pre", bws.getNumberOfPreSynapses(),
                                            "post", bws.getNumberOfPostSynapses(),
                                            "bodyId", bws.getBodyId(),
                                            "sId", sId))
                    );
                    sId++;
                } else {
                    batch.addStatement(
                            new Statement(terminalCountTextWithoutSId,
                                    parameters("pre", bws.getNumberOfPreSynapses(),
                                            "post", bws.getNumberOfPostSynapses(),
                                            "bodyId", bws.getBodyId()))
                    );
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addConnectsTo: exit");

    }

    public void addSynapsesWithRois(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addSynapses: entry");

        final String preSynapseText =
                "MERGE (s:Synapse:PreSyn:" + dataset + " {datasetLocation:$datasetLocation}) " +
                        " ON CREATE SET s.location=$location, " +
                        " s.datasetLocation = $datasetLocation," +
                        " s.confidence=$confidence, " +
                        " s.type=$type, " +
                        " s.x=$x, " +
                        " s.y=$y, " +
                        " s.z=$z \n" +
                        " WITH s \n" +
                        " CALL apoc.create.addLabels(id(s),$rois) YIELD node \n" +
                        " RETURN node";

        final String postSynapseText =
                "MERGE (s:Synapse:PostSyn:" + dataset + " {datasetLocation:$datasetLocation}) " +
                        " ON CREATE SET s.location=$location, " +
                        " s.datasetLocation = $datasetLocation," +
                        " s.confidence=$confidence, " +
                        " s.type=$type, " +
                        " s.x=$x, " +
                        " s.y=$y, " +
                        " s.z=$z \n" +
                        " WITH s \n" +
                        " CALL apoc.create.addLabels(id(s),$rois) YIELD node \n" +
                        " RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (final BodyWithSynapses bws : bodyList) {
                // issue with this body id in mb6
                if (bws.getBodyId() != 304654117 || !(dataset.equals("mb6v2") || dataset.equals("mb6"))) {
                    for (final Synapse synapse : bws.getSynapseSet()) {
                        if (synapse.getType().equals("pre")) {

                            batch.addStatement(new Statement(
                                    preSynapseText,
                                    parameters("location", synapse.getLocationString(),
                                            "datasetLocation", dataset + ":" + synapse.getLocationString(),
                                            "confidence", synapse.getConfidence(),
                                            "type", synapse.getType(),
                                            "x", synapse.getLocation().get(0),
                                            "y", synapse.getLocation().get(1),
                                            "z", synapse.getLocation().get(2),
                                            "rois", synapse.getRois()))
                            );
                        } else if (synapse.getType().equals("post")) {
                            batch.addStatement(new Statement(
                                    postSynapseText,
                                    parameters("location", synapse.getLocationString(),
                                            "datasetLocation", dataset + ":" + synapse.getLocationString(),
                                            "confidence", synapse.getConfidence(),
                                            "type", synapse.getType(),
                                            "x", synapse.getLocation().get(0),
                                            "y", synapse.getLocation().get(1),
                                            "z", synapse.getLocation().get(2),
                                            "rois", synapse.getRois()))
                            );

                        }
                    }
                }
            }
            batch.writeTransaction();
        }
        LOG.info("addSynapses: exit");
    }




    public void addSynapsesTo(final String dataset,  HashMap<String,List<String>> preToPost) {

        LOG.info("addSynapsesTo: entry");

        final String synapseRelationsText = "MERGE (s:Synapse:" + dataset + " {datasetLocation:$datasetPreLocation}) ON CREATE SET s.location = $prelocation, s.datasetLocation=$datasetPreLocation, s:createdforsynapsesto \n" +
                "MERGE (t:Synapse:" + dataset + " {datasetLocation:$datasetPostLocation}) ON CREATE SET t.location = $postlocation, t.datasetLocation=$datasetPostLocation, t:createdforsynapsesto \n" +
                "MERGE (s)-[:SynapsesTo]->(t)";

        try (final TransactionBatch batch = getBatch()) {
            for (String preLoc: preToPost.keySet()) {
                for (String postLoc : preToPost.get(preLoc)) {
                    batch.addStatement(new Statement(synapseRelationsText,
                        parameters("prelocation", preLoc,
                                "datasetPreLocation", dataset+ ":" +preLoc,
                                "datasetPostLocation", dataset+ ":" +postLoc,
                                "postlocation", postLoc))
                );
                }
            }
            batch.writeTransaction();
        }


        LOG.info("addSynapsesTo: exit");
    }


    public void addNeuronRois(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addNeuronRois: entry");

//        final String roiSynapseText = "MERGE (s:Synapse {datasetLocation:$datasetLocation}) ON CREATE SET s.location = $location, s.datasetLocation=$datasetLocation \n" +
//                "WITH s \n" +
//                "CALL apoc.create.addLabels(id(s),$rois) YIELD node \n" +
//                "RETURN node";

        final String roiNeuronText = "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) ON CREATE SET n.bodyId = $bodyId\n" +
                "WITH n \n" +
                "CALL apoc.create.addLabels(id(n),$rois) YIELD node \n" +
                "RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                for (Synapse synapse: bws.getSynapseSet()) {
                    List<String> roiList = synapse.getRois();
//                    batch.addStatement(new Statement(roiSynapseText,parameters("location", synapse.getLocationString(),
//                            "datasetLocation",dataset+":"+synapse.getLocationString(),
//                            "rois", roiList)));
                    batch.addStatement(new Statement(roiNeuronText,parameters("bodyId", bws.getBodyId(),
                            "rois", roiList)));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addNeuronRois: exit");


    }


    public void addNeuronParts(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addNeuronParts: entry");

        final String neuronPartText = "MERGE (n:Neuron:" + dataset +  " {bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId, n:createdforneuronpart \n"+
                "MERGE (p:NeuronPart {neuronPartId:$neuronPartId}) ON CREATE SET p.neuronPartId = $neuronPartId, p.pre=$pre, p.post=$post, p.size=$size \n"+
                "MERGE (p)-[:PartOf]->(n) \n" +
                "WITH p \n" +
                "CALL apoc.create.addLabels(id(p),[$roi, \"" + dataset + "\" ]) YIELD node \n" +
                "RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                for (NeuronPart np : bws.getNeuronParts()) {
                    String neuronPartId = dataset+":"+bws.getBodyId()+":"+np.getRoi();
                    batch.addStatement(new Statement(neuronPartText,parameters("bodyId",bws.getBodyId(),
                            "roi",np.getRoi(),
                            "neuronPartId",neuronPartId,
                            "pre",np.getPre(),
                            "post",np.getPost(),
                            "size",np.getPre()+np.getPost())));

                }
            }
            batch.writeTransaction();
        }
        LOG.info("addNeuronParts: exit");
    }



    public void addSizeId(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addSizeId: entry");

        final String sizeIdText = "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId, n:createdforsid \n" +
                "SET n.sId=$sId";

        int sId = 0;

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                if (bws.getNumberOfPostSynapses()+bws.getNumberOfPreSynapses() > 10) {
                    batch.addStatement(new Statement(sizeIdText, parameters("bodyId", bws.getBodyId(),
                            "sId", sId)));
                    sId++;
                }
            }
            batch.writeTransaction();
        }


        LOG.info("addSize: exit");
    }


    public void addSynapseSets(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addSynapseSets: entry");

        final String neuronContainsSSText = "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId \n" +
                "MERGE (s:SynapseSet:" + dataset + " {datasetBodyId:$datasetBodyId}) ON CREATE SET s.datasetBodyId=$datasetBodyId \n" +
                "MERGE (n)-[:Contains]->(s)";

        final String ssContainsSynapseText = "MERGE (s:Synapse:" + dataset + " {datasetLocation:$datasetLocation}) ON CREATE SET s.location=$location, s.datasetLocation=$datasetLocation \n"+
                "MERGE (t:SynapseSet:" + dataset + " {datasetBodyId:$datasetBodyId}) ON CREATE SET t.datasetBodyId=$datasetBodyId \n" +
                "MERGE (t)-[:Contains]->(s) \n";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                batch.addStatement(new Statement(neuronContainsSSText,parameters("bodyId",bws.getBodyId(),
                        "datasetBodyId",dataset+":"+bws.getBodyId()))
                );

                for (Synapse synapse : bws.getSynapseSet()) {
                    batch.addStatement(new Statement(ssContainsSynapseText, parameters("location", synapse.getLocationString(),
                            "datasetLocation",dataset+":"+synapse.getLocationString(),
                            "bodyId", bws.getBodyId(),
                            "datasetBodyId",dataset+":"+bws.getBodyId(),
                            "dataset",dataset)));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addSynapseSets: exit");
    }


    public void addSkeletonNodes(final String dataset, final List<Skeleton> skeletonList) {

        LOG.info("addSkeletonNodes: entry");

        final String rootNodeString =
                "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId \n" +
                "MERGE (r:Skeleton:" + dataset + " {skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId \n" +
                "MERGE (s:SkelNode:" + dataset + " {skelNodeId:$skelNodeId}) ON CREATE SET s.skelNodeId=$skelNodeId, s.location=$location, s.radius=$radius, s.x=$x, s.y=$y, s.z=$z \n" +
                "MERGE (n)-[:Contains]->(r) \n" +
                "MERGE (r)-[:Contains]->(s) \n";

        final String parentNodeString = "MERGE (p:SkelNode:" + dataset + " {skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=$pLocation, p.radius=$pRadius, p.x=$pX, p.y=$pY, p.z=$pZ \n";


        try (final TransactionBatch batch = getBatch()) {
            for (Skeleton skeleton: skeletonList) {

                Long associatedBodyId = skeleton.getAssociatedBodyId();
                List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

                for (SkelNode skelNode : skelNodeList ) {

                    if (skelNode.getParent()==null) {
                        batch.addStatement(new Statement(rootNodeString,parameters("bodyId", associatedBodyId,
                                "location",skelNode.getLocationString(),
                                "radius",skelNode.getRadius(),
                                "skeletonId", dataset+":"+associatedBodyId,
                                "skelNodeId", dataset+":"+associatedBodyId+":"+skelNode.getLocationString(),
                                "x",skelNode.getLocation().get(0),
                                "y", skelNode.getLocation().get(1),
                                "z", skelNode.getLocation().get(2)
                        )));
                    }

                        String addChildrenString = parentNodeString;

                        int childNodeCount = 1;
                        for (SkelNode child : skelNode.getChildren()) {
                            String childNodeId = dataset+":"+associatedBodyId+":"+child.getLocationString();
                            final String childNodeString = "MERGE (c" + childNodeCount + ":SkelNode:" + dataset + " {skelNodeId:\"" + childNodeId + "\"}) ON CREATE SET c" + childNodeCount +
                                    ".skelNodeId=\"" + childNodeId + "\", c" + childNodeCount + ".location=\"" + child.getLocationString() + "\", c" + childNodeCount + ".radius=" + child.getRadius() +
                                    ", c" + childNodeCount + ".x=" + child.getLocation().get(0) + ", c" + childNodeCount + ".y=" + child.getLocation().get(1) + ", c" + childNodeCount + ".z=" + child.getLocation().get(2) + " \n" +
                                    "MERGE (p)-[:LinksTo]-(c" + childNodeCount + ") \n";

                            addChildrenString = addChildrenString + childNodeString;

                            childNodeCount++;

                        }

                        batch.addStatement(new Statement(addChildrenString,parameters("parentSkelNodeId", dataset+":"+associatedBodyId+":"+skelNode.getLocationString(),
                                "pLocation", skelNode.getLocationString(),
                                "pRadius", skelNode.getRadius(),
                                "pX",skelNode.getLocation().get(0),
                                "pY",skelNode.getLocation().get(1),
                                "pZ",skelNode.getLocation().get(2)
                                )));




                }

                LOG.info("Added full skeleton for bodyId: " + skeleton.getAssociatedBodyId());
            }
            batch.writeTransaction();
        }


        LOG.info("addSkeletonNodes: exit");
    }




    private static final Logger LOG = LoggerFactory.getLogger(Neo4jImporter.class);
}