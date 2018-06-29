package org.janelia.flyem.neuprinter;

import org.janelia.flyem.neuprinter.db.DbConfig;

import org.janelia.flyem.neuprinter.db.DbTransactionBatch;
import org.janelia.flyem.neuprinter.db.StdOutTransactionBatch;
import org.janelia.flyem.neuprinter.db.TransactionBatch;
import org.janelia.flyem.neuprinter.model.*;
import org.neo4j.driver.v1.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.time.LocalDate;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jImporter implements AutoCloseable {

    private final Driver driver;
    private final int statementsPerTransaction;
    private final LocalDate timeStamp = LocalDate.now();

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

    //for testing
    public Neo4jImporter(final Driver driver) {
        this.driver = driver;
        this.statementsPerTransaction = 20;
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

    //TODO: convert location to spatial point values
    public void prepDatabase(String dataset) {

        LOG.info("prepDatabase: entry");

        final String[] prepTextArray = {
                "CREATE CONSTRAINT ON (n:" + dataset + ") ASSERT n.bodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (n:" + dataset + ") ASSERT n.sId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:SynapseSet) ASSERT s.datasetBodyId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:Synapse) ASSERT s.datasetLocation IS UNIQUE",
                "CREATE CONSTRAINT ON (p:NeuronPart) ASSERT p.neuronPartId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:SkelNode) ASSERT s.skelNodeId IS UNIQUE",
                "CREATE CONSTRAINT ON (s:Skeleton) ASSERT s.skeletonId IS UNIQUE",
                "CREATE CONSTRAINT ON (c:NeuronClass) ASSERT c.neuronClassId IS UNIQUE",
                "CREATE CONSTRAINT ON (t:NeuronType) ASSERT t.neuronTypeId IS UNIQUE",
                "CREATE CONSTRAINT ON (m:Meta) ASSERT m.dataset IS UNIQUE",
                "CREATE CONSTRAINT ON (n:Neuron) ASSERT n.autoName is UNIQUE",
                "CREATE INDEX ON :Neuron(status)",
                "CREATE INDEX ON :Neuron(name)",
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
                " n.somaRadius = $somaRadius, " +
                " n.timeStamp = $timeStamp \n" +
                " WITH n \n" +
                " CALL apoc.create.addLabels(id(n),$rois) YIELD node \n" +
                " RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (final Neuron neuron : neuronList) {
                String status = neuron.getStatus() != null ? neuron.getStatus() : "not annotated";
                batch.addStatement(
                        new Statement(neuronText,
                                parameters("bodyId", neuron.getId(),
                                        "name", neuron.getName(),
                                        "type", neuron.getNeuronType(),
                                        "status", status,
                                        "size", neuron.getSize(),
                                        "somaLocation", neuron.getSomaLocation(),
                                        "somaRadius", neuron.getSomaRadius(),
                                        "timeStamp", timeStamp,
                                        "rois", neuron.getRois()))
                );
            }
            batch.writeTransaction();
        }

    }

    public void addConnectsTo(final String dataset, final List<BodyWithSynapses> bodyList, Integer bigThreshold) {

        LOG.info("addConnectsTo: entry");

        final String connectsToText =
                "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId1}) ON CREATE SET n.bodyId = $bodyId1, n.status=$notAnnotated\n" +
                        "MERGE (m:Neuron:" + dataset + " {bodyId:$bodyId2}) ON CREATE SET m.bodyId = $bodyId2, m.timeStamp=$timeStamp, m.status=$notAnnotated \n" +
                        "MERGE (n)-[:ConnectsTo{weight:$weight}]->(m)";
        final String terminalCountText = "MATCH (n:Neuron:" + dataset + " {bodyId:$bodyId} ) SET n.pre = $pre, n.post = $post, n.sId=$sId, n:Big, n.timeStamp=$timeStamp";

        final String terminalCountTextWithoutSId = "MATCH (n:Neuron:" + dataset + " {bodyId:$bodyId} ) SET n.pre = $pre, n.post = $post, n.timeStamp=$timeStamp";

        int sId = 0;

        try (final TransactionBatch batch = getBatch()) {
            for (final BodyWithSynapses bws : bodyList) {
                for (final Long postsynapticBodyId : bws.getConnectsTo().keySet()) {
                    batch.addStatement(
                            new Statement(connectsToText,
                                    parameters("bodyId1", bws.getBodyId(),
                                            "bodyId2", postsynapticBodyId,
                                            "timeStamp", timeStamp,
                                            "notAnnotated", "not annotated",
                                            "weight", bws.getConnectsTo().get(postsynapticBodyId)))
                    );
                }
                if (bws.getNumberOfPostSynapses() + bws.getNumberOfPreSynapses() > bigThreshold) {
                    batch.addStatement(
                            new Statement(terminalCountText,
                                    parameters("pre", bws.getNumberOfPreSynapses(),
                                            "post", bws.getNumberOfPostSynapses(),
                                            "bodyId", bws.getBodyId(),
                                            "sId", sId,
                                            "timeStamp", timeStamp))
                    );
                    sId++;
                } else {
                    batch.addStatement(
                            new Statement(terminalCountTextWithoutSId,
                                    parameters("pre", bws.getNumberOfPreSynapses(),
                                            "post", bws.getNumberOfPostSynapses(),
                                            "bodyId", bws.getBodyId(),
                                            "timeStamp", timeStamp))
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
                        " s.z=$z, " +
                        " s.timeStamp=$timeStamp \n" +
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
                        " s.z=$z, " +
                        " s.timeStamp=$timeStamp \n" +
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
                                            "timeStamp", timeStamp,
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
                                            "timeStamp", timeStamp,
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


    public void addSynapsesTo(final String dataset, HashMap<String, List<String>> preToPost) {

        LOG.info("addSynapsesTo: entry");

        final String synapseRelationsText = "MERGE (s:Synapse:" + dataset + " {datasetLocation:$datasetPreLocation}) ON CREATE SET s.location = $prelocation, s.datasetLocation=$datasetPreLocation, s:createdforsynapsesto, s.timeStamp=$timeStamp \n" +
                "MERGE (t:Synapse:" + dataset + " {datasetLocation:$datasetPostLocation}) ON CREATE SET t.location = $postlocation, t.datasetLocation=$datasetPostLocation, t:createdforsynapsesto, t.timeStamp=$timeStamp \n" +
                "MERGE (s)-[:SynapsesTo]->(t)";

        try (final TransactionBatch batch = getBatch()) {
            for (String preLoc : preToPost.keySet()) {
                for (String postLoc : preToPost.get(preLoc)) {
                    batch.addStatement(new Statement(synapseRelationsText,
                            parameters("prelocation", preLoc,
                                    "datasetPreLocation", dataset + ":" + preLoc,
                                    "datasetPostLocation", dataset + ":" + postLoc,
                                    "timeStamp", timeStamp,
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

        final String roiNeuronText = "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) ON CREATE SET n.bodyId = $bodyId, n.timeStamp=$timeStamp, n.status=$notAnnotated \n" +
                "WITH n \n" +
                "CALL apoc.create.addLabels(id(n),$rois) YIELD node \n" +
                "RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                for (Synapse synapse : bws.getSynapseSet()) {
                    List<String> roiList = synapse.getRois();
//                    batch.addStatement(new Statement(roiSynapseText,parameters("location", synapse.getLocationString(),
//                            "datasetLocation",dataset+":"+synapse.getLocationString(),
//                            "rois", roiList)));
                    batch.addStatement(new Statement(roiNeuronText, parameters("bodyId", bws.getBodyId(),
                            "timeStamp", timeStamp,
                            "notAnnotated", "not annotated",
                            "rois", roiList)));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addNeuronRois: exit");


    }


    public void addNeuronParts(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addNeuronParts: entry");

        final String neuronPartText = "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId, n:createdforneuronpart, n.timeStamp=$timeStamp, n.status=$notAnnotated \n" +
                "MERGE (p:NeuronPart {neuronPartId:$neuronPartId}) ON CREATE SET p.neuronPartId = $neuronPartId, p.pre=$pre, p.post=$post, p.size=$size, p.timeStamp=$timeStamp \n" +
                "MERGE (p)-[:PartOf]->(n) \n" +
                "WITH p \n" +
                "CALL apoc.create.addLabels(id(p),[$roi, \"" + dataset + "\" ]) YIELD node \n" +
                "RETURN node";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                for (NeuronPart np : bws.getNeuronParts()) {
                    String neuronPartId = dataset + ":" + bws.getBodyId() + ":" + np.getRoi();
                    batch.addStatement(new Statement(neuronPartText, parameters("bodyId", bws.getBodyId(),
                            "roi", np.getRoi(),
                            "neuronPartId", neuronPartId,
                            "pre", np.getPre(),
                            "post", np.getPost(),
                            "size", np.getPre() + np.getPost(),
                            "notAnnotated", "not annotated",
                            "timeStamp", timeStamp)));

                }
            }
            batch.writeTransaction();
        }
        LOG.info("addNeuronParts: exit");
    }

    public void addAutoNames(final String dataset) throws Exception {

        LOG.info("addAutoNames: entry");

        List<AutoName> autoNameList = new ArrayList<>();
        List<Long> bodyIdsWithoutNames;
        final String autoNameText = "MATCH (n:Neuron:" + dataset + "{bodyId:$bodyId}) SET n.autoName=$autoName";
        final String autoNameToNameText = "MATCH (n:Neuron:" + dataset + "{bodyId:$bodyId}) SET n.autoName=$autoName, n.name=$autoName ";

        //autonames only added to :Big neurons
        //get the associated neuron part with the largest post. = roi1
        //get the associated neuron part with the largest pre. = roi2
        //unique four digit number (e.g. 0004)
        //FB-LAL-0001, AL-CA-0023

        //if there is no existing name, give it the autoname as name.
        try (Session session = driver.session()) {
            List<Long> bodyIdList = session.readTransaction(tx -> getAllBigNeuronBodyIds(tx, dataset));
            for (Long bodyId : bodyIdList) {
                String maxPostRoiName = session.readTransaction(tx -> getMaxInputRoi(tx, dataset, bodyId));
                String maxPreRoiName = session.readTransaction(tx -> getMaxOutputRoi(tx, dataset, bodyId));
                System.out.println("body id : " + bodyId + " max output roi: " + maxPostRoiName + " max input roi: " + maxPreRoiName);
                AutoName autoName = new AutoName(maxPostRoiName,maxPreRoiName,bodyId);
                autoNameList.add(autoName);
            }
            bodyIdsWithoutNames = session.readTransaction(tx -> getAllBigNeuronBodyIdsWithoutNames(tx,dataset));
        }

        try (final TransactionBatch batch = getBatch()) {
            for (AutoName autoName : autoNameList) {
                Long bodyId = autoName.getBodyId();
                if (bodyIdsWithoutNames.contains(bodyId)) {
                    batch.addStatement(new Statement(autoNameToNameText,
                            parameters("bodyId", autoName.getBodyId(),
                                    "autoName", autoName.getAutoName())));
                } else {
                    batch.addStatement(new Statement(autoNameText,
                            parameters("bodyId", autoName.getBodyId(),
                                    "autoName", autoName.getAutoName())));
                }
            }
            batch.writeTransaction();
        }





        LOG.info("addAutoNames: exit");


    }


    public void addSizeId(final String dataset, final List<BodyWithSynapses> bodyList) {

        LOG.info("addSizeId: entry");

        final String sizeIdText = "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId, n:createdforsid \n" +
                "SET n.sId=$sId";

        int sId = 0;

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                if (bws.getNumberOfPostSynapses() + bws.getNumberOfPreSynapses() > 10) {
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

        final String neuronContainsSSText = "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId, n.status=$notAnnotated \n" +
                "MERGE (s:SynapseSet:" + dataset + " {datasetBodyId:$datasetBodyId}) ON CREATE SET s.datasetBodyId=$datasetBodyId, s.timeStamp=$timeStamp \n" +
                "MERGE (n)-[:Contains]->(s)";

        final String ssContainsSynapseText = "MERGE (s:Synapse:" + dataset + " {datasetLocation:$datasetLocation}) ON CREATE SET s.location=$location, s.datasetLocation=$datasetLocation \n" +
                "MERGE (t:SynapseSet:" + dataset + " {datasetBodyId:$datasetBodyId}) ON CREATE SET t.datasetBodyId=$datasetBodyId \n" +
                "MERGE (t)-[:Contains]->(s) \n";

        try (final TransactionBatch batch = getBatch()) {
            for (BodyWithSynapses bws : bodyList) {
                batch.addStatement(new Statement(neuronContainsSSText, parameters("bodyId", bws.getBodyId(),
                        "notAnnotated", "not annotated",
                        "datasetBodyId", dataset + ":" + bws.getBodyId(),
                        "timeStamp", timeStamp))
                );

                for (Synapse synapse : bws.getSynapseSet()) {
                    batch.addStatement(new Statement(ssContainsSynapseText, parameters("location", synapse.getLocationString(),
                            "datasetLocation", dataset + ":" + synapse.getLocationString(),
                            "bodyId", bws.getBodyId(),
                            "datasetBodyId", dataset + ":" + bws.getBodyId(),
                            "dataset", dataset)));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("addSynapseSets: exit");
    }


    public void addSkeletonNodesOld(final String dataset, final List<Skeleton> skeletonList) {

        LOG.info("addSkeletonNodesOld: entry");

        final String rootNodeString =
                "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId \n" +
                        "MERGE (r:Skeleton:" + dataset + " {skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId, r.timeStamp=$timeStamp \n" +
                        "MERGE (s:SkelNode:" + dataset + " {skelNodeId:$skelNodeId}) ON CREATE SET s.skelNodeId=$skelNodeId, s.location=$location, s.radius=$radius, s.x=$x, s.y=$y, s.z=$z, s.rowNumber=$rowNumber \n" +
                        "MERGE (n)-[:Contains]->(r) \n" +
                        "MERGE (r)-[:Contains]->(s) \n";

        final String parentNodeString = "MERGE (r:Skeleton:" + dataset + " {skeletonId:$skeletonId}) ON CREATE SET r.timeStamp=$timeStamp \n" +
                "MERGE (p:SkelNode:" + dataset + " {skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=$pLocation, p.radius=$pRadius, p.x=$pX, p.y=$pY, p.z=$pZ, p.rowNumber=$pRowNumber \n" +
                "MERGE (r)-[:Contains]->(p) ";


        try (final TransactionBatch batch = getBatch()) {
            for (Skeleton skeleton : skeletonList) {

                Long associatedBodyId = skeleton.getAssociatedBodyId();
                List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

                for (SkelNode skelNode : skelNodeList) {

                    if (skelNode.getParent() == null) {
                        batch.addStatement(new Statement(rootNodeString, parameters("bodyId", associatedBodyId,
                                "location", skelNode.getLocationString(),
                                "radius", skelNode.getRadius(),
                                "skeletonId", dataset + ":" + associatedBodyId,
                                "skelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString(),
                                "x", skelNode.getLocation().get(0),
                                "y", skelNode.getLocation().get(1),
                                "z", skelNode.getLocation().get(2),
                                "rowNumber", skelNode.getRowNumber(),
                                "timeStamp", timeStamp
                        )));
                    }

                    String addChildrenString = parentNodeString;

                    int childNodeCount = 1;
                    for (SkelNode child : skelNode.getChildren()) {
                        String childNodeId = dataset + ":" + associatedBodyId + ":" + child.getLocationString();
                        final String childNodeString = "MERGE (c" + childNodeCount + ":SkelNode:" + dataset + " {skelNodeId:\"" + childNodeId + "\"}) ON CREATE SET c" + childNodeCount +
                                ".skelNodeId=\"" + childNodeId + "\", c" + childNodeCount + ".location=\"" + child.getLocationString() + "\", c" + childNodeCount + ".radius=" + child.getRadius() +
                                ", c" + childNodeCount + ".x=" + child.getLocation().get(0) + ", c" + childNodeCount + ".y=" + child.getLocation().get(1) + ", c" + childNodeCount + ".z=" + child.getLocation().get(2) +
                                ", c" + childNodeCount + ".rowNumber=" + child.getRowNumber() + " \n" +
                                "MERGE (p)-[:LinksTo]-(c" + childNodeCount + ") \n";

                        addChildrenString = addChildrenString + childNodeString;

                        childNodeCount++;

                    }

                    batch.addStatement(new Statement(addChildrenString, parameters("parentSkelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString(),
                            "skeletonId", dataset + ":" + associatedBodyId,
                            "pLocation", skelNode.getLocationString(),
                            "pRadius", skelNode.getRadius(),
                            "pX", skelNode.getLocation().get(0),
                            "pY", skelNode.getLocation().get(1),
                            "pZ", skelNode.getLocation().get(2),
                            "pRowNumber", skelNode.getRowNumber(),
                            "timeStamp", timeStamp
                    )));


                }

                LOG.info("Added full skeleton for bodyId: " + skeleton.getAssociatedBodyId());
            }
            batch.writeTransaction();
        }


        LOG.info("addSkeletonNodesOld: exit");
    }

    public void addSkeletonNodes(final String dataset, final List<Skeleton> skeletonList) {

        LOG.info("addSkeletonNodes: entry");

        final String neuronToSkeletonConnectionString = "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) ON CREATE SET n.bodyId=$bodyId, n.status=$notAnnotated \n" +
                "MERGE (r:Skeleton:" + dataset + " {skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId, r.timeStamp=$timeStamp \n" +
                "MERGE (n)-[:Contains]->(r) \n";

        final String rootNodeString = "MERGE (r:Skeleton:" + dataset + " {skeletonId:$skeletonId}) ON CREATE SET r.skeletonId=$skeletonId, r.timeStamp=$timeStamp \n" +
                "MERGE (s:SkelNode:" + dataset + " {skelNodeId:$skelNodeId}) ON CREATE SET s.skelNodeId=$skelNodeId, s.location=$location, s.radius=$radius, s.x=$x, s.y=$y, s.z=$z, s.rowNumber=$rowNumber \n" +
                "MERGE (r)-[:Contains]->(s) \n";

        final String parentNodeString = "MERGE (r:Skeleton:" + dataset + " {skeletonId:$skeletonId}) ON CREATE SET r.timeStamp=$timeStamp \n" +
                "MERGE (p:SkelNode:" + dataset + " {skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=$pLocation, p.radius=$pRadius, p.x=$pX, p.y=$pY, p.z=$pZ, p.rowNumber=$pRowNumber \n" +
                "MERGE (r)-[:Contains]->(p) ";

        final String childNodeString = "MERGE (p:SkelNode:" + dataset + " {skelNodeId:$parentSkelNodeId}) ON CREATE SET p.skelNodeId=$parentSkelNodeId, p.location=$pLocation, p.radius=$pRadius, p.x=$pX, p.y=$pY, p.z=$pZ, p.rowNumber=$pRowNumber \n" +
                "MERGE (c:SkelNode:" + dataset + " {skelNodeId:$childNodeId}) ON CREATE SET c.skelNodeId=$childNodeId, c.location=$childLocation, c.radius=$childRadius, c.x=$childX, c.y=$childY, c.z=$childZ, c.rowNumber=$childRowNumber \n" +
                "MERGE (p)-[:LinksTo]-(c)";


        try (final TransactionBatch batch = getBatch()) {
            for (Skeleton skeleton : skeletonList) {

                Long associatedBodyId = skeleton.getAssociatedBodyId();
                List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

                batch.addStatement(new Statement(neuronToSkeletonConnectionString, parameters("bodyId", associatedBodyId,
                        "notAnnotated", "not annotated",
                        "skeletonId", dataset + ":" + associatedBodyId,
                        "timeStamp", timeStamp
                )));

                for (SkelNode skelNode : skelNodeList) {

                    if (skelNode.getParent() == null) {
                        batch.addStatement(new Statement(rootNodeString, parameters(
                                "location", skelNode.getLocationString(),
                                "radius", skelNode.getRadius(),
                                "skeletonId", dataset + ":" + associatedBodyId,
                                "skelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString(),
                                "x", skelNode.getLocation().get(0),
                                "y", skelNode.getLocation().get(1),
                                "z", skelNode.getLocation().get(2),
                                "rowNumber", skelNode.getRowNumber(),
                                "timeStamp", timeStamp
                        )));
                    }

                    batch.addStatement(new Statement(parentNodeString, parameters(
                            "pLocation", skelNode.getLocationString(),
                            "pRadius", skelNode.getRadius(),
                            "skeletonId", dataset + ":" + associatedBodyId,
                            "parentSkelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString(),
                            "pX", skelNode.getLocation().get(0),
                            "pY", skelNode.getLocation().get(1),
                            "pZ", skelNode.getLocation().get(2),
                            "pRowNumber", skelNode.getRowNumber(),
                            "timeStamp", timeStamp
                    )));

                    for (SkelNode child : skelNode.getChildren()) {
                        String childNodeId = dataset + ":" + associatedBodyId + ":" + child.getLocationString();
                        batch.addStatement(new Statement(childNodeString, parameters("parentSkelNodeId", dataset + ":" + associatedBodyId + ":" + skelNode.getLocationString(),
                                "skeletonId", dataset + ":" + associatedBodyId,
                                "pLocation", skelNode.getLocationString(),
                                "pRadius", skelNode.getRadius(),
                                "pX", skelNode.getLocation().get(0),
                                "pY", skelNode.getLocation().get(1),
                                "pZ", skelNode.getLocation().get(2),
                                "pRowNumber", skelNode.getRowNumber(),
                                "timeStamp", timeStamp,
                                "childNodeId", childNodeId,
                                "childLocation", child.getLocationString(),
                                "childRadius", child.getRadius(),
                                "childX", child.getLocation().get(0),
                                "childY", child.getLocation().get(1),
                                "childZ", child.getLocation().get(2),
                                "childRowNumber", child.getRowNumber()
                        )));


                    }


                }

                LOG.info("Added full skeleton for bodyId: " + skeleton.getAssociatedBodyId());
            }
            batch.writeTransaction();
        }


        LOG.info("addSkeletonNodes: exit");
    }


    public void addCellTypeTree(final String dataset, final HashMap<String, NeuronTypeTree> neuronTypeTreeMap) {

        LOG.info("addCellTypeTree: enter");


        final String cellTypeTreeString = "MERGE (nc:NeuronClass:" + dataset + " {neuronClassId:$neuronClassId}) ON CREATE SET nc.neuronClassId=$neuronClassId, nc.neuronClass=$neuronClass, nc.timeStamp=$timeStamp \n" +
                "MERGE (nt:NeuronType:" + dataset + "{neuronTypeId:$neuronTypeId}) ON CREATE SET nt.neuronTypeId=$neuronTypeId, nt.neuronType=$neuronType, nt.description=$description, nt.putativeNeurotransmitter=$neurotransmitter, nt.timeStamp=$timeStamp \n" +
                "MERGE (nc)-[:Contains]->(nt)";

        try (final TransactionBatch batch = getBatch()) {

            for (String neuronClass : neuronTypeTreeMap.keySet()) {
                for (NeuronType neuronType : neuronTypeTreeMap.get(neuronClass).getNeuronTypeList()) {

                    batch.addStatement(new Statement(cellTypeTreeString, parameters("neuronClassId", dataset + neuronClass,
                            "neuronClass", neuronClass,
                            "neuronType", neuronType.getCellType(),
                            "neuronTypeId", dataset + neuronType.getCellType(),
                            "description", neuronType.getCellDescription(),
                            "neurotransmitter", neuronType.getPutativeTransmitter(),
                            "timeStamp", timeStamp
                    )));
                }
            }
            batch.writeTransaction();
        }


        LOG.info("addCellTypeTree: exit");

    }


    public void createMetaNode(final String dataset) {

        LOG.info("createMetaNode: enter");

        final String metaNodeString = "MERGE (m:Meta:" + dataset + " {dataset:$dataset}) ON CREATE SET m.lastDatabaseEdit=$timeStamp," +
                "m.dataset=$dataset, m.totalPreCount=$totalPre, m.totalPostCount=$totalPost";


        long totalPre;
        long totalPost;
        List<String> roiNameList;
        Set<Roi> roiSet = new HashSet<>();

        try (Session session = driver.session()) {
            totalPre = session.readTransaction(tx -> getTotalPreCount(tx, dataset));
            totalPost = session.readTransaction(tx -> getTotalPostCount(tx, dataset));
            roiNameList = session.readTransaction(tx -> getAllRois(tx, dataset));
            for (String roi : roiNameList) {
                if (!roi.equals("NeuronPart") && !roi.equals(dataset)) {
                    long roiPreCount = session.readTransaction(tx -> getRoiPreCount(tx, dataset, roi));
                    long roiPostCount = session.readTransaction(tx -> getRoiPostCount(tx, dataset, roi));
                    roiSet.add(new Roi(roi,roiPreCount,roiPostCount));
                }
            }
        }

        try (final TransactionBatch batch = getBatch()) {
            batch.addStatement(new Statement(metaNodeString, parameters("dataset", dataset,
                    "totalPre", totalPre,
                    "totalPost", totalPost,
                    "timeStamp", timeStamp
            )));

            for (Roi roi : roiSet) {

                String metaNodeRoiString = "MATCH (m:Meta:" + dataset + " {dataset:$dataset}) SET m." + roi.getRoiName() +
                        "PreCount=$roiPreCount, m." + roi.getRoiName() + "PostCount=$roiPostCount ";

                batch.addStatement(new Statement(metaNodeRoiString,
                        parameters("dataset",dataset,
                                "roiPreCount", roi.getPreCount(),
                                "roiPostCount", roi.getPostCount())));
            }

            batch.writeTransaction();

        }

        LOG.info("createMetaNode: exit");

    }


    private static long getTotalPreCount(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:Neuron:" + dataset + ") RETURN sum(n.pre)");
        return result.single().get(0).asLong();
    }

    private static long getTotalPostCount(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:Neuron:" + dataset + ") RETURN sum(n.post)");
        return result.single().get(0).asLong();
    }

    private static long getRoiPreCount(final Transaction tx, final String dataset, final String roi) {
        StatementResult result = tx.run("MATCH (n:NeuronPart:" + dataset + ":" + roi + ") RETURN sum(n.pre)");
        return result.single().get(0).asLong();
    }

    private static long getRoiPostCount(final Transaction tx, final String dataset, final String roi) {
        StatementResult result = tx.run("MATCH (n:NeuronPart:" + dataset + ":" + roi + ") RETURN sum(n.post)");
        return result.single().get(0).asLong();
    }

    private static List<String> getAllRois(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:NeuronPart:" + dataset + ") WITH distinct labels(n) AS labels UNWIND labels AS label RETURN DISTINCT label");
        List<String> roiList = new ArrayList<>();
        while (result.hasNext()) {
            roiList.add(result.next().asMap().get("label").toString());
        }
        return roiList;
    }

    private static String getMaxInputRoi(final Transaction tx, final String dataset, Long bodyId){
        TreeSet<String> maxPostLabelList = new TreeSet<>();
        StatementResult result = tx.run("MATCH (n:Neuron:" + dataset + "{bodyId:$bodyId})<-[:PartOf]-(np:NeuronPart) WITH max(np.post) AS maxPost " +
                "MATCH (n:Neuron:" + dataset + "{bodyId:$bodyId})<-[:PartOf]-(np:NeuronPart) WHERE np.post=maxPost WITH DISTINCT labels(np) AS labels " +
                "UNWIND labels AS label RETURN DISTINCT label",parameters("bodyId",bodyId));
        while (result.hasNext()) {
            String roiName = result.next().asMap().get("label").toString();
            if (!roiName.equals("NeuronPart") && !roiName.equals(dataset)) {
                maxPostLabelList.add(roiName);
            }
        }

        try {
            return maxPostLabelList.first();
        } catch (NoSuchElementException nse) {
            System.out.println(("No max input roi found for " + bodyId));
            return null;
        }
    }

    private static String getMaxOutputRoi(final Transaction tx, final String dataset, Long bodyId){
        TreeSet<String> maxPreLabelList = new TreeSet<>();
        StatementResult result = tx.run("MATCH (n:Neuron:" + dataset + "{bodyId:$bodyId})<-[:PartOf]-(np:NeuronPart) WITH max(np.pre) AS maxPre " +
                "MATCH (n:Neuron:" + dataset + "{bodyId:$bodyId})<-[:PartOf]-(np:NeuronPart) WHERE np.pre=maxPre WITH DISTINCT labels(np) AS labels " +
                "UNWIND labels AS label RETURN DISTINCT label",parameters("bodyId",bodyId));
        while (result.hasNext()) {
            String roiName = result.next().asMap().get("label").toString();
            if (!roiName.equals("NeuronPart") && !roiName.equals(dataset)) {
                maxPreLabelList.add(roiName);
            }
        }

        try {
            return maxPreLabelList.first();
        } catch (NoSuchElementException nse) {
            System.out.println(("No max output roi found for " + bodyId));
            return null;
        }
    }

    private static List<Long> getAllBigNeuronBodyIds(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:Neuron:Big:" + dataset + ") RETURN n.bodyId ");
        List<Long> bodyIdList = new ArrayList<>();
        while (result.hasNext()) {
            bodyIdList.add((Long) result.next().asMap().get("n.bodyId"));
        }
        return bodyIdList;
    }

    private static List<Long> getAllBigNeuronBodyIdsWithoutNames(final Transaction tx, final String dataset) {
        StatementResult result = tx.run("MATCH (n:Neuron:Big:" + dataset + ") WHERE NOT exists(n.name) RETURN n.bodyId ");
        List<Long> bodyIdList = new ArrayList<>();
        while (result.hasNext()) {
            bodyIdList.add((Long) result.next().asMap().get("n.bodyId"));
        }
        return bodyIdList;
    }




    private static final Logger LOG = LoggerFactory.getLogger(Neo4jImporter.class);
}