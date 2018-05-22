package org.janelia.flyem.connconvert;

import org.janelia.flyem.connconvert.db.DbConfig;
import org.janelia.flyem.connconvert.db.DbTransactionBatch;
import org.janelia.flyem.connconvert.db.StdOutTransactionBatch;
import org.janelia.flyem.connconvert.db.TransactionBatch;
import org.janelia.flyem.connconvert.model.Neuron;
import org.janelia.flyem.connconvert.model.SkelNode;
import org.janelia.flyem.connconvert.model.Skeleton;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jEditor implements AutoCloseable {

    private final Driver driver;
    private final int statementsPerTransaction;

    public Neo4jEditor(final DbConfig dbConfig) {

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

    public void updateSkelNodesRowNumber (final String dataset, final List<Skeleton> skeletonList) {

        LOG.info("updateSkelNodes: entry");

        final String skelMergeString =
                "MERGE (s:SkelNode:" + dataset + " {skelNodeId:$skelNodeId}) ON MATCH SET s.rowNumber=$rowNumber";

        try (final TransactionBatch batch = getBatch()) {
            for(Skeleton skeleton : skeletonList) {

                Long associatedBodyId = skeleton.getAssociatedBodyId();
                List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

                for (SkelNode skelNode : skelNodeList) {
                    batch.addStatement(new Statement(skelMergeString, parameters("rowNumber", skelNode.getRowNumber(),
                            "skelNodeId", dataset+":"+associatedBodyId+":"+skelNode.getLocationString())));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("updateSkelNodes: exit");

    }


    public void linkAllSkelNodesToSkeleton (final String dataset, final List<Skeleton> skeletonList) {

        LOG.info("linkAllSkelNodesToSkeleton : entry");

        final String skelToSkeletonLinkString =
                "MERGE (r:Skeleton:" + dataset + " {skeletonId:$skeletonId}) \n" +
                "MERGE (s:SkelNode:" + dataset + " {skelNodeId:$skelNodeId}) \n" +
                        "MERGE (r)-[:Contains]->(s) ";

        try (final TransactionBatch batch = getBatch()) {
            for(Skeleton skeleton : skeletonList) {

                Long associatedBodyId = skeleton.getAssociatedBodyId();
                List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

                for (SkelNode skelNode : skelNodeList) {
                    batch.addStatement(new Statement(skelToSkeletonLinkString, parameters("skeletonId", dataset+":"+associatedBodyId,
                            "skelNodeId", dataset+":"+associatedBodyId+":"+skelNode.getLocationString())));
                }
            }
            batch.writeTransaction();
        }

        LOG.info("linkAllSkelNodesToSkeleton : exit");

    }



    public void updateNeuronProperties (final String dataset, final List<Neuron> neuronList) {

        LOG.info("updateNeuronProperties : entry");


        final String neuronText = "MERGE (n:Neuron:" + dataset + " {bodyId:$bodyId}) " +
                "ON MATCH SET n.name = $name," +
                " n.type = $type," +
                " n.status = $status," +
                " n.size = $size," +
                " n.somaLocation = $somaLocation," +
                " n.somaRadius = $somaRadius \n" +
                "ON CREATE SET n.bodyId = $bodyId," +
                " n.name = $name," +
                " n.type = $type," +
                " n.status = $status," +
                " n.size = $size," +
                " n.somaLocation = $somaLocation," +
                " n.somaRadius = $somaRadius \n";

        try (final TransactionBatch batch = getBatch()) {
            for (final Neuron neuron : neuronList) {
                if (neuron.getRois() != null) {
                    LOG.error("Found neuron with rois listed. bodyId: " + neuron.getId());
                }
                batch.addStatement(
                        new Statement(neuronText,
                                parameters("bodyId", neuron.getId(),
                                        "name", neuron.getName(),
                                        "type", neuron.getNeuronType(),
                                        "status", neuron.getStatus(),
                                        "size", neuron.getSize(),
                                        "somaLocation", neuron.getSomaLocation(),
                                        "somaRadius", neuron.getSomaRadius()))
                );
            }
            batch.writeTransaction();
        }

        LOG.info("updateNeuronProperties : exit");


    }



    private static final Logger LOG = LoggerFactory.getLogger(Neo4jEditor.class);

}
