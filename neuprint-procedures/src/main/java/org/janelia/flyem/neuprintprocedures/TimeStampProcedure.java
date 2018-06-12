package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.time.LocalDate;

public class TimeStampProcedure {

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;


    @Procedure(value = "connprocedures.timeStamp", mode = Mode.WRITE)
    @Description("For the node with the given node-id, add properties for the provided keys to index per label")
    public void timeStamp(@Name("nodeId") long nodeId) {

        Node node = dbService.getNodeById(nodeId);

        node.setProperty("timeStamp", LocalDate.now());

    }

    //for testing in embedded mode or registering transaction events
    public static void timeStampEmbedded(long nodeId, GraphDatabaseService dbService) {

        try (Transaction tx = dbService.beginTx()) {
            Node node = dbService.getNodeById(nodeId);
            node.setProperty("timeStamp", LocalDate.now());
            tx.success();
        }

    }

}

