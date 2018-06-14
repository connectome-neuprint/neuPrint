package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;

import java.time.LocalDate;
import java.util.Set;

public class TimeStampProcedure {

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;


    @Procedure(value = "neuPrintProcedures.timeStamp", mode = Mode.WRITE)
    @Description("For the node with the given node ID, add time stamp property with current date")
    public void timeStamp(@Name("nodeId") long nodeId) {

        Node node = dbService.getNodeById(nodeId);

        node.setProperty("timeStamp", LocalDate.now());

    }

    //for testing in embedded mode or registering transaction events
    public static void timeStampEmbedded(Set<Long> nodeIdSet, GraphDatabaseService dbService) {

        for (Long nodeId : nodeIdSet) {
            Node node = dbService.getNodeById(nodeId);
            node.setProperty("timeStamp", LocalDate.now());
        }

    }

}

