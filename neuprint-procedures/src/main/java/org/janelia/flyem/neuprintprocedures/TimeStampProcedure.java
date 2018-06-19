package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.time.LocalDate;
import java.util.Set;

public class TimeStampProcedure {

    //for testing in embedded mode or registering transaction events
    public static void timeStampEmbedded(Set<Long> nodeIdSet, GraphDatabaseService dbService) {

        for (Long nodeId : nodeIdSet) {
            Node node = dbService.getNodeById(nodeId);
            node.setProperty("timeStamp", LocalDate.now());
        }

    }

}

