package org.janelia.flyem.neuprintprocedures;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.time.LocalDate;
import java.util.Set;

public class TimeStampProcedure {

    public static void timeStampEmbedded(Set<Long> nodeIdSet, GraphDatabaseService dbService) {

        for (Long nodeId : nodeIdSet) {
            try {
                Node node = dbService.getNodeById(nodeId);
                node.setProperty("timeStamp", LocalDate.now());
            } catch (org.neo4j.graphdb.NotFoundException nfe) {
                System.out.println(nfe + ". Time stamp not applied.");
            }
        }

    }

}

