package org.janelia.flyem.neuprintprocedures.triggers;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Set;

class TimeStampProcedure {

    static void timeStampEmbedded(Set<Node> nodeSet, GraphDatabaseService dbService) {

        Set<Node> notFoundNodes = new HashSet<>();

        for (Node node : nodeSet) {
            try {
                node.setProperty("timeStamp", LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS));
            } catch (org.neo4j.graphdb.NotFoundException nfe) {
                notFoundNodes.add(node);
            }
        }

        if (notFoundNodes.size() > 0) {
            System.out.println(LocalDateTime.now() + " The following nodes not found in database. Time stamp not applied: " +
                    notFoundNodes);
        }

    }

}

