package org.janelia.flyem.neuprintreader;

import org.janelia.flyem.neuprinter.db.DbConfig;
import org.janelia.flyem.neuprinter.db.DbTransactionBatch;
import org.janelia.flyem.neuprinter.db.StdOutTransactionBatch;
import org.janelia.flyem.neuprinter.db.TransactionBatch;
import org.janelia.flyem.neuprintprocedures.SynapticConnectionVertexMap;
import org.neo4j.driver.v1.*;

public class Neo4jReader implements AutoCloseable {

    private final Driver driver;
    private final int statementsPerTransaction;

    public Neo4jReader(final DbConfig dbConfig) {

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


    public String getLineGraphVerticesJson(String dataset, String roi) {

        String lineGraphVerticesJson = null;
        try (Session session = driver.session()) {
            lineGraphVerticesJson = session.readTransaction(tx -> getLineGraphResult(tx, dataset, roi));
        }

        return lineGraphVerticesJson;
    }

//    public void getLineGraphEdgesAndVerticesJson(String dataset, String roi, String lineGraphVerticesJson) {
//
//        SynapticConnectionVertexMap synapticConnectionVertexMap = new SynapticConnectionVertexMap(lineGraphVerticesJson);
//        synapticConnectionVertexMap.writeEdgesAsJson(dataset, roi);
//        synapticConnectionVertexMap.writeVerticesAsJson(dataset, roi);
//
//    }


    private static String getLineGraphResult(final Transaction tx, final String dataset, final String roi) {
        String functionCall = "CALL analysis.getLineGraph(\"" + dataset + "\",\"" + roi + "\") YIELD value RETURN value";
        StatementResult result = tx.run(functionCall);
        return result.single().get(0).asString();
    }
}
