package org.janelia.flyem.neuprinter.db;

import org.neo4j.driver.v1.Statement;

/**
 * Simply prints each statement to standard out (for tests or debugging).
 *
 * TODO: adapt this to print to file(s) - maybe even csv files suitable for neo4j batch import.
 */
public class StdOutTransactionBatch
        implements TransactionBatch {

    @Override
    public void addStatement(final Statement statement) {
        System.out.println(statement);
    }

    @Override
    public void writeTransaction() {
        // no-op
    }

    @Override
    public void close() {
        // no-op
    }

}
