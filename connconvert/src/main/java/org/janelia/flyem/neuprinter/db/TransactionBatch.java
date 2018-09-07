package org.janelia.flyem.neuprinter.db;

import org.neo4j.driver.v1.Statement;

/**
 * Common interface for all transaction batch implementations.
 */
public interface TransactionBatch
        extends AutoCloseable {

    /**
     * Adds the specified statement to the current batch and
     * if the batch limit is reached, writes (commits) all previously batched statements.
     *
     * @param statement statement containing query
     */
    void addStatement(final Statement statement);

    /**
     * Writes (commits) any remaining uncommitted previously batched statements.
     */
    void writeTransaction();

    /**
     * Closes the transactional parent resource (e.g. session) if one exists.
     */
    @Override
    void close();

}
