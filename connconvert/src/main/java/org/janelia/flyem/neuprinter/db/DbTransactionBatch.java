package org.janelia.flyem.neuprinter.db;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.TransactionWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collects statements into transaction batches reducing the total number of
 * database commits.
 */
public class DbTransactionBatch implements TransactionBatch {

    private final Session session;
    private final int statementsPerBatch;
    private final List<Statement> statementsToWrite;

    /**
     * Class constructor.
     *
     * @param session session in which transactions occur
     * @param statementsPerBatch number of statements per transaction batch
     */
    public DbTransactionBatch(final Session session,
                              final int statementsPerBatch) {
        this.session = session;
        this.statementsPerBatch = statementsPerBatch;
        this.statementsToWrite = new ArrayList<>(statementsPerBatch);
    }

    public void addStatement(final Statement statement) {
        statementsToWrite.add(statement);
        if (statementsToWrite.size() >= statementsPerBatch) {
            writeTransaction();
        }
    }

    public void writeTransaction() {
        // see https://neo4j.com/docs/developer-manual/current/drivers/sessions-transactions/#driver-transactions-transaction-functions
        final TransactionWork<Void> work = tx -> {
            statementsToWrite.forEach(tx::run);
            return null;
        };
        session.writeTransaction(work);

        final int statementCount = statementsToWrite.size();
        statementsToWrite.clear();

        LOG.info("writeTransaction: exit, committed {} statements", statementCount);
    }

    @Override
    public void close() {
         session.close();
    }

    private static final Logger LOG = LoggerFactory.getLogger(DbTransactionBatch.class);

}
