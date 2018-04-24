package connconvert;

import connconvert.db.DbConfig;

import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import static org.neo4j.driver.v1.Values.parameters;

public class Neo4jImporter implements AutoCloseable {

    private final Driver driver;
    private final int statementsPerTransaction;

    public Neo4jImporter(final DbConfig dbConfig) {

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
    public void close() throws Exception {
        driver.close();
        System.out.println("Driver closed.");
    }

    public void prepDatabase() throws Exception {
        try (Session session = driver.session()) {
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE CONSTRAINT ON (n:Neuron) ASSERT n.datasetBodyId IS UNIQUE");
                tx.success();
            }
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE CONSTRAINT ON (s:SynapseSet) ASSERT s.datasetBodyId IS UNIQUE");
                tx.success();
            }
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE INDEX ON :Neuron(bodyId)");
                tx.success();
            }
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE INDEX ON :Synapse(x)");
                tx.success();
            }
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE INDEX ON :Synapse(y)");
                tx.success();
            }
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE INDEX ON :Synapse(z)");
                tx.success();
            }
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE INDEX ON :Neuron(status)");
                tx.success();
            }
            try (Transaction tx = session.beginTransaction()) {
                tx.run("CREATE INDEX ON :Synapse(location)");
                tx.success();
            }
            try(Transaction tx= session.beginTransaction()) {
                tx.run("CREATE CONSTRAINT ON (s:Synapse) ASSERT s.datasetLocation IS UNIQUE");
                tx.success();
            }

            try(Transaction tx=session.beginTransaction()) {
                tx.run("CREATE CONSTRAINT ON (p:NeuronPart) ASSERT p.neuronPartId IS UNIQUE");
                tx.success();
            }
        }
        System.out.println("Database prepped.");
    }

}
