package org.janelia.flyem.neuprint.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Neo4J database connection parameters that can be loaded from a java
 * {@link Properties} file.
 */
public class DbConfig {

    private final String uri;
    private final String user;
    private final String password;
    private final int statementsPerTransaction;
    private final int connectionInfoStatementsPerTransaction;

    /**
     * Class constructor.
     *
     * @param uri                      uri for accessing the database
     * @param user                     username for database
     * @param password                 password for database
     * @param statementsPerTransaction number of statements per database transaction
     * @param connectionInfoStatementsPerTransaction number of connection info statements per database transaction
     */
    private DbConfig(final String uri,
                     final String user,
                     final String password,
                     final int statementsPerTransaction,
                     final int connectionInfoStatementsPerTransaction) {
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.statementsPerTransaction = statementsPerTransaction;
        this.connectionInfoStatementsPerTransaction = connectionInfoStatementsPerTransaction;
    }

    /**
     * @return the uri for the database connection
     */
    public String getUri() {
        return uri;
    }

    /**
     * @return the user for the database connection
     */
    public String getUser() {
        return user;
    }

    /**
     * @return the password for the database connection
     */
    public String getPassword() {
        return password;
    }

    /**
     * @return the number of statements per database transaction
     */
    public int getStatementsPerTransaction() {
        return statementsPerTransaction;
    }

    /**
     *
     * @return the number of statements per database transaction for adding connection information (procedure call that adds ConnectsTo relationships, ConnectionSets, etc; more complicated than other statements so generally needs a smaller batch size)
     */
    public int getConnectionInfoStatementsPerTransaction() {
        return connectionInfoStatementsPerTransaction;
    }

    /**
     * Returns a DbConfig object based on a java {@link Properties} file. The
     * properties file must contain uri, username, and password properties.
     * Optionally, a statementsPerTransaction property can be used to specify
     * the number of statements per transaction. The default value is 100.
     *
     * @param file a {@link File} object representing the properties file
     * @return a {@link DbConfig} object
     * @throws IllegalArgumentException when properties file is invalid
     */
    public static DbConfig fromFile(final File file)
            throws IllegalArgumentException {

        DbConfig dbConfig;
        final Properties properties = new Properties();

        final String path = file.getAbsolutePath();

        FileInputStream in = null;
        try {
            in = new FileInputStream(file);
            properties.load(in);

            final String uri = properties.getProperty("uri");
            final String user = properties.getProperty("username");
            final String password = properties.getProperty("password");

            final String statementsPerTransactionString = properties.getProperty("statementsPerTransaction");
            final int statementsPerTransaction;
            if (statementsPerTransactionString == null) {
                statementsPerTransaction = 100;
            } else {
                try {
                    statementsPerTransaction = Integer.parseInt(statementsPerTransactionString);
                } catch (final NumberFormatException nfe) {
                    throw new IllegalArgumentException(
                            "invalid statementsPerTransaction value '" + statementsPerTransactionString +
                                    "' specified in " + file, nfe);
                }
            }

            final String connectionInfoStatementsPerTransactionString = properties.getProperty("connectionInfoStatementsPerTransaction");
            final int connectionInfoStatementsPerTransaction;
            if (connectionInfoStatementsPerTransactionString == null) {
                connectionInfoStatementsPerTransaction = Math.max(1, statementsPerTransaction/40);
            } else {
                try {
                    connectionInfoStatementsPerTransaction = Integer.parseInt(connectionInfoStatementsPerTransactionString);
                } catch (final NumberFormatException nfe) {
                    throw new IllegalArgumentException(
                            "invalid connectionInfoStatementsPerTransaction value '" + connectionInfoStatementsPerTransactionString +
                                    "' specified in " + file, nfe);
                }
            }

            if (uri == null) {
                throw new IllegalArgumentException("failed to read uri from " + file);
            }
            if (password == null) {
                throw new IllegalArgumentException("failed to read password from " + file);
            }
            if (user == null) {
                throw new IllegalArgumentException("failed to read username from " + file);
            }

            dbConfig = new DbConfig(uri, user, password, statementsPerTransaction, connectionInfoStatementsPerTransaction);

        } catch (final Exception e) {
            throw new IllegalArgumentException("failed to load properties from " + path, e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (final IOException e) {
                    LOG.warn("failed to close " + path + ", ignoring error");
                }
            }
        }

        LOG.info("loaded database configuration from {}", path);

        return dbConfig;
    }

    private static final Logger LOG = LoggerFactory.getLogger(DbConfig.class);
}
