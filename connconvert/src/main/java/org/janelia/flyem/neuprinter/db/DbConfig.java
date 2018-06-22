package org.janelia.flyem.neuprinter.db;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Neo4J database connection parameters that can be loaded from a java {@link Properties} file.
 */
public class DbConfig {

    private final String uri;
    private final String user;
    private final String password;
    private final int statementsPerTransaction;

    private DbConfig(final String uri,
                    final String user,
                    final String password,
                    final int statementsPerTransaction) {
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.statementsPerTransaction = statementsPerTransaction;
    }

    public String getUri() {
        return uri;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public int getStatementsPerTransaction() {
        return statementsPerTransaction;
    }

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

            if (uri==null) {
                throw new IllegalArgumentException("failed to read uri from " + file);
            }
            if (password==null) {
                throw new IllegalArgumentException("failed to read password from " + file);
            }
            if (user==null) {
                throw new IllegalArgumentException("failed to read password from " + file);
            }

            dbConfig = new DbConfig(uri, user, password, statementsPerTransaction);

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
