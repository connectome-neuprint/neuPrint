package org.janelia.flyem.neuprintreader;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.janelia.flyem.neuprinter.db.DbConfig;
import org.janelia.flyem.neuprinter.json.JsonUtils;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;



public class ConnReader {


    @Parameters(separators = "=")
    public static class ConverterParameters {

        @Parameter(
                names = "--dbProperties",
                description = "Properties file containing database information (required)",
                required = true)
        public String dbProperties;

        @Parameter(
                names = "--datasetLabel",
                description = "Dataset value for all nodes (required)",
                required = true)
        public String datasetLabel;

        @Parameter(
                names = "--roi",
                description = "Roi to query (required)",
                required = true)
        public String roi;

        @Parameter(
                names = "--help",
                help = true)
        public boolean help;

        public DbConfig getDbConfig() {
            return (dbProperties == null) ? null : DbConfig.fromFile(new File(dbProperties));
        }

        @Override
        public String toString() {
            return JsonUtils.GSON.toJson(this);
        }
    }

    public static String dataset;
    public static String roi;


    public static void main(String[] args) {
        final ConverterParameters parameters = new ConverterParameters();
        final JCommander jCommander = new JCommander(parameters);
        jCommander.setProgramName("java -cp neuprint-reader.jar " + ConnReader.class.getName());


        boolean parseFailed = true;
        try {
            jCommander.parse(args);
            parseFailed = false;
        } catch (final ParameterException pe) {
            JCommander.getConsole().println("\nERROR: failed to parse command line arguments\n\n" + pe.getMessage());
        } catch (final Throwable t) {
            LOG.log(Level.INFO, "failed to parse command line arguments", t);
        }

        if (parameters.help || parseFailed) {
            JCommander.getConsole().println("");
            jCommander.usage();
            System.exit(1);
        }

        LOG.info("running with parameters: " + parameters);

        dataset = parameters.datasetLabel;

        LOG.info("Dataset is: " + dataset);

        roi = parameters.roi;

        LOG.info("Roi is: " + roi);

        try (Neo4jReader neo4jReader = new Neo4jReader(parameters.getDbConfig())) {

            String vertexJson = neo4jReader.getLineGraphVerticesJson(dataset, roi);
            //neo4jReader.getLineGraphEdgesAndVerticesJson(dataset, roi, vertexJson);


        }




    }

    private static final Logger LOG = Logger.getLogger("NeuPrinterMain.class");

}





