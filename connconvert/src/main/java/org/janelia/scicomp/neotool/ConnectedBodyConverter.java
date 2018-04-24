package org.janelia.scicomp.neotool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import java.io.File;
import java.io.FileReader;
import java.util.List;

import org.janelia.scicomp.neotool.db.DbConfig;
import org.janelia.scicomp.neotool.json.JsonUtils;
import org.janelia.scicomp.neotool.model.Body;
import org.janelia.scicomp.neotool.model.Neuron;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts JSON neuron and synapse data exported from DVID into a graph model
 * and (optionally) imports that model into a neo4j database.
 */
public class ConnectedBodyConverter {

    @Parameters
    public static class ConverterParameters {

        @Parameter(
                names = "--dbProperties",
                description = "Properties file containing database information (omit to print statements to stdout)",
                required = false)
        public String dbProperties;

        @Parameter(
                names = "--prepDatabase",
                description = "Indicates that database constraints and indexes should be setup (omit to skip)",
                required = false,
                arity = 0)
        public boolean prepDatabase;

        @Parameter(
                names = "--neuronJson",
                description = "JSON file containing neuron data to import",
                required = false)
        public String neuronJson;

        @Parameter(
                names = "--neuronDataSet",
                description = "Data set value for all neurons",
                required = false)
        public String neuronDataSet;

        @Parameter(
                names = "--synapseJson",
                description = "JSON file containing body synapse data to import",
                required = false)
        public String synapseJson;

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

    public static void main(String[] args) {

        final ConverterParameters parameters = new ConverterParameters();
        final JCommander jCommander = new JCommander(parameters);
        jCommander.setProgramName("java -cp converter.jar " + ConnectedBodyConverter.class.getName());

        boolean parseFailed = true;
        try {
            jCommander.parse(args);
            parseFailed = false;
        } catch (final ParameterException pe) {
            JCommander.getConsole().println("\nERROR: failed to parse command line arguments\n\n" + pe.getMessage());
        } catch (final Throwable t) {
            LOG.error("failed to parse command line arguments", t);
        }

        if (parameters.help || parseFailed) {
            JCommander.getConsole().println("");
            jCommander.usage();
            System.exit(1);
        }

        LOG.info("running with parameters: {}", parameters);

        try {
            final NeoImporter neoImporter = new NeoImporter(parameters.getDbConfig());

            if (parameters.prepDatabase) {
                neoImporter.prepDatabase();
            }

            if (parameters.neuronJson != null) {
                final File neuronJsonFile = new File(parameters.neuronJson).getAbsoluteFile();
                final List<Neuron> neuronList = Neuron.fromJsonArray(new FileReader(neuronJsonFile));
                neoImporter.addNeurons(parameters.neuronDataSet, neuronList);
            }

            if (parameters.synapseJson != null) {
                final File synapsesJsonFile = new File(parameters.synapseJson).getAbsoluteFile();
                final SynapseMapper mapper = new SynapseMapper();
                final List<Body> bodyList = mapper.loadAndMapBodies(synapsesJsonFile);
                neoImporter.importBodyList(bodyList);
            }

        } catch (final Throwable t) {
            LOG.error("failed to convert data", t);
            System.exit(1);
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(ConnectedBodyConverter.class);

}
