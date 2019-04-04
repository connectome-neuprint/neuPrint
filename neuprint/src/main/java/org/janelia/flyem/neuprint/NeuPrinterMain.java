package org.janelia.flyem.neuprint;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;
import org.janelia.flyem.neuprint.db.DbConfig;
import org.janelia.flyem.neuprint.json.JsonUtils;
import org.janelia.flyem.neuprint.model.MetaInfo;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Skeleton;
import org.janelia.flyem.neuprint.model.Synapse;
import org.janelia.flyem.neuprint.model.SynapticConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The NeuPrinterMain class implements an application that loads neuron and synapse
 * data provided by JSON files into a neo4j database.
 *
 * @see <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">neuron and synapse JSON spec</a>
 */
public class NeuPrinterMain {

    /**
     * Class containing {@link JCommander} parameters.
     */
    @Parameters(separators = "=")
    public static class NeuPrinterParameters {

        @Parameter(
                names = "--dbProperties",
                description = "Properties file containing database information (required)",
                required = true)
        String dbProperties;

        @Parameter(
                names = "--synapseJson",
                description = "JSON file containing body synapse data to import")
        String synapseJson;

        @Parameter(
                names = "--connectionsJson",
                description = "Path to JSON file containing synaptic connections.")
        String connectionsJson;

        @Parameter(
                names = "--neuronJson",
                description = "JSON file containing neuron data to import")
        String neuronJson;

        @Parameter(
                names = "--datasetLabel",
                description = "Dataset value for all nodes (required)",
                required = true)
        String datasetLabel;

        @Parameter(
                names = "--dataModelVersion",
                description = "Data model version (required)")
        float dataModelVersion = 1.0F;

        @Parameter(
                names = "--preHPThreshold",
                description = "Confidence threshold to distinguish high-precision presynaptic densities (default is 0.0)")
        float preHPThreshold;

        @Parameter(
                names = "--postHPThreshold",
                description = "Confidence threshold to distinguish high-precision postsynaptic densities (default is 0.0)")
        float postHPThreshold;

        @Parameter(
                names = "--addConnectionSetRoiInfoAndWeightHP",
                description = "Indicates that an roiInfo property should be added to each ConnectionSet and that the weightHP property should be added to all ConnectionSets (true by default).",
                arity = 1
        )
        boolean addConnectionSetRoiInfoAndWeightHP = true;

        @Parameter(
                names = "--skeletonDirectory",
                description = "Path to directory containing skeleton files for this dataset")
        String skeletonDirectory;

        @Parameter(
                names = "--metaInfoJson",
                description = "JSON file containing meta information for dataset"
        )
        String metaInfoJson;

        @Parameter(
                names = "--addAutoNames",
                description = "Indicates that automatically generated names should be added for this dataset. Auto-names are in the format " +
                        "ROIA-ROIB_8 where ROIA is the roi in which a given neuron has the most inputs (postsynaptic densities) " +
                        "and ROIB is the roi in which a neuron has the most outputs (presynaptic densities). The final number renders " +
                        "this name unique per dataset. Names are only generated for neurons that have greater than the number of synapses " +
                        "indicated by neuronThreshold. If neurons do not already have a name, the auto-name is added to the name property. (skip to omit)",
                arity = 0)
        public boolean addAutoNames;

        @Parameter(
                names = "--neuronThreshold",
                description = "Integer indicating the number of synaptic densities (>=neuronThreshold/5 pre OR >=neuronThreshold post) a neuron should have to be given " +
                        "the label of :Neuron (all have the :Segment label by default) and an auto-name. To add auto-names, must have" +
                        " --addAutoName OR --addAutoNamesOnly enabled.")
        Integer neuronThreshold = 10;

        @Parameter(
                names = "--addClusterNames",
                description = "Indicates that cluster names should be added to Neuron nodes. (true by default)",
                arity = 1)
        boolean addClusterNames = true;

        @Parameter(
                names = "--help",
                help = true)
        boolean help;

        /**
         * Returns {@link DbConfig} object containing database configuration read from file.
         *
         * @return DbConfig
         */
        DbConfig getDbConfig() {
            return (dbProperties == null) ? null : DbConfig.fromFile(new File(dbProperties));
        }

        @Override
        public String toString() {
            return JsonUtils.GSON.toJson(this);
        }
    }

    /**
     * Returns a list of {@link Neuron} objects read from a JSON file
     * at the provided file path.
     *
     * @param filepath path to neuron JSON file
     * @return list of Neurons
     */
    public static List<Neuron> readNeuronsJson(String filepath) {
        List<Neuron> neuronList = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            neuronList = Neuron.fromJson(reader);
            LOG.info(String.format("Loaded %d neurons/segments from JSON.", neuronList.size()));
        } catch (Exception e) {
            LOG.error("Error reading neurons JSON: " + e);
            System.exit(1);
        }

//        try (JsonReader reader = new JsonReader(new FileReader(filepath)) ) {
//            neuronList = new ArrayList<>();
//            reader.beginArray();
//            int i = 0;
//            while (reader.hasNext() && i < 50000) {
//
//                Neuron neuron = Neuron.fromJsonSingleObject(reader);
//                //System.out.println("added neuron " + neuron.getId());
//
//                neuronList.add(neuron);
//                i++;
//            }
//
//
//        }
//
        return neuronList;

    }

    /**
     * Returns a list of {@link Synapse} objects read from a JSON file
     * at the provided file path.
     *
     * @param filepath path to synapses JSON file
     * @return list of {@link Synapse} objects
     */
    public static List<Synapse> readSynapsesJson(String filepath) {
        List<Synapse> synapseList = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            synapseList = Synapse.fromJson(reader);
            LOG.info(String.format("Loaded %d synapses from JSON.", synapseList.size()));
        } catch (Exception e) {
            LOG.error("Error reading synapse JSON: " + e);
            System.exit(1);
        }
        return synapseList;
    }

    /**
     * Returns a list of {@link SynapticConnection} objects read from a JSON file
     * at the provided file path.
     *
     * @param filepath path to connections JSON file
     * @return list of {@link SynapticConnection} objects
     */
    public static List<SynapticConnection> readConnectionsJson(String filepath) {
        List<SynapticConnection> connectionList = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            connectionList = SynapticConnection.fromJson(reader);
            LOG.info(String.format("Loaded %d synaptic connections from JSON.", connectionList.size()));
        } catch (Exception e) {
            LOG.error("Error reading connections JSON: " + e);
            System.exit(1);
        }
        return connectionList;
    }

    public static MetaInfo readMetaInfoJson(String filepath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            MetaInfo metaInfo = MetaInfo.fromJson(reader);
            LOG.info(String.format("Loaded meta info for dataset: %s", metaInfo));
            return metaInfo;
        } catch (Exception e) {
            LOG.error("Error reading meta info json: " + e);
            System.exit(1);
        }
        return null;
    }

    /**
     * Returns a list of {@link Skeleton} objects read from an array of swc files.
     *
     * @param arrayOfSwcFiles {@link File} array of swc files
     * @return list of Skeletons
     */
    public static List<Skeleton> createSkeletonListFromSwcFileArray(File[] arrayOfSwcFiles) {
        List<Skeleton> skeletonList = new ArrayList<>();
        for (File swcFile : arrayOfSwcFiles) {
            String filepath = swcFile.getAbsolutePath();
            Long associatedBodyId = setSkeletonAssociatedBodyId(filepath);
            Skeleton skeleton = new Skeleton();

            try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
                skeleton.fromSwc(reader, associatedBodyId);
                skeletonList.add(skeleton);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return skeletonList;
    }

    /**
     * Returns the associated bodyId for the Skeleton read from the swc file name.
     *
     * @param swcFilePath path to swc file
     * @return bodyId
     */
    private static Long setSkeletonAssociatedBodyId(String swcFilePath) {

        String patternSurroundingId = ".*/(.*?).swc";
        Pattern r = Pattern.compile(patternSurroundingId);
        Matcher mR = r.matcher(swcFilePath);
        mR.matches();
        return Long.parseLong(mR.group(1));

    }

    public static void initializeDatabase(Neo4jImporter neo4jImporter,
                                           String dataset,
                                           float dataModelVersion,
                                           double preHPThreshold,
                                           double postHPThreshold,
                                           boolean addConnectionSetRoiInfoAndWeightHP,
                                           boolean addClusterNames,
                                           LocalDateTime timeStamp) {
        neo4jImporter.prepDatabase(dataset);
        neo4jImporter.createMetaNodeWithDataModelNode(dataset, dataModelVersion, preHPThreshold, postHPThreshold, addConnectionSetRoiInfoAndWeightHP, timeStamp);
        if (addClusterNames) {
            neo4jImporter.prepDatabaseForClusterNames(dataset);
        }

    }

    public static void main(String[] args) {

        final NeuPrinterParameters parameters = new NeuPrinterParameters();
        final JCommander jCommander = new JCommander(parameters);
        jCommander.setProgramName("java -jar neuprint.jar");

        boolean parseFailed = true;
        try {
            jCommander.parse(args);
            parseFailed = false;
        } catch (final ParameterException pe) {
            JCommander.getConsole().println("\nERROR: failed to parse command line arguments\n\n" + pe.getMessage());
        } catch (final Throwable t) {
            LOG.info("failed to parse command line arguments", t);
        }

        if (parameters.help || parseFailed) {
            JCommander.getConsole().println("");
            jCommander.usage();
            System.exit(1);
        }

        LOG.info("running with parameters: " + parameters);

        final String dataset = parameters.datasetLabel;
        final float dataModelVersion = parameters.dataModelVersion;
        final float preHPThreshold = parameters.preHPThreshold;
        final float postHPThreshold = parameters.postHPThreshold;
        final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

        boolean databaseInitialized = false;

        LOG.info("Dataset is: " + dataset);

        try {

            Stopwatch timer = Stopwatch.createStarted();

            if (parameters.synapseJson != null) {

                // TODO: add batching option here
                List<Synapse> synapseList = readSynapsesJson(parameters.synapseJson);
                LOG.info(String.format("Reading in synapse JSON took: %s", timer.stop()));
                timer.reset();

                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                    initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, parameters.addConnectionSetRoiInfoAndWeightHP, parameters.addClusterNames, timeStamp);
                    databaseInitialized = true;
                    timer.start();
                    neo4jImporter.addSynapsesWithRois(dataset, synapseList, timeStamp);
                    LOG.info(String.format("Loading all synapses took: %s", timer.stop()));
                    timer.reset();

                    neo4jImporter.indexBooleanRoiProperties(dataset);

                }
            }

            if (parameters.connectionsJson != null) {

                // TODO: add batching option here
                timer.start();
                List<SynapticConnection> connectionsList = readConnectionsJson(parameters.connectionsJson);
                LOG.info(String.format("Reading in synaptic connections JSON took: %s", timer.stop()));
                timer.reset();

                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                    if (!databaseInitialized) {
                        initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, parameters.addConnectionSetRoiInfoAndWeightHP, parameters.addClusterNames, timeStamp);
                    }
                    timer.start();
                    neo4jImporter.addSynapsesTo(dataset, connectionsList, timeStamp);
                    LOG.info(String.format("Loading all synaptic connections took: %s", timer.stop()));
                    timer.reset();

                }
            }

            if (parameters.neuronJson != null) {

                // TODO: add batching option here
                timer.start();
                List<Neuron> neuronList = readNeuronsJson(parameters.neuronJson);
                LOG.info(String.format("Reading in neurons JSON took: %s", timer.stop()));
                timer.reset();

                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                    if (!databaseInitialized) {
                        initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, parameters.addConnectionSetRoiInfoAndWeightHP, parameters.addClusterNames, timeStamp);
                    }
                    timer.start();
                    neo4jImporter.addSegments(dataset, neuronList, parameters.addConnectionSetRoiInfoAndWeightHP, preHPThreshold, postHPThreshold, parameters.neuronThreshold, timeStamp);
                    LOG.info(String.format("Loading all neurons took: %s", timer.stop()));
                    timer.reset();

                    neo4jImporter.indexBooleanRoiProperties(dataset);

                }
            }

            if (parameters.addAutoNames) {

                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                    if (!databaseInitialized) {
                        initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, parameters.addConnectionSetRoiInfoAndWeightHP, parameters.addClusterNames, timeStamp);
                    }
                    timer.start();
                    neo4jImporter.addAutoNames(dataset, timeStamp);
                    LOG.info(String.format("Adding all auto names took: %s", timer.stop()));
                    timer.reset();

                }


            }

            if (parameters.skeletonDirectory != null) {

                final File folder = new File(parameters.skeletonDirectory);
                final File[] arrayOfSwcFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".swc"));

                assert arrayOfSwcFiles != null : "No swc files found.";
                LOG.info("Reading in " + arrayOfSwcFiles.length + " swc files.");

                final List<Skeleton> skeletonList = createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                    if (!databaseInitialized) {
                        initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, parameters.addConnectionSetRoiInfoAndWeightHP, parameters.addClusterNames, timeStamp);
                    }

                    timer.start();
                    neo4jImporter.addSkeletonNodes(dataset, skeletonList, timeStamp);
                    LOG.info("Loading all Skeleton nodes took: " + timer.stop());
                    timer.reset();
                }

            }

            if (parameters.metaInfoJson != null) {

                // read meta info data
                MetaInfo metaInfo = readMetaInfoJson(parameters.metaInfoJson);
                if (metaInfo != null) {
                    try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
                        if (!databaseInitialized) {
                            initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, parameters.addConnectionSetRoiInfoAndWeightHP, parameters.addClusterNames, timeStamp);
                        }
                        neo4jImporter.addMetaInfo(dataset, metaInfo, timeStamp);
                        LOG.info("Finished adding meta info.");
                    }
                }

            }

        } catch (Exception e) {
            LOG.error("Error loading data: " + e);
            System.exit(1);
        }

    }

    private static final Logger LOG = LoggerFactory.getLogger(NeuPrinterMain.class);

}





