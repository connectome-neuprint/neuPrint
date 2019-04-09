package org.janelia.flyem.neuprint;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;
import com.google.gson.stream.JsonReader;
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
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The NeuPrintMain class implements an application that loads neuron and synapse
 * data provided by JSON files into a neo4j database.
 *
 * @see <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">neuron and synapse JSON spec</a>
 */
public class NeuPrintMain {

    /**
     * Class containing {@link JCommander} parameters.
     */
    @Parameters(separators = "=")
    public static class NeuPrintParameters {

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
                names = "--connectionJson",
                description = "Path to JSON file containing synaptic connections.")
        String connectionJson;

        @Parameter(
                names = "--neuronJson",
                description = "JSON file containing neuron data to import")
        String neuronJson;

        @Parameter(
                names = "--synapseBatchSize",
                description = "If > 0, the synapse JSON file will be loaded in batches of this size."
        )
        int synapseBatchSize;

        @Parameter(
                names = "--connectionBatchSize",
                description = "If > 0, the connection JSON file will be loaded in batches of this size."
        )
        int connectionBatchSize;

        @Parameter(
                names = "--neuronBatchSize",
                description = "If > 0, the neuron JSON file will be loaded in batches of this size."
        )
        int neuronBatchSize;

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
                names = "--neuronThreshold",
                description = "Integer indicating the number of synaptic densities (>=neuronThreshold/5 pre OR >=neuronThreshold post) a neuron should have to be given " +
                        "the label of :Neuron (all have the :Segment label by default). "
        )
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

        return neuronList;

    }

    public static void loadNeuronJsonInBatches(String filepath,
                                               int neuronBatchSize,
                                               Neo4jImporter neo4jImporter,
                                               String dataset,
                                               boolean databaseInitialized,
                                               float dataModelVersion,
                                               double preHPThreshold,
                                               double postHPThreshold,
                                               long neuronThreshold,
                                               boolean addConnectionSetRoiInfoAndWeightHP,
                                               boolean addClusterNames,
                                               LocalDateTime timeStamp) {

        Stopwatch timer = Stopwatch.createUnstarted();

        if (!databaseInitialized) {
            initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, addConnectionSetRoiInfoAndWeightHP, addClusterNames, timeStamp);

        }

        LOG.info(String.format("Loading neurons in batches of size %d", neuronBatchSize));

        try (JsonReader reader = new JsonReader(new FileReader(filepath))) {
            reader.beginArray();
            while (reader.hasNext()) {
                List<Neuron> neuronList = new ArrayList<>();
                int i = 0;
                while (reader.hasNext() && i < neuronBatchSize) {
                    Neuron neuron = Neuron.fromJsonSingleObject(reader);
                    neuronList.add(neuron);
                    i++;
                }

                timer.start();
                neo4jImporter.addSegments(dataset, neuronList, timeStamp);
                LOG.info(String.format("Loading batch of neurons took: %s", timer.stop()));
                timer.reset();

                timer.start();
                neo4jImporter.addConnectionInfo(dataset, neuronList, addConnectionSetRoiInfoAndWeightHP, preHPThreshold, postHPThreshold, neuronThreshold);
                LOG.info(String.format("Loading all connection info for batch took: %s", timer.stop()));
                timer.reset();
            }

        } catch (IOException e) {
            LOG.error("Error reading neurons JSON: " + e);
            System.exit(1);
        }

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

    public static void loadSynapseJsonInBatches(String filepath,
                                                int synapseBatchSize,
                                                Neo4jImporter neo4jImporter,
                                                String dataset,
                                                boolean databaseInitialized,
                                                float dataModelVersion,
                                                double preHPThreshold,
                                                double postHPThreshold,
                                                boolean addConnectionSetRoiInfoAndWeightHP,
                                                boolean addClusterNames,
                                                LocalDateTime timeStamp) {

        Stopwatch timer = Stopwatch.createUnstarted();

        if (!databaseInitialized) {
            initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, addConnectionSetRoiInfoAndWeightHP, addClusterNames, timeStamp);

        }

        LOG.info(String.format("Loading synapses in batches of size %d", synapseBatchSize));

        try (JsonReader reader = new JsonReader(new FileReader(filepath))) {
            reader.beginArray();
            while (reader.hasNext()) {
                List<Synapse> synapseList = new ArrayList<>();
                int i = 0;
                while (reader.hasNext() && i < synapseBatchSize) {
                    Synapse synapse = Synapse.fromJsonSingleObject(reader);
                    synapseList.add(synapse);
                    i++;
                }

                timer.start();
                neo4jImporter.addSynapsesWithRois(dataset, synapseList, timeStamp);
                LOG.info(String.format("Loading batch of synapses took: %s", timer.stop()));
                timer.reset();

                neo4jImporter.indexBooleanRoiProperties(dataset);

            }

        } catch (IOException e) {
            LOG.error("Error reading synapse JSON: " + e);
            System.exit(1);
        }

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

    public static void loadConnectionJsonInBatches(String filepath,
                                                   int connectionBatchSize,
                                                   Neo4jImporter neo4jImporter,
                                                   String dataset,
                                                   boolean databaseInitialized,
                                                   float dataModelVersion,
                                                   double preHPThreshold,
                                                   double postHPThreshold,
                                                   boolean addConnectionSetRoiInfoAndWeightHP,
                                                   boolean addClusterNames,
                                                   LocalDateTime timeStamp) {

        Stopwatch timer = Stopwatch.createUnstarted();

        if (!databaseInitialized) {
            initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, addConnectionSetRoiInfoAndWeightHP, addClusterNames, timeStamp);
        }

        LOG.info(String.format("Loading connections in batches of size %d", connectionBatchSize));

        try (JsonReader reader = new JsonReader(new FileReader(filepath))) {
            reader.beginArray();
            while (reader.hasNext()) {
                List<SynapticConnection> connectionsList = new ArrayList<>();
                int i = 0;
                while (reader.hasNext() && i < connectionBatchSize) {
                    SynapticConnection synapticConnection = SynapticConnection.fromJsonSingleObject(reader);
                    connectionsList.add(synapticConnection);
                    i++;
                }

                timer.start();
                neo4jImporter.addSynapsesTo(dataset, connectionsList, timeStamp);
                LOG.info(String.format("Loading batch of synaptic connections took: %s", timer.stop()));
                timer.reset();

            }

        } catch (IOException e) {
            LOG.error("Error reading connection JSON: " + e);
            System.exit(1);
        }

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

    public static void runStandardLoad(Neo4jImporter neo4jImporter,
                                       String dataset,
                                       List<Synapse> synapseList,
                                       List<SynapticConnection> connectionsList,
                                       List<Neuron> neuronList,
                                       List<Skeleton> skeletonList,
                                       MetaInfo metaInfo,
                                       float dataModelVersion,
                                       double preHPThreshold,
                                       double postHPThreshold,
                                       long neuronThreshold,
                                       boolean addConnectionSetRoiInfoAndWeightHP,
                                       boolean addClusterNames,
                                       LocalDateTime timeStamp) {

        runStandardLoadWithoutMetaInfo(neo4jImporter, dataset, synapseList, connectionsList, neuronList, skeletonList, dataModelVersion, preHPThreshold, postHPThreshold, neuronThreshold, addConnectionSetRoiInfoAndWeightHP, addClusterNames, timeStamp);
        neo4jImporter.addMetaInfo("test", metaInfo, timeStamp);
    }

    public static void runStandardLoadWithoutMetaInfo(Neo4jImporter neo4jImporter,
                                                      String dataset,
                                                      List<Synapse> synapseList,
                                                      List<SynapticConnection> connectionsList,
                                                      List<Neuron> neuronList,
                                                      List<Skeleton> skeletonList,
                                                      float dataModelVersion,
                                                      double preHPThreshold,
                                                      double postHPThreshold,
                                                      long neuronThreshold,
                                                      boolean addConnectionSetRoiInfoAndWeightHP,
                                                      boolean addClusterNames,
                                                      LocalDateTime timeStamp) {

        NeuPrintMain.initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, addConnectionSetRoiInfoAndWeightHP, addClusterNames, timeStamp);
        neo4jImporter.addSynapsesWithRois("test", synapseList, timeStamp);
        neo4jImporter.indexBooleanRoiProperties(dataset);
        neo4jImporter.addSynapsesTo("test", connectionsList, timeStamp);
        neo4jImporter.addSegments("test", neuronList, timeStamp);
        neo4jImporter.addConnectionInfo("test", neuronList, addConnectionSetRoiInfoAndWeightHP, preHPThreshold, postHPThreshold, neuronThreshold);
        neo4jImporter.addSkeletonNodes("test", skeletonList, timeStamp);
    }

    public static void main(String[] args) {

        final NeuPrintParameters parameters = new NeuPrintParameters();
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
        final long neuronThreshold = parameters.neuronThreshold;
        final LocalDateTime timeStamp = LocalDateTime.now().truncatedTo(ChronoUnit.SECONDS);

        boolean databaseInitialized = false;

        LOG.info("Dataset is: " + dataset);

        try {

            Stopwatch timer = Stopwatch.createUnstarted();

            if (parameters.synapseJson != null) {

                if (parameters.synapseBatchSize > 0) {
                    try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
                        loadSynapseJsonInBatches(parameters.synapseJson, parameters.synapseBatchSize, neo4jImporter, dataset, false, dataModelVersion, preHPThreshold, postHPThreshold, parameters.addConnectionSetRoiInfoAndWeightHP, parameters.addClusterNames, timeStamp);
                        databaseInitialized = true;
                    }
                } else {

                    timer.start();
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

            }

            if (parameters.connectionJson != null) {

                if (parameters.connectionBatchSize > 0) {
                    try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
                        loadConnectionJsonInBatches(parameters.connectionJson, parameters.connectionBatchSize, neo4jImporter, dataset, databaseInitialized, dataModelVersion, preHPThreshold, postHPThreshold, parameters.addConnectionSetRoiInfoAndWeightHP, parameters.addClusterNames, timeStamp);
                        databaseInitialized = true;
                    }
                } else {
                    timer.start();
                    List<SynapticConnection> connectionsList = readConnectionsJson(parameters.connectionJson);
                    LOG.info(String.format("Reading in synaptic connections JSON took: %s", timer.stop()));
                    timer.reset();

                    try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                        if (!databaseInitialized) {
                            initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, parameters.addConnectionSetRoiInfoAndWeightHP, parameters.addClusterNames, timeStamp);
                            databaseInitialized = true;
                        }
                        timer.start();
                        neo4jImporter.addSynapsesTo(dataset, connectionsList, timeStamp);
                        LOG.info(String.format("Loading all synaptic connections took: %s", timer.stop()));
                        timer.reset();

                    }
                }
            }

            if (parameters.neuronJson != null) {

                if (parameters.neuronBatchSize > 0) {
                    try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
                        loadNeuronJsonInBatches(parameters.neuronJson, parameters.neuronBatchSize, neo4jImporter, dataset, databaseInitialized, dataModelVersion, preHPThreshold, postHPThreshold, neuronThreshold, parameters.addConnectionSetRoiInfoAndWeightHP, parameters.addClusterNames, timeStamp);
                        databaseInitialized = true;
                    }
                } else {
                    timer.start();
                    List<Neuron> neuronList = readNeuronsJson(parameters.neuronJson);
                    LOG.info(String.format("Reading in neurons JSON took: %s", timer.stop()));
                    timer.reset();

                    try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                        if (!databaseInitialized) {
                            initializeDatabase(neo4jImporter, dataset, dataModelVersion, preHPThreshold, postHPThreshold, parameters.addConnectionSetRoiInfoAndWeightHP, parameters.addClusterNames, timeStamp);
                            databaseInitialized = true;
                        }
                        timer.start();
                        neo4jImporter.addSegments(dataset, neuronList, timeStamp);
                        LOG.info(String.format("Loading all neurons took: %s", timer.stop()));
                        timer.reset();

                        timer.start();
                        neo4jImporter.addConnectionInfo(dataset, neuronList, parameters.addConnectionSetRoiInfoAndWeightHP, preHPThreshold, postHPThreshold, neuronThreshold);
                        LOG.info(String.format("Loading all connection info took: %s", timer.stop()));
                        timer.reset();
                    }
                }
            }

            if (parameters.skeletonDirectory != null) {

                // TODO: allow batching here
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

    private static final Logger LOG = LoggerFactory.getLogger(NeuPrintMain.class);

}





