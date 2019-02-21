package org.janelia.flyem.neuprinter;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.base.Stopwatch;
import org.janelia.flyem.neuprinter.db.DbConfig;
import org.janelia.flyem.neuprinter.json.JsonUtils;
import org.janelia.flyem.neuprinter.model.BodyWithSynapses;
import org.janelia.flyem.neuprinter.model.Neuron;
import org.janelia.flyem.neuprinter.model.Skeleton;
import org.janelia.flyem.neuprinter.model.SynapseLocationToBodyIdMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
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
                names = "--loadNeurons",
                description = "Indicates that data from neurons JSON should be loaded to database (omit to skip)",
                arity = 0)
        boolean loadNeurons;

        @Parameter(
                names = "--loadSynapses",
                description = "Indicates that data from synapses JSON should be loaded to database (omit to skip)",
                arity = 0)
        boolean loadSynapses;

        @Parameter(
                names = "--startFromSynapseLoad",
                description = "Indicates that load should start from the synapses JSON.",
                arity = 0)
        boolean startFromSynapsesLoad;

        @Parameter(
                names = "--addNeuronsAndSynapses",
                description = "Indicates that both neurons and synapses JSONs should be loaded and all database features added",
                arity = 0)
        boolean addNeuronsAndSynapses;

        @Parameter(
                names = "--prepDatabase",
                description = "Indicates that database constraints and indexes should be setup (omit to skip)",
                arity = 0)
        public boolean prepDatabase;

        @Parameter(
                names = "--addConnectsTo",
                description = "Indicates that ConnectsTo relations should be added (omit to skip)",
                arity = 0)
        public boolean addConnectsTo;

        @Parameter(
                names = "--addSynapses",
                description = "Indicates that synapse nodes should be added (omit to skip)",
                arity = 0)
        public boolean addSynapses;

        @Parameter(
                names = "--addSynapsesTo",
                description = "Indicates that SynapsesTo relations should be added (omit to skip)",
                arity = 0)
        public boolean addSynapsesTo;

        @Parameter(
                names = "--addSegmentRois",
                description = "Indicates that neuron ROI labels should be added (omit to skip)",
                arity = 0)
        public boolean addNeuronRois;

        @Parameter(
                names = "--addConnectionSets",
                description = "Indicates that connection set nodes should be added (omit to skip)",
                arity = 0)
        public boolean addConnectionSets;

        @Parameter(
                names = "--addSkeletons",
                description = "Indicates that skeleton nodes should be added (omit to skip)",
                arity = 0)
        boolean addSkeletons;

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
                description = "Confidence threshold to distinguish high-precision presynaptic densities (required)")
        float preHPThreshold;

        @Parameter(
                names = "--postHPThreshold",
                description = "Confidence threshold to distinguish high-precision postsynaptic densities (required)")
        float postHPThreshold;

        @Parameter(
                names = "--synapseJson",
                description = "JSON file containing body synapse data to import")
        String synapseJson;

        @Parameter(
                names = "--skeletonDirectory",
                description = "Path to directory containing skeleton files for this dataset")
        String skeletonDirectory;

        @Parameter(
                names = "--editMode",
                description = "Indicates that neuprint is being used in edit mode to alter data in an existing database (omit to skip).",
                arity = 0)
        public boolean editMode;

        @Parameter(
                names = "--addMetaNodeOnly",
                description = "Indicates that only the Meta Node should be added for this dataset. Requires the existing dataset to be completely loaded into neo4j. (omit to skip)",
                arity = 0)
        boolean addMetaNodeOnly;

        @Parameter(
                names = "--indexBooleanRoiPropertiesOnly",
                description = "Indicates that only boolean roi properties should be indexed. Requires the existing dataset to be completely loaded into neo4j. (omit to skip)",
                arity = 0)
        boolean indexBooleanRoiPropertiesOnly;

        @Parameter(
                names = "--addAutoNamesOnly",
                description = "Indicates that only the autoNames should be added for this dataset. Requires the existing dataset to be completely loaded into neo4j. Names are only generated for neurons that have greater than the number of synapses" +
                        "indicated by neuronThreshold (omit to skip)",
                arity = 0
        )
        boolean addAutoNamesOnly;

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
                names = "--getSuperLevelRoisFromSynapses",
                description = "Indicates that super level rois should be computed from synapses JSON and added to the Meta node.",
                arity = 0)
        public boolean getSuperLevelRoisFromSynapses;

        @Parameter(
                names = "--uuid",
                description = "DVID UUID to be added to Meta node."
        )
        public String dvidUuid;

        @Parameter(
                names = "--server",
                description = "DVID server to be added to Meta node."
        )
        public String dvidServer;

        @Parameter(
                names = "--addClusterNames",
                description = "Indicates that cluster names should be added to Neuron nodes.",
                arity = 0)
        public boolean addClusterNames;

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

    private static List<Neuron> neuronList;
    private static List<BodyWithSynapses> bodyList;

    /**
     * Returns a list of {@link Neuron} objects read from a JSON file
     * at the provided file path.
     *
     * @param filepath path to neuron JSON file
     * @return list of Neurons
     */
    public static List<Neuron> readNeuronsJson(String filepath) {

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            neuronList = Neuron.fromJson(reader);
            LOG.info("Number of neurons/segments: " + neuronList.size());
        } catch (Exception e) {
            e.printStackTrace();
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
     * Returns a list of {@link BodyWithSynapses} objects read from a JSON file
     * at the provided file path.
     *
     * @param filepath path to synapses JSON file
     * @return list of BodyWithSynapses
     */
    private static List<BodyWithSynapses> readSynapsesJson(String filepath) {

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            bodyList = BodyWithSynapses.fromJson(reader);
            LOG.info("Number of bodies with synapses: " + bodyList.size());
        } catch (Exception e) {
            e.printStackTrace();
        }

//        try (JsonReader reader = new JsonReader(new FileReader(filepath)) ) {
//            Gson gson = new GsonBuilder().setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE).create();
//            bodyList = new ArrayList<>();
//            reader.beginArray();
//            int i = 0;
//            //Stopwatch timer_bws = Stopwatch.createStarted();
//            while (reader.hasNext() && i < 2400000) {
//
//                BodyWithSynapses bws = gson.fromJson(reader, BodyWithSynapses.class);
//                //System.out.println("added neuron " + neuron.getId());
//
//                bodyList.add(bws);
//                i++;
//                //System.out.println(timer_bws);
//
//            }
//
//
//        }
        return bodyList;
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

    public static void main(String[] args) {

        final NeuPrinterParameters parameters = new NeuPrinterParameters();
        final JCommander jCommander = new JCommander(parameters);
        jCommander.setProgramName("java -cp neuprint.jar");

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

        LOG.info("Dataset is: " + dataset);

        try {

            if (parameters.loadNeurons || parameters.addNeuronsAndSynapses) {

                // read in the neurons data
                Stopwatch timer2 = Stopwatch.createStarted();
                neuronList = readNeuronsJson(parameters.neuronJson);
                LOG.info("Reading in neurons JSON took: " + timer2.stop());
                timer2.reset();
                //write it to the database
                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                    if (parameters.prepDatabase || parameters.addNeuronsAndSynapses) {
                        neo4jImporter.prepDatabase(dataset);
                    }

                    Stopwatch timer = Stopwatch.createStarted();
                    neo4jImporter.addSegments(dataset, neuronList);
                    LOG.info("Loading all Segment nodes took: " + timer.stop());
                    timer.reset();
                }
            }

            if (parameters.loadSynapses || parameters.addNeuronsAndSynapses || parameters.startFromSynapsesLoad) {

                Stopwatch timer = Stopwatch.createStarted();
                SynapseMapper mapper = new SynapseMapper();
                bodyList = mapper.loadAndMapBodies(parameters.synapseJson);
                LOG.info("Number of bodies with synapses: " + bodyList.size());
                LOG.info("Reading in synapse JSON took: " + timer.stop());
                timer.reset();

                final HashMap<String, Set<String>> preToPost = mapper.getPreToPostMap();
                final SynapseLocationToBodyIdMap synapseLocationToBodyIdMap = mapper.getSynapseLocationToBodyIdMap();

                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                    if (parameters.startFromSynapsesLoad || (parameters.prepDatabase && !(parameters.loadNeurons || parameters.addNeuronsAndSynapses))) {
                        neo4jImporter.prepDatabase(dataset);
                    }

                    if (parameters.startFromSynapsesLoad || parameters.addConnectsTo || parameters.addNeuronsAndSynapses) {

                        timer.start();
                        neo4jImporter.addConnectsTo(dataset, bodyList);
                        LOG.info("Loading all ConnectsTo took: " + timer.stop());
                        timer.reset();

                    }

                    if (parameters.startFromSynapsesLoad || parameters.addSynapses || parameters.addNeuronsAndSynapses) {
                        timer.start();
                        neo4jImporter.addSynapsesWithRois(dataset, bodyList);
                        LOG.info("Loading all Synapses took: " + timer.stop());
                        timer.reset();
                    }

                    if (parameters.startFromSynapsesLoad || parameters.addSynapsesTo || parameters.addNeuronsAndSynapses) {
                        timer.start();
                        neo4jImporter.addSynapsesTo(dataset, preToPost);
                        LOG.info("Loading all SynapsesTo took: " + timer.stop());
                        timer.reset();
                    }

                    if (parameters.startFromSynapsesLoad || parameters.addNeuronRois || parameters.addNeuronsAndSynapses) {
                        timer.start();
                        neo4jImporter.addSegmentRois(dataset, bodyList);
                        LOG.info("Loading all Segment ROI labels took: " + timer.stop());
                        timer.reset();
                    }

                    if (parameters.startFromSynapsesLoad || parameters.addConnectionSets || parameters.addNeuronsAndSynapses) {
                        timer.start();
                        neo4jImporter.addConnectionSets(dataset, bodyList, synapseLocationToBodyIdMap, preHPThreshold, postHPThreshold);
                        LOG.info("Loading ConnectionSets took: " + timer.stop());
                        timer.reset();

                        timer.start();
                        neo4jImporter.addSynapseSets(dataset, bodyList);
                        LOG.info("Loading SynapseSets took: " + timer.stop());
                        timer.reset();
                    }

                    if (parameters.addAutoNames) {
                        timer.start();
                        neo4jImporter.addAutoNamesAndNeuronLabels(dataset, parameters.neuronThreshold);
                        LOG.info("Adding autoNames and :Neuron labels took: " + timer.stop());
                        timer.reset();
                    } else {
                        timer.start();
                        neo4jImporter.addNeuronLabels(dataset, parameters.neuronThreshold);
                        LOG.info("Adding :Neuron labels took: " + timer.stop());
                        timer.reset();
                    }

                    timer.start();
                    neo4jImporter.indexBooleanRoiProperties(dataset);
                    LOG.info("Adding indices on boolean roi properties took: " + timer.stop());
                    timer.reset();

                    timer.start();
                    neo4jImporter.createMetaNodeWithDataModelNode(dataset, dataModelVersion, preHPThreshold, postHPThreshold);
                    LOG.info("Adding :Meta node took: " + timer.stop());
                    timer.reset();

                    if (parameters.dvidUuid != null) {
                        neo4jImporter.addDvidUuid(dataset, parameters.dvidUuid);
                    }

                    if (parameters.dvidServer != null) {
                        neo4jImporter.addDvidServer(dataset, parameters.dvidServer);
                    }

                    if (parameters.addClusterNames) {
                        neo4jImporter.addClusterNames(dataset, .1F);
                    }

                }
            }

            if (parameters.addSkeletons) {

                final File folder = new File(parameters.skeletonDirectory);
                final File[] arrayOfSwcFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".swc"));

                assert arrayOfSwcFiles != null : "No swc files found.";
                LOG.info("Reading in " + arrayOfSwcFiles.length + " swc files.");

                final List<Skeleton> skeletonList = createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                    if (parameters.prepDatabase && !(parameters.loadNeurons || parameters.addNeuronsAndSynapses || parameters.loadSynapses)) {
                        neo4jImporter.prepDatabase(dataset);
                    }

                    Stopwatch timer = Stopwatch.createStarted();
                    neo4jImporter.addSkeletonNodes(dataset, skeletonList);
                    LOG.info("Loading all Skeleton nodes took: " + timer.stop());
                    timer.reset();
                }

            }

            if (parameters.indexBooleanRoiPropertiesOnly) {
                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
                    neo4jImporter.prepDatabase(dataset);
                    neo4jImporter.indexBooleanRoiProperties(dataset);
                }
            }

            if (parameters.addMetaNodeOnly) {
                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
                    neo4jImporter.prepDatabase(dataset);
                    neo4jImporter.createMetaNodeWithDataModelNode(dataset, dataModelVersion, preHPThreshold, postHPThreshold);
                }
            }

            if (parameters.getSuperLevelRoisFromSynapses) {

                Stopwatch timer = Stopwatch.createStarted();
                final SynapseMapper mapper = new SynapseMapper();
                bodyList = mapper.loadAndMapBodies(parameters.synapseJson);
                LOG.info("Number of bodies with synapses: " + bodyList.size());
                LOG.info("Reading in synapse JSON took: " + timer.stop());
                timer.reset();

                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
                    neo4jImporter.setSuperLevelRois(dataset, bodyList);
                }
            }

            if (parameters.addAutoNamesOnly) {

                Stopwatch timer = Stopwatch.createStarted();
                try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
                    neo4jImporter.prepDatabase(dataset);
                    if (parameters.neuronThreshold != null) {
                        neo4jImporter.addAutoNamesAndNeuronLabels(dataset, parameters.neuronThreshold);
                    } else {
                        neo4jImporter.addAutoNamesAndNeuronLabels(dataset, 10);
                    }
                    LOG.info("Adding autoNames took: " + timer.stop());
                    timer.reset();
                }

            }
        } catch (Exception e) {
            LOG.error("An error occurred: ", e);
        }

        if (parameters.editMode) {

        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(NeuPrinterMain.class);

}





