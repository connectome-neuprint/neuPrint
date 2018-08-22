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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NeuPrinterMain {

    @Parameters(separators = "=")
    public static class NeuPrinterParameters {

        @Parameter(
                names = "--dbProperties",
                description = "Properties file containing database information (required)",
                required = true)
        public String dbProperties;

        @Parameter(
                names = "--loadNeurons",
                description = "Indicates that data from neurons json should be loaded to database (omit to skip)",
                required = false,
                arity = 0)
        public boolean loadNeurons;

        @Parameter(
                names = "--loadSynapses",
                description = "Indicates that data from synapses json should be loaded to database (omit to skip)",
                required = false,
                arity = 0)
        public boolean loadSynapses;

        @Parameter(
                names = "--doAll",
                description = "Indicates that both Neurons and Synapses jsons should be loaded and all database features added",
                required = false,
                arity = 0)
        public boolean doAll;

        @Parameter(
                names = "--prepDatabase",
                description = "Indicates that database constraints and indexes should be setup (omit to skip)",
                required = false,
                arity = 0)
        public boolean prepDatabase;

        @Parameter(
                names = "--addConnectsTo",
                description = "Indicates that ConnectsTo relations should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addConnectsTo;

        @Parameter(
                names = "--addSynapses",
                description = "Indicates that synapse nodes should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addSynapses;

        @Parameter(
                names = "--addSynapsesTo",
                description = "Indicates that SynapsesTo relations should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addSynapsesTo;

        @Parameter(
                names = "--addNeuronRois",
                description = "Indicates that neuron ROI labels should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addNeuronRois;

        @Parameter(
                names = "--addSynapseSets",
                description = "Indicates that synapse set nodes should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addSynapseSets;

        @Parameter(
                names = "--addSkeletons",
                description = "Indicates that skeleton nodes should be added (omit to skip)",
                required = false,
                arity = 0)
        public boolean addSkeletons;

        @Parameter(
                names = "--neuronJson",
                description = "JSON file containing neuron data to import",
                required = false)
        public String neuronJson;

        @Parameter(
                names = "--datasetLabel",
                description = "Dataset value for all nodes (required)",
                required = true)
        public String datasetLabel;

        @Parameter(
                names = "--dataModelVersion",
                description = "Data model version (required)",
                required = true)
        public float dataModelVersion;

        @Parameter(
                names = "--synapseJson",
                description = "JSON file containing body synapse data to import",
                required = false)
        public String synapseJson;

        @Parameter(
                names = "--skeletonDirectory",
                description = "Path to directory containing skeleton files for this dataset",
                required = false)
        public String skeletonDirectory;

        @Parameter(
                names = "--editMode",
                description = "Indicates that neuprinter is being used in edit mode to alter data in an existing database (omit to skip).",
                required = false,
                arity = 0)
        public boolean editMode;

        @Parameter(
                names = "--addMetaNodeOnly",
                description = "Indicates that only the Meta Node should be added for this dataset. Requires the existing dataset to be completely loaded into neo4j. (omit to skip)",
                required = false,
                arity = 0)
        public boolean addMetaNodeOnly;

        @Parameter(
                names = "--addAutoNamesOnly",
                description = "Indicates that only the autoNames should be added for this dataset. Requires the existing dataset to be completely loaded into neo4j. Names are only generated for neurons that have greater than the number of synapses" +
                        "indicated by autoNameThreshold (omit to skip)",
                required = false,
                arity = 0
        )
        public boolean addAutoNamesOnly;

        @Parameter(
                names = "--addAutoNames",
                description = "Indicates that automatically generated names should be added for this dataset. Auto-names are in the format " +
                        "ROIA-ROIB-8 where ROIA is the roi in which a given neuron has the most inputs (postsynaptic densities) " +
                        "and ROIB is the roi in which a neuron has the most outputs (presynaptic densities). The final number renders " +
                        "this name unique per dataset. Names are only generated for neurons that have greater than the number of synapses " +
                        "indicated by autoNameThreshold. If neurons do not already have a name, the auto-name is added to the name property. (skip to omit)",
                required = false,
                arity = 0)
        public boolean addAutoNames;

        @Parameter(
                names = "--autoNameThreshold",
                description = "Integer indicating the number of (presynaptic densities + postsynaptic densities) a neuron should have to be given an " +
                        "auto-name (default is 10). Must have --addAutoName OR --addAutoNamesOnly enabled.",
                required = false)
        public Integer autoNameThreshold;

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

    private static List<Neuron> neuronList;
    private static List<BodyWithSynapses> bodyList;

    public static List<Neuron> readNeuronsJson(String filepath) {

        try (BufferedReader reader = new BufferedReader(new FileReader(filepath))) {
            neuronList = Neuron.fromJson(reader);
            LOG.info("Number of neurons: " + neuronList.size());
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

    public static Long setSkeletonAssociatedBodyId(String swcFilePath) {

        String patternSurroundingId = ".*/(.*?).swc";
        Pattern r = Pattern.compile(patternSurroundingId);
        Matcher mR = r.matcher(swcFilePath);
        mR.matches();
        Long associatedBodyId = Long.parseLong(mR.group(1));
        return associatedBodyId;

    }

    public static void main(String[] args) {

        final NeuPrinterParameters parameters = new NeuPrinterParameters();
        final JCommander jCommander = new JCommander(parameters);
        jCommander.setProgramName("java -cp neuprinter.jar " + NeuPrinterMain.class.getName());

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

        String dataset = parameters.datasetLabel;
        float dataModelVersion = parameters.dataModelVersion;

        LOG.info("Dataset is: " + dataset);

        if (parameters.loadNeurons || parameters.doAll) {

            // read in the neurons data
            Stopwatch timer2 = Stopwatch.createStarted();
            neuronList = readNeuronsJson(parameters.neuronJson);
            LOG.info("Reading in neurons json took: " + timer2.stop());
            timer2.reset();
            //write it to the database
            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                if (parameters.prepDatabase || parameters.doAll) {
                    neo4jImporter.prepDatabase(dataset);
                }

                Stopwatch timer = Stopwatch.createStarted();
                neo4jImporter.addNeurons(dataset, neuronList);
                LOG.info("Loading all Neuron nodes took: " + timer.stop());
                timer.reset();
            }
        }

        if (parameters.loadSynapses || parameters.doAll) {

            Stopwatch timer = Stopwatch.createStarted();
            SynapseMapper mapper = new SynapseMapper();
            bodyList = mapper.loadAndMapBodies(parameters.synapseJson);
            LOG.info("Number of bodies with synapses: " + bodyList.size());
            LOG.info("Reading in synapse json took: " + timer.stop());
            timer.reset();

            HashMap<String, List<String>> preToPost = mapper.getPreToPostMap();

            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                if (parameters.prepDatabase && !(parameters.loadNeurons || parameters.doAll)) {
                    neo4jImporter.prepDatabase(dataset);
                }

                if (parameters.addConnectsTo || parameters.doAll) {

                    timer.start();
                    neo4jImporter.addConnectsTo(dataset, bodyList);
                    LOG.info("Loading all ConnectsTo took: " + timer.stop());
                    timer.reset();

                }

                if (parameters.addSynapses || parameters.doAll) {
                    timer.start();
                    neo4jImporter.addSynapsesWithRois(dataset, bodyList);
                    LOG.info("Loading all Synapses took: " + timer.stop());
                    timer.reset();
                }

                if (parameters.addSynapsesTo || parameters.doAll) {
                    timer.start();
                    neo4jImporter.addSynapsesTo(dataset, preToPost);
                    LOG.info("Loading all SynapsesTo took: " + timer.stop());
                    timer.reset();
                }

                if (parameters.addNeuronRois || parameters.doAll) {
                    timer.start();
                    neo4jImporter.addNeuronRois(dataset, bodyList);
                    LOG.info("Loading all Neuron ROI labels took: " + timer.stop());
                    timer.reset();
                }

                if (parameters.addSynapseSets || parameters.doAll) {
                    timer.start();
                    neo4jImporter.addSynapseSets(dataset, bodyList);
                    LOG.info("Loading SynapseSets took: " + timer.stop());
                    timer.reset();
                }

            }
        }

        if (parameters.doAll) {
            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                Stopwatch timer = Stopwatch.createUnstarted();

                if (parameters.addAutoNames) {
                    timer.start();
                    if (parameters.autoNameThreshold != null) {
                        neo4jImporter.addAutoNames(dataset, parameters.autoNameThreshold);
                    } else {
                        neo4jImporter.addAutoNames(dataset, 10);
                    }
                    LOG.info("Adding autoNames took: " + timer.stop());
                    timer.reset();
                }

                timer.start();
                neo4jImporter.createMetaNodeWithDataModelNode(dataset, dataModelVersion);
                LOG.info("Adding :Meta node took: " + timer.stop());
                timer.reset();
            }
        }

        if (parameters.addSkeletons) {

            File folder = new File(parameters.skeletonDirectory);
            File[] arrayOfSwcFiles = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".swc"));

            LOG.info("Reading in " + arrayOfSwcFiles.length + " swc files.");

            List<Skeleton> skeletonList = createSkeletonListFromSwcFileArray(arrayOfSwcFiles);

            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {

                if (parameters.prepDatabase && !(parameters.loadNeurons || parameters.doAll || parameters.loadSynapses)) {
                    neo4jImporter.prepDatabase(dataset);
                }

                Stopwatch timer = Stopwatch.createStarted();
                neo4jImporter.addSkeletonNodes(dataset, skeletonList);
                LOG.info("Loading all Skeleton nodes took: " + timer.stop());
                timer.reset();
            }

        }

        if (parameters.addMetaNodeOnly) {
            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
                neo4jImporter.prepDatabase(dataset);
                neo4jImporter.createMetaNodeWithDataModelNode(dataset, dataModelVersion);
            }
        }

        if (parameters.addAutoNamesOnly) {

            Stopwatch timer = Stopwatch.createStarted();
            try (Neo4jImporter neo4jImporter = new Neo4jImporter(parameters.getDbConfig())) {
                neo4jImporter.prepDatabase(dataset);
                if (parameters.autoNameThreshold != null) {
                    neo4jImporter.addAutoNames(dataset, parameters.autoNameThreshold);
                } else {
                    neo4jImporter.addAutoNames(dataset, 10);
                }
                LOG.info("Adding autoNames took: " + timer.stop());
                timer.reset();
            }

        }

        if (parameters.editMode) {

            neuronList = readNeuronsJson(parameters.neuronJson);

            try (Neo4jEditor neo4jEditor = new Neo4jEditor(parameters.getDbConfig())) {
                neo4jEditor.updateNeuronProperties(dataset, neuronList);
            }

        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(NeuPrinterMain.class);

}





