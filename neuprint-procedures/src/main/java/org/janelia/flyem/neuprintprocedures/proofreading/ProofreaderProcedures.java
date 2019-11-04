package org.janelia.flyem.neuprintprocedures.proofreading;

import com.google.common.base.Stopwatch;
import org.janelia.flyem.neuprint.json.JsonUtils;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.SkelNode;
import org.janelia.flyem.neuprint.model.Skeleton;
import org.janelia.flyem.neuprint.model.Synapse;
import org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools;
import org.janelia.flyem.neuprintloadprocedures.Location;
import org.janelia.flyem.neuprintloadprocedures.model.RoiInfo;
import org.janelia.flyem.neuprintloadprocedures.model.SynapseCounter;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.*;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.addConnectsToRelationship;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.addSynapseToRoiInfo;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.addSynapseToRoiInfoWithHP;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.addSynapseToSynapseSet;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.convertSegmentToNeuron;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.getMetaNodeRoiSet;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.getPreAndPostHPThresholdFromMetaNode;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.getRoiInfoAsMap;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.incrementSegmentPostCount;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.incrementSegmentPreCount;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.removeSynapseFromRoiInfoWithHP;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.setConnectionSetRoiInfoAndGetWeightAndWeightHP;
import static org.janelia.flyem.neuprintloadprocedures.procedures.LoadingProcedures.setConnectionSetRoiInfoWeightAndWeightHP;

public class ProofreaderProcedures {

    @Context
    public GraphDatabaseService dbService;
    @Context
    public Log log;

    @Procedure(value = "proofreader.updateProperties", mode = Mode.WRITE)
    @Description("proofreader.updateProperties(neuronJsonObject, dataset) : Update properties on a Neuron/Segment node. Supports adding status, type, name, size, and soma. Input JSON should follow specifications for \"Neurons\" JSON file supply a single Neuron/Segment object as a string: https://github.com/connectome-neuprint/neuPrint/blob/master/jsonspecs.md")
    public void updateProperties(@Name("neuronJsonObject") String neuronJsonObject, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.updateProperties: entry");

        try {

            if (neuronJsonObject == null || datasetLabel == null) {
                log.error("proofreader.updateProperties: Missing input arguments.");
                throw new RuntimeException("proofreader.updateProperties: Missing input arguments.");
            }

            Neuron neuron = JsonUtils.GSON.fromJson(neuronJsonObject, Neuron.class);

            Node neuronNode = getSegment(dbService, neuron.getId(), datasetLabel);

            Node metaNode = getMetaNode(dbService, datasetLabel);
            if (metaNode == null) {
                log.error("Meta node not found for dataset: " + datasetLabel);
                throw new RuntimeException("Meta node not found for dataset: " + datasetLabel);
            }
            acquireWriteLockForNode(metaNode);

            if (neuronNode == null) {
                log.warn("Neuron with id " + neuron.getId() + " not found in database. Aborting update.");
            } else {

                acquireWriteLockForNode(neuronNode);

                boolean isNeuron = false;

                if (neuron.getStatus() != null) {
                    neuronNode.setProperty(STATUS, neuron.getStatus());
                    // adding a status makes it a Neuron
                    isNeuron = true;
                    log.info("Updated status for neuron " + neuron.getId() + ".");
                }

                if (neuron.getName() != null) {
                    neuronNode.setProperty(NAME, neuron.getName());
                    // adding a name makes it a Neuron
                    isNeuron = true;
                    log.info("Updated name for neuron " + neuron.getId() + ".");
                }

                if (neuron.getInstance() != null) {
                    neuronNode.setProperty(INSTANCE, neuron.getInstance());
                    // adding an instance makes it a Neuron
                    isNeuron = true;
                    log.info("Updated instance for neuron " + neuron.getId() + ".");
                }

                if (neuron.getPrimaryNeurite() != null) {
                    neuronNode.setProperty(PRIMARY_NEURITE, neuron.getPrimaryNeurite());
                    isNeuron = true;
                    log.info("Updated primaryNeurite for neuron " + neuron.getId() + ".");
                }

                if (neuron.getMajorInput() != null) {
                    neuronNode.setProperty(MAJOR_INPUT, neuron.getMajorInput());
                    isNeuron = true;
                    log.info("Updated majorInput for neuron " + neuron.getId() + ".");
                }

                if (neuron.getMajorOutput() != null) {
                    neuronNode.setProperty(MAJOR_OUTPUT, neuron.getMajorOutput());
                    isNeuron = true;
                    log.info("Updated majorOutput for neuron " + neuron.getId() + ".");
                }

                if (neuron.getClonalUnit() != null) {
                    neuronNode.setProperty(CLONAL_UNIT, neuron.getClonalUnit());
                    isNeuron = true;
                    log.info("Updated clonalUnit for neuron " + neuron.getId() + ".");
                }

                if (neuron.getNeurotransmitter() != null) {
                    neuronNode.setProperty(NEUROTRANSMITTER, neuron.getNeurotransmitter());
                    isNeuron = true;
                    log.info("Updated neurotransmitter for neuron " + neuron.getId() + ".");
                }

                if (neuron.getSize() != null) {
                    neuronNode.setProperty(SIZE, neuron.getSize());
                    log.info("Updated size for neuron " + neuron.getId() + ".");
                }

                if (neuron.getSoma() != null) {
                    org.janelia.flyem.neuprint.model.Location somaLocation = neuron.getSoma().getLocation();
                    Point somaLocationPoint = new Location(somaLocation.getX(), somaLocation.getY(), somaLocation.getZ());
                    neuronNode.setProperty(SOMA_LOCATION, somaLocationPoint);
                    neuronNode.setProperty(SOMA_RADIUS, neuron.getSoma().getRadius());
                    log.info("Updated soma for neuron " + neuron.getId() + ".");

                    //adding a soma makes it a Neuron
                    isNeuron = true;
                }

                if (neuron.getType() != null) {
                    neuronNode.setProperty(TYPE, neuron.getType());
                    log.info("Updated type for neuron " + neuron.getId() + ".");
                }

                if (isNeuron) {
                    convertSegmentToNeuron(neuronNode, datasetLabel, metaNode);
                }
            }

        } catch (Exception e) {
            log.error("Error running proofreader.updateProperties: " + e);
            throw new RuntimeException("Error running proofreader.updateProperties: " + e);
        }

        log.info("proofreader.updateProperties: exit");

    }

    @Procedure(value = "proofreader.deleteSoma", mode = Mode.WRITE)
    @Description("proofreader.deleteSoma(bodyId, datasetLabel): Delete soma (radius and location) from Neuron/Segment node.")
    public void deleteSoma(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteSoma: entry");

        try {

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteSoma: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteSoma: Missing input arguments.");
            }

            // get the neuron node
            Node neuronNode = getSegment(dbService, bodyId, datasetLabel);

            if (neuronNode == null) {
                log.warn("Neuron with id " + bodyId + " not found in database. Aborting deletion of soma.");
            } else {

                acquireWriteLockForNode(neuronNode);

                // delete soma radius
                neuronNode.removeProperty(SOMA_RADIUS);

                // delete soma location
                neuronNode.removeProperty(SOMA_LOCATION);

                // check if it should still be labeled neuron and remove designation if necessary
                if (shouldNotBeLabeledNeuron(neuronNode)) {
                    removeNeuronDesignationFromNode(neuronNode, datasetLabel);
                }

                log.info("Successfully deleted soma information from " + bodyId);
            }

        } catch (Exception e) {
            log.error("Error running proofreader.deleteSoma: " + e);
            throw new RuntimeException("Error running proofreader.deleteSoma: " + e);
        }

        log.info("proofreader.deleteSoma: exit");

    }

    @Procedure(value = "proofreader.deleteName", mode = Mode.WRITE)
    @Description("proofreader.deleteName(bodyId, datasetLabel): Delete name from Neuron/Segment node.")
    public void deleteName(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteName: entry");

        try {

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteName: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteName: Missing input arguments.");
            }

            // get the neuron node
            Node neuronNode = getSegment(dbService, bodyId, datasetLabel);

            if (neuronNode == null) {
                log.warn("Neuron with id " + bodyId + " not found in database. Aborting deletion of name.");
            } else {

                acquireWriteLockForNode(neuronNode);

                // delete name
                neuronNode.removeProperty(NAME);

                // check if it should still be labeled neuron and remove designation if necessary
                if (shouldNotBeLabeledNeuron(neuronNode)) {
                    removeNeuronDesignationFromNode(neuronNode, datasetLabel);
                }

                log.info("Successfully deleted name information from " + bodyId);
            }

        } catch (Exception e) {
            log.error("Error running proofreader.deleteName: " + e);
            throw new RuntimeException("Error running proofreader.deleteName: " + e);
        }

        log.info("proofreader.deleteName: exit");
    }

    @Procedure(value = "proofreader.deleteInstance", mode = Mode.WRITE)
    @Description("proofreader.deleteInstance(bodyId, datasetLabel): Delete instance from Neuron/Segment node.")
    public void deleteInstance(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteInstance: entry");

        try {

            //TODO: refactor this common logic into shared method

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteInstance: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteInstance: Missing input arguments.");
            }

            // get the neuron node
            Node neuronNode = getSegment(dbService, bodyId, datasetLabel);

            if (neuronNode == null) {
                log.warn("Neuron with id " + bodyId + " not found in database. Aborting deletion of instance.");
            } else {

                acquireWriteLockForNode(neuronNode);

                // delete instance
                neuronNode.removeProperty(INSTANCE);

                // check if it should still be labeled neuron and remove designation if necessary
                if (shouldNotBeLabeledNeuron(neuronNode)) {
                    removeNeuronDesignationFromNode(neuronNode, datasetLabel);
                }

                log.info("Successfully deleted instance information from " + bodyId);
            }

        } catch (Exception e) {
            log.error("Error running proofreader.deleteInstance: " + e);
            throw new RuntimeException("Error running proofreader.deleteInstance: " + e);
        }

        log.info("proofreader.deleteInstance: exit");
    }

    @Procedure(value = "proofreader.deletePrimaryNeurite", mode = Mode.WRITE)
    @Description("proofreader.deletePrimaryNeurite(bodyId, datasetLabel): Delete primaryNeurite from Neuron/Segment node.")
    public void deletePrimaryNeurite(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deletePrimaryNeurite: entry");

        try {

            //TODO: refactor this common logic into shared method

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deletePrimaryNeurite: Missing input arguments.");
                throw new RuntimeException("proofreader.deletePrimaryNeurite: Missing input arguments.");
            }

            // get the neuron node
            Node neuronNode = getSegment(dbService, bodyId, datasetLabel);

            if (neuronNode == null) {
                log.warn("Neuron with id " + bodyId + " not found in database. Aborting deletion of primaryNeurite.");
            } else {

                acquireWriteLockForNode(neuronNode);

                // delete primaryNeurite
                neuronNode.removeProperty(PRIMARY_NEURITE);

                // check if it should still be labeled neuron and remove designation if necessary
                if (shouldNotBeLabeledNeuron(neuronNode)) {
                    removeNeuronDesignationFromNode(neuronNode, datasetLabel);
                }

                log.info("Successfully deleted primaryNeurite information from " + bodyId);
            }

        } catch (Exception e) {
            log.error("Error running proofreader.deletePrimaryNeurite: " + e);
            throw new RuntimeException("Error running proofreader.deletePrimaryNeurite: " + e);
        }

        log.info("proofreader.deletePrimaryNeurite: exit");
    }

    @Procedure(value = "proofreader.deleteMajorInput", mode = Mode.WRITE)
    @Description("proofreader.deleteMajorInput(bodyId, datasetLabel): Delete majorInput from Neuron/Segment node.")
    public void deleteMajorInput(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteMajorInput: entry");

        try {

            //TODO: refactor this common logic into shared method

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteMajorInput: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteMajorInput: Missing input arguments.");
            }

            // get the neuron node
            Node neuronNode = getSegment(dbService, bodyId, datasetLabel);

            if (neuronNode == null) {
                log.warn("Neuron with id " + bodyId + " not found in database. Aborting deletion of majorInput.");
            } else {

                acquireWriteLockForNode(neuronNode);

                // delete majorInput
                neuronNode.removeProperty(MAJOR_INPUT);

                // check if it should still be labeled neuron and remove designation if necessary
                if (shouldNotBeLabeledNeuron(neuronNode)) {
                    removeNeuronDesignationFromNode(neuronNode, datasetLabel);
                }

                log.info("Successfully deleted majorInput information from " + bodyId);
            }

        } catch (Exception e) {
            log.error("Error running proofreader.deleteMajorInput: " + e);
            throw new RuntimeException("Error running proofreader.deleteMajorInput: " + e);
        }

        log.info("proofreader.deleteMajorInput: exit");
    }

    @Procedure(value = "proofreader.deleteMajorOutput", mode = Mode.WRITE)
    @Description("proofreader.deleteMajorOutput(bodyId, datasetLabel): Delete majorOutput from Neuron/Segment node.")
    public void deleteMajorOutput(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteMajorOutput: entry");

        try {

            //TODO: refactor this common logic into shared method

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteMajorOutput: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteMajorOutput: Missing input arguments.");
            }

            // get the neuron node
            Node neuronNode = getSegment(dbService, bodyId, datasetLabel);

            if (neuronNode == null) {
                log.warn("Neuron with id " + bodyId + " not found in database. Aborting deletion of majorOutput.");
            } else {

                acquireWriteLockForNode(neuronNode);

                // delete majorOutput
                neuronNode.removeProperty(MAJOR_OUTPUT);

                // check if it should still be labeled neuron and remove designation if necessary
                if (shouldNotBeLabeledNeuron(neuronNode)) {
                    removeNeuronDesignationFromNode(neuronNode, datasetLabel);
                }

                log.info("Successfully deleted majorOutput information from " + bodyId);
            }

        } catch (Exception e) {
            log.error("Error running proofreader.deleteMajorOutput: " + e);
            throw new RuntimeException("Error running proofreader.deleteMajorOutput: " + e);
        }

        log.info("proofreader.deleteMajorOutput: exit");
    }

    @Procedure(value = "proofreader.deleteClonalUnit", mode = Mode.WRITE)
    @Description("proofreader.deleteClonalUnit(bodyId, datasetLabel): Delete clonalUnit from Neuron/Segment node.")
    public void deleteClonalUnit(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteClonalUnit: entry");

        try {

            //TODO: refactor this common logic into shared method

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteClonalUnit: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteClonalUnit: Missing input arguments.");
            }

            // get the neuron node
            Node neuronNode = getSegment(dbService, bodyId, datasetLabel);

            if (neuronNode == null) {
                log.warn("Neuron with id " + bodyId + " not found in database. Aborting deletion of clonalUnit.");
            } else {

                acquireWriteLockForNode(neuronNode);

                // delete clonalUnit
                neuronNode.removeProperty(CLONAL_UNIT);

                // check if it should still be labeled neuron and remove designation if necessary
                if (shouldNotBeLabeledNeuron(neuronNode)) {
                    removeNeuronDesignationFromNode(neuronNode, datasetLabel);
                }

                log.info("Successfully deleted clonalUnit information from " + bodyId);
            }

        } catch (Exception e) {
            log.error("Error running proofreader.deleteClonalUnit: " + e);
            throw new RuntimeException("Error running proofreader.deleteClonalUnit: " + e);
        }

        log.info("proofreader.deleteClonalUnit: exit");
    }

    @Procedure(value = "proofreader.deleteNeurotransmitter", mode = Mode.WRITE)
    @Description("proofreader.deleteNeurotransmitter(bodyId, datasetLabel): Delete neurotransmitter from Neuron/Segment node.")
    public void deleteNeurotransmitter(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteNeurotransmitter: entry");

        try {

            //TODO: refactor this common logic into shared method

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteNeurotransmitter: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteNeurotransmitter: Missing input arguments.");
            }

            // get the neuron node
            Node neuronNode = getSegment(dbService, bodyId, datasetLabel);

            if (neuronNode == null) {
                log.warn("Neuron with id " + bodyId + " not found in database. Aborting deletion of neurotransmitter.");
            } else {

                acquireWriteLockForNode(neuronNode);

                // delete neurotransmitter
                neuronNode.removeProperty(NEUROTRANSMITTER);

                // check if it should still be labeled neuron and remove designation if necessary
                if (shouldNotBeLabeledNeuron(neuronNode)) {
                    removeNeuronDesignationFromNode(neuronNode, datasetLabel);
                }

                log.info("Successfully deleted neurotransmitter information from " + bodyId);
            }

        } catch (Exception e) {
            log.error("Error running proofreader.deleteNeurotransmitter: " + e);
            throw new RuntimeException("Error running proofreader.deleteNeurotransmitter: " + e);
        }

        log.info("proofreader.deleteNeurotransmitter: exit");
    }

    @Procedure(value = "proofreader.deleteStatus", mode = Mode.WRITE)
    @Description("proofreader.deleteStatus(bodyId, datasetLabel): Delete status from Neuron/Segment node.")
    public void deleteStatus(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteStatus: entry");

        try {

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteStatus: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteStatus: Missing input arguments.");
            }

            // get the neuron node
            Node neuronNode = getSegment(dbService, bodyId, datasetLabel);

            if (neuronNode == null) {
                log.warn("Neuron with id " + bodyId + " not found in database. Aborting deletion of status.");
            } else {

                acquireWriteLockForNode(neuronNode);

                // delete status
                neuronNode.removeProperty(STATUS);

                // check if it should still be labeled neuron and remove designation if necessary
                if (shouldNotBeLabeledNeuron(neuronNode)) {
                    removeNeuronDesignationFromNode(neuronNode, datasetLabel);
                }

                log.info("Successfully deleted status information from " + bodyId);
            }

        } catch (Exception e) {
            log.error("Error running proofreader.deleteStatus: " + e);
            throw new RuntimeException("Error running proofreader.deleteStatus: " + e);
        }

        log.info("proofreader.deleteStatus: exit");
    }

    @Procedure(value = "proofreader.deleteType", mode = Mode.WRITE)
    @Description("proofreader.deleteType(bodyId, datasetLabel): Delete type from Neuron/Segment node.")
    public void deleteType(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteType: entry");

        try {

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteType: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteType: Missing input arguments.");
            }

            // get the neuron node
            Node neuronNode = getSegment(dbService, bodyId, datasetLabel);

            if (neuronNode == null) {
                log.warn("Neuron with id " + bodyId + " not found in database. Aborting deletion of type.");
            } else {

                acquireWriteLockForNode(neuronNode);

                // delete type
                neuronNode.removeProperty(TYPE);

                log.info("Successfully deleted type information from " + bodyId);
            }

        } catch (Exception e) {
            log.error("Error running proofreader.deleteType: " + e);
            throw new RuntimeException("Error running proofreader.deleteTypes: " + e);
        }

        log.info("proofreader.deleteType: exit");
    }

    @Procedure(value = "proofreader.addNeuron", mode = Mode.WRITE)
    @Description("proofreader.addNeuron(neuronAdditionJsonObject, datasetLabel): add a Neuron/Segment with properties, synapses, and connections specified by an input JSON (see https://github.com/connectome-neuprint/neuPrint/blob/master/graphupdateAPI.md) ")
    public void addNeuron(@Name("neuronAdditionJson") String neuronAdditionJson, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.addNeuron: entry");

        try {

            if (neuronAdditionJson == null || datasetLabel == null) {
                log.error("proofreader.addNeuron: Missing input arguments.");
                throw new RuntimeException("proofreader.addNeuron: Missing input arguments.");
            }

            NeuronAddition neuronAddition = JsonUtils.GSON.fromJson(neuronAdditionJson, NeuronAddition.class);

            if (neuronAddition.getMutationUuid() == null || neuronAddition.getBodyId() == null) {
                log.error("proofreader.addNeuron: body id and uuid are required fields in the neuron addition json.");
                throw new RuntimeException("proofreader.addNeuron: body id and uuid are required fields in the neuron addition json.");
            }

            if (neuronAddition.getMutationId() == null) {
                neuronAddition.setToInitialMutationId();
            }

            // check that this mutation hasn't been done before (in order to be unique, needs to include uuid+mutationid+bodyId)
            String mutationKey = neuronAddition.getMutationUuid() + ":" + neuronAddition.getMutationId() + ":" + neuronAddition.getBodyId();
            Node existingMutatedNode = dbService.findNode(Label.label(datasetLabel + "-" + SEGMENT), MUTATION_UUID_ID, mutationKey);
            if (existingMutatedNode != null) {
                log.error("Mutation already found in the database: " + neuronAddition.toString());
                throw new RuntimeException("Mutation already found in the database: " + neuronAddition.toString());
            }

            log.info("Beginning addition: " + neuronAddition);
            // create a new node and synapse set for that node
            final long newNeuronBodyId = neuronAddition.getBodyId();
            final Node newNeuron = dbService.createNode(Label.label(SEGMENT),
                    Label.label(datasetLabel),
                    Label.label(datasetLabel + "-" + SEGMENT));

            try {
                newNeuron.setProperty(BODY_ID, newNeuronBodyId);
            } catch (org.neo4j.graphdb.ConstraintViolationException cve) {
                log.error("Body id " + newNeuronBodyId + " already exists in database. Aborting addition for mutation with id : " + mutationKey);
                throw new RuntimeException("Body id " + newNeuronBodyId + " already exists in database. Aborting addition for mutation with id : " + mutationKey);
            }

            Node metaNode = GraphTraversalTools.getMetaNode(dbService, datasetLabel);
            if (metaNode == null) {
                log.error("Meta node not found for dataset: " + datasetLabel);
                throw new RuntimeException("Meta node not found for dataset: " + datasetLabel);
            }
            acquireWriteLockForNode(metaNode);
            Set<String> metaNodeRoiSet = getMetaNodeRoiSet(metaNode);

            final Node newSynapseSet = createSynapseSetForSegment(newNeuron, datasetLabel);

            // add appropriate synapses via synapse sets; add each synapse to the new body's synapseset
            // completely add everything here so that there's nothing left on the synapse store
            // add the new body id to the synapse sources

            Set<Synapse> currentSynapses = neuronAddition.getCurrentSynapses();
            long preCount = 0L;
            long postCount = 0L;

            if (currentSynapses != null) {
                Set<Synapse> notFoundSynapses = new HashSet<>(currentSynapses);

                // from synapses, derive connectsto, connection sets, rois/roiInfo, pre/post counts
                final ConnectsToRelationshipMap connectsToRelationshipMap = new ConnectsToRelationshipMap();
                final RoiInfo roiInfo = new RoiInfo();

                for (Synapse synapse : currentSynapses) {

                    // get the synapse by location
                    org.janelia.flyem.neuprint.model.Location synapseLocation = synapse.getLocation();
                    Point synapseLocationPoint = new Location(synapseLocation.getX(), synapseLocation.getY(), synapseLocation.getZ());
                    Node synapseNode = getSynapse(dbService, synapseLocationPoint, datasetLabel);

                    if (synapseNode == null) {
                        log.error("Synapse not found in database: " + synapse);
                        throw new RuntimeException("Synapse not found in database: " + synapse);
                    }

                    if (synapseNode.hasRelationship(RelationshipType.withName(CONTAINS))) {
                        Node bodyWithSynapse = getSegmentThatContainsSynapse(synapseNode);
                        Long bodyWithSynapseId;
                        try {
                            bodyWithSynapseId = (Long) bodyWithSynapse.getProperty(BODY_ID);
                        } catch (Exception e) {
                            log.error("Error retrieving body ID from segment with Neo4j ID " + bodyWithSynapse.getId() + ": " + e);
                            throw new RuntimeException("Error retrieving body ID from segment with Neo4j ID " + bodyWithSynapse.getId() + ": " + e);

                        }
                        log.error("Synapse is already assigned to another body. body id: " + bodyWithSynapseId + ", synapse: " + synapse);
                        throw new RuntimeException("Synapse is already assigned to another body. body id: " + bodyWithSynapseId + ", synapse: " + synapse);
                    }

                    // add synapse to the new synapse set
                    newSynapseSet.createRelationshipTo(synapseNode, RelationshipType.withName(CONTAINS));
                    // remove this synapse from the not found set
                    notFoundSynapses.remove(synapse);

                    // get the synapse type
                    String synapseType;
                    if (synapseNode.hasProperty(TYPE)) {
                        synapseType = (String) synapseNode.getProperty(TYPE);
                    } else {
                        log.error(String.format("Synapse at location [%d,%d,%d] does not have type property.", synapseLocation.getX(), synapseLocation.getY(), synapseLocation.getZ()));
                        throw new RuntimeException(String.format("Synapse at location [%d,%d,%d] does not have type property.", synapseLocation.getX(), synapseLocation.getY(), synapseLocation.getZ()));
                    }

                    // get synapse rois for adding to the body and roiInfo
                    final Set<String> synapseRois = getSynapseRois(synapseNode, metaNodeRoiSet);

                    if (synapseType.equals(PRE)) {
                        for (String roi : synapseRois) {
                            roiInfo.incrementPreForRoi(roi);
                        }
                        preCount++;
                    } else if (synapseType.equals(POST)) {
                        for (String roi : synapseRois) {
                            roiInfo.incrementPostForRoi(roi);
                        }
                        postCount++;
                    } else {
                        log.error(String.format("Synapse at location [%d,%d,%d] does not have type property equal to 'pre' or 'post'.", synapseLocation.getX(), synapseLocation.getY(), synapseLocation.getZ()));
                        throw new RuntimeException(String.format("Synapse at location [%d,%d,%d] does not have type property equal to 'pre' or 'post'.", synapseLocation.getX(), synapseLocation.getY(), synapseLocation.getZ()));
                    }

                    if (synapseNode.hasRelationship(RelationshipType.withName(SYNAPSES_TO))) {
                        for (Relationship synapticRelationship : synapseNode.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                            Node synapticPartner = synapticRelationship.getOtherNode(synapseNode);

                            Node connectedSegment = getSegmentThatContainsSynapse(synapticPartner);

                            if (connectedSegment != null) {
                                if (synapseType.equals(PRE)) {
                                    connectsToRelationshipMap.insertSynapsesIntoConnectsToRelationship(newNeuron, connectedSegment, synapseNode, synapticPartner);
                                } else {
                                    connectsToRelationshipMap.insertSynapsesIntoConnectsToRelationship(connectedSegment, newNeuron, synapticPartner, synapseNode);
                                }
                            }
                        }
                    }

                }

                if (!notFoundSynapses.isEmpty()) {
                    log.error("Some synapses were not found for neuron addition. Mutation UUID: " + neuronAddition.getMutationUuid() + " Mutation ID: " + neuronAddition.getMutationId() + " Synapse(s): " + notFoundSynapses);
                    throw new RuntimeException("Some synapses were not found for neuron addition. Mutation UUID: " + neuronAddition.getMutationUuid() + " Mutation ID: " + neuronAddition.getMutationId() + " Synapse(s): " + notFoundSynapses);
                }

                log.info("Found and added all synapses to synapse set for body id " + newNeuronBodyId);
                log.info("Completed making map of ConnectsTo relationships.");

                // add synapse and synaptic partners to connection set; set connectsto relationships
                createConnectionSetsAndConnectsToRelationships(connectsToRelationshipMap, datasetLabel, metaNode, metaNodeRoiSet);
                log.info("Completed creating ConnectionSets and ConnectsTo relationships.");

                // add roi boolean properties and roi info
                addRoiPropertiesToSegmentGivenSynapseCountsPerRoi(newNeuron, roiInfo);
                newNeuron.setProperty(ROI_INFO, roiInfo.getAsJsonString());
                log.info("Completed updating roi information.");

            }
            // set pre and post on body; other properties
            newNeuron.setProperty(PRE, preCount);
            newNeuron.setProperty(POST, postCount);
            newNeuron.setProperty(SIZE, neuronAddition.getSize());
            newNeuron.setProperty(MUTATION_UUID_ID, mutationKey);

            // check for optional properties; add to neuron if present; decide if there should be a neuron label (has name, has soma, has pre+post>10)
            boolean isNeuron = false;
            if (neuronAddition.getStatus() != null) {
                newNeuron.setProperty(STATUS, neuronAddition.getStatus());
            }

            if (neuronAddition.getName() != null) {
                newNeuron.setProperty(NAME, neuronAddition.getName());
                isNeuron = true;
            }

            if (neuronAddition.getInstance() != null) {
                newNeuron.setProperty(INSTANCE, neuronAddition.getInstance());
                isNeuron = true;
            }

            if (neuronAddition.getPrimaryNeurite() != null) {
                newNeuron.setProperty(PRIMARY_NEURITE, neuronAddition.getPrimaryNeurite());
                isNeuron = true;
            }

            if (neuronAddition.getMajorInput() != null) {
                newNeuron.setProperty(MAJOR_INPUT, neuronAddition.getMajorInput());
                isNeuron = true;
            }

            if (neuronAddition.getMajorOutput() != null) {
                newNeuron.setProperty(MAJOR_OUTPUT, neuronAddition.getMajorOutput());
                isNeuron = true;
            }

            if (neuronAddition.getClonalUnit() != null) {
                newNeuron.setProperty(CLONAL_UNIT, neuronAddition.getClonalUnit());
                isNeuron = true;
            }

            if (neuronAddition.getNeurotransmitter() != null) {
                newNeuron.setProperty(NEUROTRANSMITTER, neuronAddition.getNeurotransmitter());
                isNeuron = true;
            }

            if (neuronAddition.getSoma() != null) {
                newNeuron.setProperty(SOMA_RADIUS, neuronAddition.getSoma().getRadius());
                org.janelia.flyem.neuprint.model.Location somaLocation = neuronAddition.getSoma().getLocation();
                Point somaLocationPoint = new Location(somaLocation.getX(), somaLocation.getY(), somaLocation.getZ());
                newNeuron.setProperty(SOMA_LOCATION, somaLocationPoint);
                isNeuron = true;
            }

            if (preCount >= 2 || postCount >= 10) {
                isNeuron = true;
            }

            if (isNeuron) {
                convertSegmentToNeuron(newNeuron, datasetLabel, metaNode);
            }

            // update meta node
            metaNode.setProperty("latestMutationId", neuronAddition.getMutationId());
            metaNode.setProperty("uuid", neuronAddition.getMutationUuid());

//            add skeleton?

            log.info("Completed neuron addition with uuid " + neuronAddition.getMutationUuid() + ", mutation id " + neuronAddition.getMutationId() + ", body id " + neuronAddition.getBodyId() + ".");

        } catch (Exception e) {
            log.error("Error running proofreader.addNeuron: " + e);
            throw new RuntimeException("Error running proofreader.addNeuron: " + e);
        }

        log.info("proofreader.addNeuron: exit");

    }

    @Procedure(value = "proofreader.deleteNeuron", mode = Mode.WRITE)
    @Description("proofreader.deleteNeuron(bodyId, datasetLabel) : Delete a Neuron/Segment from the database. Will orphan any synapses contained by the body.")
    public void deleteNeuron(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteNeuron: entry");

        try {

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteNeuron: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteNeuron: Missing input arguments.");
            }

            deleteSegment(bodyId, datasetLabel);

        } catch (Exception e) {
            log.error("Error running proofreader.deleteNeuron: " + e.getMessage());
            throw new RuntimeException("Error running proofreader.deleteNeuron: " + e.getMessage());
        }

        log.info("proofreader.deleteNeuron: exit");

    }

    @Procedure(value = "proofreader.addSkeleton", mode = Mode.WRITE)
    @Description("proofreader.addSkeleton(fileUrl,datasetLabel) : Load skeleton from provided URL and connect it to its associated Neuron/Segment. (Note: file URL must end with \"<bodyID>.swc\" or \"<bodyID>_swc\" where <bodyID> is the body ID of the Neuron/Segment) ")
    public void addSkeleton(@Name("fileUrl") String fileUrlString, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.addSkeleton: entry");

        try {

            if (fileUrlString == null || datasetLabel == null) {
                log.error("proofreader.addSkeleton: Missing input arguments.");
                throw new RuntimeException("proofreader.addSkeleton: Missing input arguments.");
            }

            String bodyIdPattern = ".*/(.*?)[._]swc";
            Matcher bodyIdMatcher = Pattern.compile(bodyIdPattern).matcher(fileUrlString);
            Long bodyId;
            boolean bodyIdMatches = bodyIdMatcher.matches();
            if (bodyIdMatches) {
                bodyId = Long.parseLong(bodyIdMatcher.group(1));
            } else {
                log.error("proofreader.addSkeleton: Cannot parse body id from swc file path.");
                throw new RuntimeException("proofreader.addSkeleton: Cannot parse body id from swc file path.");
            }

            String uuidPattern = ".*/api/node/(.*?)/segmentation.*";
            Matcher uuidMatcher = Pattern.compile(uuidPattern).matcher(fileUrlString);
            Optional<String> uuid = Optional.empty();
            boolean uuidMatches = uuidMatcher.matches();
            if (uuidMatches) {
                uuid = Optional.ofNullable(uuidMatcher.group(1));
            }

            Skeleton skeleton = new Skeleton();
            URL fileUrl;

            Stopwatch timer = Stopwatch.createStarted();
            try {
                fileUrl = new URL(fileUrlString);
            } catch (MalformedURLException e) {
                log.error(String.format("proofreader.addSkeleton: Malformed URL: %s", e.getMessage()));
                throw new RuntimeException(String.format("Malformed URL: %s", e.getMessage()));
            }
            log.info("Time to create url:" + timer.stop());
            timer.reset();

            timer.start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileUrl.openStream()))) {
                skeleton.fromSwc(reader, bodyId, uuid.orElse("none"));
            } catch (IOException e) {
                log.error(String.format("proofreader.addSkeleton: IOException: %s", e.getMessage()));
                throw new RuntimeException(String.format("IOException: %s", e.getMessage()));
            }
            log.info("Time to read in swc file: " + timer.stop());
            timer.reset();

            timer.start();
            Node segment = GraphTraversalTools.getSegment(dbService, bodyId, datasetLabel);
            log.info("Time to get segment:" + timer.stop());
            timer.reset();

            if (segment != null) {
                // grab write locks upfront
                acquireWriteLockForSegmentSubgraph(segment);

                timer.start();
                //check if skeleton already exists
                Node existingSkeleton = GraphTraversalTools.getSkeleton(dbService, bodyId, datasetLabel);
                if (existingSkeleton != null) {
                    log.warn(String.format("proofreader.addSkeleton: Skeleton for body ID %d already exists in dataset %s. Aborting addSkeleton.", bodyId, datasetLabel));
                } else {

                    addSkeletonNodes(datasetLabel, skeleton, segment);

                    log.info("Successfully added Skeleton to body Id " + bodyId + ".");
                }
                log.info("Time to add skeleton:" + timer.stop());
                timer.reset();

            } else {
                log.warn("Body Id " + bodyId + " does not exist in the dataset.");
            }

        } catch (Exception e) {
            log.error("Error running proofreader.addSkeleton: " + e);
            throw new RuntimeException("Error running proofreader.addSkeleton: " + e);
        }

        log.info("proofreader.addSkeleton: exit");

    }

    @Procedure(value = "proofreader.deleteSkeleton", mode = Mode.WRITE)
    @Description("proofreader.deleteSkeleton(bodyId,datasetLabel) : delete skeleton for Neuron/Segment with provided body id ")
    public void deleteSkeleton(@Name("bodyId") Long bodyId, @Name("datasetLabel") String datasetLabel) {

        log.info("proofreader.deleteSkeleton: entry");

        try {

            if (bodyId == null || datasetLabel == null) {
                log.error("proofreader.deleteSkeleton: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteSkeleton: Missing input arguments.");
            }

            Node neuron = GraphTraversalTools.getSegment(dbService, bodyId, datasetLabel);

            if (neuron != null) {

                acquireWriteLockForSegmentSubgraph(neuron);

                Node skeleton = GraphTraversalTools.getSkeletonNodeForNeuron(neuron);

                if (skeleton != null) {
                    log.info("proofreader.deleteSkeleton: skeleton found for body id " + bodyId + ".");
                    // delete neuron relationship to skeleton
                    skeleton.getSingleRelationship(RelationshipType.withName(CONTAINS), Direction.INCOMING).delete();
                    deleteSkeleton(skeleton);
                } else {
                    log.warn("proofreader.deleteSkeleton: no skeleton found for body id " + bodyId + ". Aborting deletion...");
                }

            } else {
                log.warn("proofreader.deleteSkeleton: body id " + bodyId + " not found. Aborting deletion...");
            }

        } catch (Exception e) {
            log.error("Error running proofreader.deleteSkeleton: " + e);
            throw new RuntimeException("Error running proofreader.deleteSkeleton: " + e);
        }

        log.info("proofreader.deleteSkeleton: exit");

    }

    @Procedure(value = "proofreader.addRoiToSynapse", mode = Mode.WRITE)
    @Description("proofreader.addRoiToSynapse(x,y,z,roiName,dataset) : add an ROI to a synapse. ")
    public void addRoiToSynapse(@Name("x") final Double x, @Name("y") final Double y, @Name("z") final Double z, @Name("roiName") final String roiName, @Name("dataset") final String dataset) {

        log.info("proofreader.addRoiToSynapse: entry");

        try {

            if (x == null || y == null || z == null || roiName == null || dataset == null) {
                log.error("proofreader.addRoiToSynapse: Missing input arguments.");
                throw new RuntimeException("proofreader.addRoiToSynapse: Missing input arguments.");
            }

            Node synapse = getSynapse(dbService, x, y, z, dataset);

            Node neuron = getSegmentThatContainsSynapse(synapse);
            if (neuron != null) {
                acquireWriteLockForSegmentSubgraph(neuron);
            }
            Node metaNode = getMetaNode(dbService, dataset);
            if (metaNode != null) {
                acquireWriteLockForNode(metaNode);
            }

            if (synapse == null) {
                log.error("proofreader.addRoiToSynapse: No synapse found at location: [" + x + "," + y + "," + z + "]");
                throw new RuntimeException("proofreader.addRoiToSynapse: No synapse found at location: [" + x + "," + y + "," + z + "]");
            }

            boolean roiAlreadyPresent = synapse.hasProperty(roiName);

            if (!roiAlreadyPresent) {

                // add roi to synapse
                synapse.setProperty(roiName, true);

                String synapseType;
                if (synapse.hasProperty(TYPE)) {
                    synapseType = (String) synapse.getProperty(TYPE);
                } else {
                    log.error(String.format("Synapse at location [%f,%f,%f] does not have type property.", x, y, z));
                    throw new RuntimeException(String.format("Synapse at location [%f,%f,%f] does not have type property.", x, y, z));
                }
                if (!synapseType.equals(PRE) && !synapseType.equals(POST)) {
                    log.error(String.format("Synapse at location [%f,%f,%f] does not have type property equal to 'pre' or 'post'.", x, y, z));
                    throw new RuntimeException(String.format("Synapse at location [%f,%f,%f] does not have type property equal to 'pre' or 'post'.", x, y, z));
                }

                Double synapseConfidence;
                try {
                    synapseConfidence = (Double) synapse.getProperty(CONFIDENCE);
                } catch (ClassCastException cce) {
                    log.warn("Found float confidence value. Converting to double...");
                    float floatConfidence = (Float) synapse.getProperty(CONFIDENCE);
                    synapseConfidence = (double) floatConfidence;
                    // fix the issue
                    synapse.setProperty(CONFIDENCE, synapseConfidence);
                } catch (Exception e) {
                    log.warn("proofreader.addRoiToSynapse: No confidence value found on synapse. Assumed to be 0.0: " + synapse.getAllProperties());
                    synapseConfidence = 0D;
                }

                // update connection set counts (no need to update roiInfos if synapse is not pre or post)
                // get the connection sets that it's part of
                List<Node> connectionSetList = getConnectionSetsForSynapse(synapse);
                Map<String, Double> thresholdMap = getPreAndPostHPThresholdFromMetaNode(metaNode);
                // change roiInfo for each connection set
                for (Node connectionSetNode : connectionSetList) {

                    String roiInfoString = (String) connectionSetNode.getProperty(ROI_INFO, "{}");
                    String roiInfoJsonString = addSynapseToRoiInfoWithHP(roiInfoString, roiName, synapseType, synapseConfidence, thresholdMap.get(PRE_HP_THRESHOLD), thresholdMap.get(POST_HP_THRESHOLD));
                    connectionSetNode.setProperty(ROI_INFO, roiInfoJsonString);

                }

                // update roi info and roi properties on neuron/segment
                if (neuron != null) {
                    // add boolean property
                    neuron.setProperty(roiName, true);

                    // update roi info
                    String roiInfoString = (String) neuron.getProperty(ROI_INFO, "{}");
                    String roiInfoJsonString = addSynapseToRoiInfo(roiInfoString, roiName, synapseType);
                    neuron.setProperty(ROI_INFO, roiInfoJsonString);

                } else {
                    log.warn("proofreader.addRoiToSynapse: Synapse not connected to neuron: " + synapse.getAllProperties());
                }

                // update meta node
                addSynapseToMetaRoiInfo(metaNode, roiName, synapseType);

            } else {
                log.warn("proofreader.addRoiToSynapse: roi already present on synapse. Ignoring update request: " + synapse.getAllProperties());
            }

        } catch (Exception e) {
            log.error("proofreader.addRoiToSynapse: " + e);
            throw new RuntimeException("proofreader.addRoiToSynapse: " + e);
        }

        log.info("proofreader.addRoiToSynapse: exit");

    }

    @Procedure(value = "proofreader.removeRoiFromSynapse", mode = Mode.WRITE)
    @Description("proofreader.removeRoiFromSynapse(x,y,z,roiName,dataset) : remove an ROI from a synapse. ")
    public void removeRoiFromSynapse(@Name("x") final Double x, @Name("y") final Double y, @Name("z") final Double z, @Name("roiName") final String roiName, @Name("dataset") final String dataset) {

        log.info("proofreader.removeRoiFromSynapse: entry");

        try {

            if (x == null || y == null || z == null || roiName == null || dataset == null) {
                log.error("proofreader.removeRoiFromSynapse: Missing input arguments.");
                throw new RuntimeException("proofreader.removeRoiFromSynapse: Missing input arguments.");
            }

            Node synapse = getSynapse(dbService, x, y, z, dataset);

            Node neuron = getSegmentThatContainsSynapse(synapse);
            if (neuron != null) {
                acquireWriteLockForSegmentSubgraph(neuron);
            }
            Node metaNode = getMetaNode(dbService, dataset);
            if (metaNode != null) {
                acquireWriteLockForNode(metaNode);
            }

            if (synapse == null) {
                log.error("proofreader.removeRoiFromSynapse: No synapse found at location: [" + x + "," + y + "," + z + "]");
                throw new RuntimeException("proofreader.removeRoiFromSynapse: No synapse found at location: [" + x + "," + y + "," + z + "]");
            }

            boolean roiPresent = synapse.hasProperty(roiName);

            if (roiPresent) {

                // remove roi from synapse
                synapse.removeProperty(roiName);

                String synapseType;
                if (synapse.hasProperty(TYPE)) {
                    synapseType = (String) synapse.getProperty(TYPE);
                } else {
                    log.error(String.format("Synapse at location [%f,%f,%f] does not have type property.", x, y, z));
                    throw new RuntimeException(String.format("Synapse at location [%f,%f,%f] does not have type property.", x, y, z));
                }
                if (!synapseType.equals(PRE) && !synapseType.equals(POST)) {
                    log.error(String.format("Synapse at location [%f,%f,%f] does not have type property equal to 'pre' or 'post'.", x, y, z));
                    throw new RuntimeException(String.format("Synapse at location [%f,%f,%f] does not have type property equal to 'pre' or 'post'.", x, y, z));
                }

                Double synapseConfidence;
                try {
                    synapseConfidence = (Double) synapse.getProperty(CONFIDENCE);
                } catch (ClassCastException cce) {
                    log.warn("Found float confidence value. Converting to double...");
                    float floatConfidence = (Float) synapse.getProperty(CONFIDENCE);
                    synapseConfidence = (double) floatConfidence;
                    // fix the issue
                    synapse.setProperty(CONFIDENCE, synapseConfidence);
                } catch (Exception e) {
                    log.warn("proofreader.addRoiToSynapse: No confidence value found on synapse. Assumed to be 0.0: " + synapse.getAllProperties());
                    synapseConfidence = 0D;
                }

                // update connection set counts (no need to update roiInfos if synapse is not pre or post)
                // get the connection sets that it's part of
                List<Node> connectionSetList = getConnectionSetsForSynapse(synapse);
                Map<String, Double> thresholdMap = getPreAndPostHPThresholdFromMetaNode(metaNode);
                // change roiInfo for each connection set
                for (Node connectionSetNode : connectionSetList) {

                    String roiInfoString = (String) connectionSetNode.getProperty(ROI_INFO, "{}");
                    String roiInfoJsonString = removeSynapseFromRoiInfoWithHP(roiInfoString, roiName, synapseType, synapseConfidence, thresholdMap.get(PRE_HP_THRESHOLD), thresholdMap.get(POST_HP_THRESHOLD));
                    connectionSetNode.setProperty(ROI_INFO, roiInfoJsonString);

                }

                // update roi info and roi properties on neuron/segment
                if (neuron != null) {

                    // update roi info
                    String roiInfoString = (String) neuron.getProperty(ROI_INFO, "{}");

                    String roiInfoJsonString = removeSynapseFromRoiInfo(roiInfoString, roiName, synapseType);
                    neuron.setProperty(ROI_INFO, roiInfoJsonString);

                    // remove boolean property if no longer present on neuron
                    if (!roiInfoContainsRoi(roiInfoJsonString, roiName)) {
                        neuron.removeProperty(roiName);
                    }

                } else {
                    log.warn("proofreader.removeRoiFromSynapse: Synapse not connected to neuron: " + synapse.getAllProperties());
                }

                // update meta node
                String metaRoiInfoString = (String) metaNode.getProperty(ROI_INFO, "{}");
                String roiInfoJsonString = removeSynapseFromRoiInfo(metaRoiInfoString, roiName, synapseType);
                metaNode.setProperty(ROI_INFO, roiInfoJsonString);

            } else {
                log.warn("proofreader.removeRoiFromSynapse: roi not present on synapse. Ignoring update request: " + synapse.getAllProperties());
            }

        } catch (Exception e) {
            log.error("proofreader.removeRoiFromSynapse: " + e);
            throw new RuntimeException("proofreader.removeRoiFromSynapse: " + e);
        }

        log.info("proofreader.removeRoiFromSynapse: exit");

    }

    @Procedure(value = "proofreader.addSynapse", mode = Mode.WRITE)
    @Description("proofreader.addSynapse(synapseJson, dataset) : Add a synapse node to the dataset specified by an input JSON (see https://github.com/connectome-neuprint/neuPrint/blob/master/graphupdateAPI.md). Will only add the Synapse node, not the connections to other Synapse nodes.")
    public void addSynapse(@Name("synapseJson") final String synapseJson, @Name("dataset") final String dataset) {

        log.info("proofreader.addSynapse: entry");

        try {

            if (synapseJson == null || dataset == null) {
                log.error("proofreader.addSynapse: Missing input arguments.");
                throw new RuntimeException("proofreader.addSynapse: Missing input arguments.");
            }

            // get the meta node for updating
            Node metaNode = getMetaNode(dbService, dataset);
            if (metaNode != null) {
                acquireWriteLockForNode(metaNode);
            }

            Synapse synapse = JsonUtils.GSON.fromJson(synapseJson, Synapse.class);

            // add basic synapse labels
            final Node newSynapseNode = dbService.createNode(
                    Label.label(SYNAPSE),
                    Label.label(dataset),
                    Label.label(dataset + "-" + SYNAPSE));

            // add location
            org.janelia.flyem.neuprint.model.Location synapseLocationList = synapse.getLocation();
            Point synapseLocationPoint = new Location(synapseLocationList.getX(), synapseLocationList.getY(), synapseLocationList.getZ());

            try {
                newSynapseNode.setProperty(LOCATION, synapseLocationPoint);
            } catch (org.neo4j.graphdb.ConstraintViolationException cve) {
                log.error("Synapse with location " + synapseLocationList + " already exists in database. Aborting synapse addition.");
                throw new RuntimeException("Synapse with location " + synapseLocationList + " already exists in database. Aborting synapse addition.");
            }

            // synapse must have type
            if (synapse.getType() == null) {
                log.error("Synapse with location " + synapseLocationList + " does not have a type specified. Aborting synapse addition.");
                throw new RuntimeException("Synapse with location " + synapseLocationList + " does not have a type specified. Aborting synapse addition.");
            }

            // add PreSyn or PostSyn label and type property and increment counts on meta node
            if (synapse.getType().equals(POST)) {
                newSynapseNode.addLabel(Label.label(POST_SYN));
                newSynapseNode.addLabel(Label.label(dataset + "-" + POST_SYN));
                newSynapseNode.setProperty(TYPE, synapse.getType());
                incrementMetaNodeTotalPostCount(metaNode);
            } else if (synapse.getType().equals(PRE)) {
                newSynapseNode.addLabel(Label.label(PRE_SYN));
                newSynapseNode.addLabel(Label.label(dataset + "-" + PRE_SYN));
                newSynapseNode.setProperty(TYPE, synapse.getType());
                incrementMetaNodeTotalPreCount(metaNode);
            } else {
                log.error("Synapse type must be either 'pre' or 'post'. Was " + synapse.getType() + ". Aborting synapse addition.");
                throw new RuntimeException("Synapse type must be either 'pre' or 'post'. Was " + synapse.getType() + ". Aborting synapse addition.");
            }

            // add confidence (default value will be 0.0)
            newSynapseNode.setProperty(CONFIDENCE, synapse.getConfidence());

            // add rois and update roiInfo on Meta node
            for (String roi : synapse.getRois()) {
                newSynapseNode.setProperty(roi, true);
                addSynapseToMetaRoiInfo(metaNode, roi, synapse.getType());
            }

        } catch (Exception e) {
            log.error("Error running proofreader.addSynapse: " + e);
            throw new RuntimeException("Error running proofreader.addSynapse: " + e);
        }

        log.info("proofreader.addSynapse: exit");

    }

    @Procedure(value = "proofreader.addConnectionBetweenSynapseNodes", mode = Mode.WRITE)
    @Description("proofreader.addConnectionBetweenSynapseNodes(preX,preY,preZ,postX,postY,postZ,dataset) : Add a SynapsesTo relationship between two Synapse nodes. Both nodes must exist in the dataset, and neither can be currently owned by a Neuron/Segment.")
    public void addConnectionBetweenSynapseNodes(@Name("preX") final Double preX, @Name("preY") final Double preY, @Name("preZ") final Double preZ, @Name("postX") final Double postX, @Name("postY") final Double postY, @Name("postZ") final Double postZ, @Name("dataset") final String dataset) {

        log.info("proofreader.addConnectionBetweenSynapseNodes: entry");

        try {
            if (preX == null || preY == null || preZ == null || postX == null || postY == null || postZ == null || dataset == null) {
                log.error("proofreader.addConnectionBetweenSynapseNodes: Missing input arguments.");
                throw new RuntimeException("proofreader.addConnectionBetweenSynapseNodes: Missing input arguments.");
            }

            // acquire both synapse nodes
            Node preSynapse = getSynapse(dbService, preX, preY, preZ, dataset);
            Node postSynapse = getSynapse(dbService, postX, postY, postZ, dataset);

            // error if synapses are not found
            if (preSynapse == null) {
                log.error(String.format("proofreader.addConnectionBetweenSynapseNodes: No synapse with location [%f,%f,%f] in dataset %s.", preX, preY, preZ, dataset));
                throw new RuntimeException(String.format("proofreader.addConnectionBetweenSynapseNodes: No synapse with location [%f,%f,%f] in dataset %s.", preX, preY, preZ, dataset));
            }
            if (postSynapse == null) {
                log.error(String.format("proofreader.addConnectionBetweenSynapseNodes: No synapse with location [%f,%f,%f] in dataset %s.", postX, postY, postZ, dataset));
                throw new RuntimeException(String.format("proofreader.addConnectionBetweenSynapseNodes: No synapse with location [%f,%f,%f] in dataset %s.", postX, postY, postZ, dataset));
            }

            acquireWriteLockForNode(preSynapse);
            acquireWriteLockForNode(postSynapse);

            // error if 1st location not pre or 2nd location not post
            if (!preSynapse.hasLabel(Label.label(PRE_SYN))) {
                log.error(String.format("proofreader.addConnectionBetweenSynapseNodes: Synapse with location [%f,%f,%f] is not a presynaptic density.", preX, preY, preZ));
                throw new RuntimeException(String.format("proofreader.addConnectionBetweenSynapseNodes: Synapse with location [%f,%f,%f] is not a presynaptic density.", preX, preY, preZ));
            }
            if (!postSynapse.hasLabel(Label.label(POST_SYN))) {
                log.error(String.format("proofreader.addConnectionBetweenSynapseNodes: Synapse with location [%f,%f,%f] is not a postsynaptic density.", postX, postY, postZ));
                throw new RuntimeException(String.format("proofreader.addConnectionBetweenSynapseNodes: Synapse with location [%f,%f,%f] is not a postsynaptic density.", postX, postY, postZ));
            }

            // error if a synapse already belongs to a body
            // TODO: may in the future add ability to connect to synapses that are already on a body
            if (getSegmentThatContainsSynapse(preSynapse) != null) {
                log.error(String.format("proofreader.addConnectionBetweenSynapseNodes: Synapse with location [%f,%f,%f] is currently owned by a body.", preX, preY, preZ));
                throw new RuntimeException(String.format("proofreader.addConnectionBetweenSynapseNodes: Synapse with location [%f,%f,%f] is currently owned by a body.", preX, preY, preZ));
            }
            if (getSegmentThatContainsSynapse(postSynapse) != null) {
                log.error(String.format("proofreader.addConnectionBetweenSynapseNodes: Synapse with location [%f,%f,%f] is currently owned by a body.", postX, postY, postZ));
                throw new RuntimeException(String.format("proofreader.addConnectionBetweenSynapseNodes: Synapse with location [%f,%f,%f] is currently owned by a body.", postX, postY, postZ));
            }

            // create connection between synapses
            preSynapse.createRelationshipTo(postSynapse, RelationshipType.withName(SYNAPSES_TO));

        } catch (Exception e) {
            log.error("Error running proofreader.addConnectionBetweenSynapseNodes: " + e);
            throw new RuntimeException("Error running proofreader.addConnectionBetweenSynapseNodes: " + e);
        }

        log.info("proofreader.addConnectionBetweenSynapseNodes: exit");

    }

    @Procedure(value = "proofreader.addSynapseToSegment", mode = Mode.WRITE)
    @Description("proofreader.addSynapseToSegment(x, y, z, bodyId, dataset) : Add an orphaned Synapse node to a Neuron/Segment. Synapse and Neuron/Segment must exist in the dataset.")
    public void addSynapseToSegment(@Name("x") final Double x, @Name("y") final Double y, @Name("z") final Double z, @Name("bodyId") Long bodyId, @Name("dataset") final String dataset) {

        log.info("proofreader.addSynapseToSegment: entry");

        try {
            if (x == null || y == null || z == null || bodyId == null || dataset == null) {
                log.error("proofreader.addSynapseToSegment: Missing input arguments.");
                throw new RuntimeException("proofreader.addSynapseToSegment: Missing input arguments.");
            }
            // acquire the synapse node
            Node synapse = getSynapse(dbService, x, y, z, dataset);

            // error if synapse not found
            if (synapse == null) {
                log.error(String.format("proofreader.addSynapseToSegment: No synapse with location [%f,%f,%f] in dataset %s.", x, y, z, dataset));
                throw new RuntimeException(String.format("proofreader.addSynapseToSegment: No synapse with location [%f,%f,%f] in dataset %s.", x, y, z, dataset));
            }
            acquireWriteLockForNode(synapse);

            String synapseType;
            if (synapse.hasProperty(TYPE)) {
                synapseType = (String) synapse.getProperty(TYPE);
            } else {
                log.error(String.format("Synapse at location [%f,%f,%f] does not have type property.", x, y, z));
                throw new RuntimeException(String.format("Synapse at location [%f,%f,%f] does not have type property.", x, y, z));
            }
            if (!synapseType.equals(PRE) && !synapseType.equals(POST)) {
                log.error(String.format("Synapse at location [%f,%f,%f] does not have type property equal to 'pre' or 'post'.", x, y, z));
                throw new RuntimeException(String.format("Synapse at location [%f,%f,%f] does not have type property equal to 'pre' or 'post'.", x, y, z));
            }

            Node segment = getSegment(dbService, bodyId, dataset);
            //error if segment not found
            if (segment == null) {
                log.error(String.format("proofreader.addSynapseToSegment: No neuron/segment with body ID %d in dataset %s.", bodyId, dataset));
                throw new RuntimeException(String.format("proofreader.addSynapseToSegment: No neuron/segment with body ID %d in dataset %s.", bodyId, dataset));
            }
            acquireWriteLockForSegmentSubgraph(segment);

            // acquire meta node for updating
            Node metaNode = getMetaNode(dbService, dataset);
            if (metaNode == null) {
                log.error("Meta node not found for dataset: " + dataset);
                throw new RuntimeException("Meta node not found for dataset: " + dataset);
            }
            acquireWriteLockForNode(metaNode);
            Set<String> metaNodeRoiSet = getMetaNodeRoiSet(metaNode);
            Map<String, Double> thresholdMap = getPreAndPostHPThresholdFromMetaNode(metaNode);

            // add synapse to synapse set
            Node synapseSet = getSynapseSetForNeuron(segment);
            if (synapseSet == null) {
                // create synapse set if it doesn't exist
                synapseSet = createSynapseSetForSegment(segment, dataset);
            }
            addSynapseToSynapseSet(synapseSet, synapse);

            // for each synapse that the synapse SynapsesTo, create or add to a ConnectionSet and ConnectsTo
            for (Relationship synapsesToRel : synapse.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                Node otherSynapse = synapsesToRel.getOtherNode(synapse);
                Node otherSegment = getSegmentThatContainsSynapse(otherSynapse);
                Node connectionSet;
                if (otherSegment == null) {
                    log.warn("Synapse does not belong to segment: " + otherSynapse.getAllProperties());
                } else {
                    Long otherBodyId;
                    if (otherSegment.hasProperty(BODY_ID)) {
                        otherBodyId = (Long) otherSegment.getProperty(BODY_ID);
                    } else {
                        log.error("Segment node is missing a bodyId. Neo4j ID is: " + otherSegment.getId());
                        throw new RuntimeException("Segment node is missing a bodyId. Neo4j ID is: " + otherSegment.getId());
                    }
                    if (synapseType.equals(PRE)) {
                        // look for connection set from original segment to other segment (create ConnectsTo and ConnectionSet if doesn't exist)
                        connectionSet = getConnectionSetOrCreateConnectionSetAndConnectsToRelFromSynapses(bodyId, otherBodyId, segment, otherSegment, synapse, otherSynapse, dataset);
                    } else {
                        // look for connection set from other segment to original segment (create ConnectsTo and ConnectionSet if doesn't exist)
                        connectionSet = getConnectionSetOrCreateConnectionSetAndConnectsToRelFromSynapses(otherBodyId, bodyId, otherSegment, segment, otherSynapse, synapse, dataset);
                    }

                    Set<Node> synapseForConnectionSet = getSynapsesForConnectionSet(connectionSet);
                    // recompute roiInfo on connection sets and set weight and weightHP
                    setConnectionSetRoiInfoWeightAndWeightHP(synapseForConnectionSet, connectionSet, thresholdMap.get(PRE_HP_THRESHOLD), thresholdMap.get(POST_HP_THRESHOLD), metaNodeRoiSet);
                }
            }

            // update neuron pre/post, roiInfo, rois
            // recompute information on containing segment
            Set<String> synapseRois = getSynapseRois(synapse, metaNodeRoiSet);
            recomputeSegmentPropertiesFollowingSynapseAddition(synapseRois, synapseType, segment, dataset, metaNode);

        } catch (Exception e) {
            log.error("Error running proofreader.addSynapseToSegment: " + e);
            throw new RuntimeException("Error running proofreader.addSynapseToSegment: " + e);
        }

        log.info("proofreader.addSynapseToSegment: exit");

    }

    @Procedure(value = "proofreader.deleteSynapse", mode = Mode.WRITE)
    @Description("proofreader.deleteSynapse(x, y, z, dataset) : Remove a synapse node specified by the 3D location provided.")
    public void deleteSynapse(@Name("x") final Double x, @Name("y") final Double y, @Name("z") final Double z, @Name("dataset") final String dataset) {

        log.info("proofreader.deleteSynapse: entry");

        try {

            if (x == null || y == null || z == null || dataset == null) {
                log.error("proofreader.deleteSynapse: Missing input arguments.");
                throw new RuntimeException("proofreader.deleteSynapse: Missing input arguments.");
            }

            // acquire the synapse node
            Node synapse = getSynapse(dbService, x, y, z, dataset);

            // acquire meta node for updating
            Node metaNode = getMetaNode(dbService, dataset);
            if (metaNode == null) {
                log.error("Meta node not found for dataset: " + dataset);
                throw new RuntimeException("Meta node not found for dataset: " + dataset);
            }
            acquireWriteLockForNode(metaNode);
            Set<String> metaNodeRoiSet = getMetaNodeRoiSet(metaNode);
            Map<String, Double> thresholdMap = getPreAndPostHPThresholdFromMetaNode(metaNode);

            // warn if it doesn't exist
            if (synapse == null) {
                log.warn(String.format("proofreader.deleteSynapse: No synapse found at location [%f,%f,%f]. Aborting deletion. ", x, y, z));
            } else {

                acquireWriteLockForNode(synapse);

                String synapseType;
                if (synapse.hasProperty(TYPE)) {
                    synapseType = (String) synapse.getProperty(TYPE);
                } else {
                    log.error(String.format("Synapse at location [%f,%f,%f] does not have type property.", x, y, z));
                    throw new RuntimeException(String.format("Synapse at location [%f,%f,%f] does not have type property.", x, y, z));
                }
                if (!synapseType.equals(PRE) && !synapseType.equals(POST)) {
                    log.error(String.format("Synapse at location [%f,%f,%f] does not have type property equal to 'pre' or 'post'.", x, y, z));
                    throw new RuntimeException(String.format("Synapse at location [%f,%f,%f] does not have type property equal to 'pre' or 'post'.", x, y, z));
                }

                // if orphan continue, otherwise update synapse set, connection set info and relationships
                Node containingSegment = getSegmentThatContainsSynapse(synapse);

                if (containingSegment != null) {
                    orphanSynapse(synapse, dataset, thresholdMap, metaNodeRoiSet);
                }

                // delete synapsesTo relationships (may be multiple)
                for (Relationship synapsesToRel : synapse.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                    synapsesToRel.delete();
                }

                // remove from meta node counts and roiInfo
                Set<String> synapseRois = getSynapseRois(synapse, metaNodeRoiSet);
                if (synapseType.equals(PRE)) {
                    decrementMetaNodeTotalPreCount(metaNode);
                } else {
                    decrementMetaNodeTotalPostCount(metaNode);
                }

                for (String roi : synapseRois) {
                    removeSynapseFromMetaRoiInfo(metaNode, roi, synapseType);
                }

                // delete synapse node
                synapse.delete();

                // recompute information on containing segment
                if (containingSegment != null) {
                    recomputeSegmentPropertiesFollowingSynapseRemoval(synapseRois, synapseType, containingSegment, dataset, metaNodeRoiSet);
                }

            }

        } catch (Exception e) {
            log.error("proofreader.deleteSynapse: " + e);
            throw new RuntimeException("proofreader.deleteSynapse: " + e);
        }

        log.info("proofreader.deleteSynapse: exit");

    }

    @Procedure(value = "proofreader.orphanSynapse", mode = Mode.WRITE)
    @Description("proofreader.orphanSynapse(x, y, z, dataset) : Orphan (but do not delete) a synapse node specified by the 3D location provided.")
    public void orphanSynapse(@Name("x") final Double x, @Name("y") final Double y, @Name("z") final Double z, @Name("dataset") final String dataset) {

        log.info("proofreader.orphanSynapse: entry");

        try {

            if (x == null || y == null || z == null || dataset == null) {
                log.error("proofreader.orphanSynapse: Missing input arguments.");
                throw new RuntimeException("proofreader.orphanSynapse: Missing input arguments.");
            }

            // acquire the synapse node
            Node synapse = getSynapse(dbService, x, y, z, dataset);

            // acquire meta node for updating
            Node metaNode = getMetaNode(dbService, dataset);
            if (metaNode == null) {
                log.error("Meta node not found for dataset: " + dataset);
                throw new RuntimeException("Meta node not found for dataset: " + dataset);
            }
            acquireWriteLockForNode(metaNode);
            Set<String> metaNodeRoiSet = getMetaNodeRoiSet(metaNode);
            Map<String, Double> thresholdMap = getPreAndPostHPThresholdFromMetaNode(metaNode);

            // warn if it doesn't exist
            if (synapse == null) {
                log.warn(String.format("proofreader.orphanSynapse: No synapse found at location [%f,%f,%f]. Aborting orphan procedure. ", x, y, z));
            } else {

                acquireWriteLockForNode(synapse);

                String synapseType;
                if (synapse.hasProperty(TYPE)) {
                    synapseType = (String) synapse.getProperty(TYPE);
                } else {
                    log.error(String.format("Synapse at location [%f,%f,%f] does not have type property.", x, y, z));
                    throw new RuntimeException(String.format("Synapse at location [%f,%f,%f] does not have type property.", x, y, z));
                }
                if (!synapseType.equals(PRE) && !synapseType.equals(POST)) {
                    log.error(String.format("Synapse at location [%f,%f,%f] does not have type property equal to 'pre' or 'post'.", x, y, z));
                    throw new RuntimeException(String.format("Synapse at location [%f,%f,%f] does not have type property equal to 'pre' or 'post'.", x, y, z));
                }

                Node containingSegment = getSegmentThatContainsSynapse(synapse);

                // if orphan already, do nothing
                if (containingSegment == null) {
                    log.warn(String.format("proofreader.orphanSynapse: Synapse at location [%f,%f,%f] is already orphaned. Aborting orphan procedure. ", x, y, z));
                } else {

                    orphanSynapse(synapse, dataset, thresholdMap, metaNodeRoiSet);

                    Set<String> synapseRois = getSynapseRois(synapse, metaNodeRoiSet);

                    // recompute information on containing segment
                    recomputeSegmentPropertiesFollowingSynapseRemoval(synapseRois, synapseType, containingSegment, dataset, metaNodeRoiSet);

                }

            }

        } catch (Exception e) {
            log.error("proofreader.orphanSynapse: " + e);
            throw new RuntimeException("proofreader.orphanSynapse: " + e);
        }

        log.info("proofreader.orphanSynapse: exit");

    }

    @Procedure(value = "proofreader.addGenericCollectionToSegment", mode = Mode.WRITE)
    @Description("proofreader.addGenericCollectionToSegment(bodyId, dataset, collectionLabel, collectionItemJson) ")
    public void addGenericCollectionToSegment(@Name("bodyId") Long bodyId, @Name("dataset") String dataset, @Name("collectionLabel") String collectionLabel, @Name("collectionItemJson") String collectionItemJson) {

        log.info("proofreader.addGenericCollectionToSegment: entry");

        throw new RuntimeException("proofreader.addGenericCollectionToSegment: Not yet implemented.");

//        try {
//
//            if (bodyId == null || dataset == null || collectionLabel == null || collectionItemJson == null) {
//                log.error("proofreader.addGenericCollectionToSegment: Missing input arguments.");
//                throw new RuntimeException("proofreader.addGenericCollectionToSegment: Missing input arguments.");
//            }
//
//            // acquire segment
//            Node segment = getSegment(dbService, bodyId, dataset);
//            if (segment == null) {
//                log.error(String.format("proofreader.addGenericCollectionToSegment: No segment with body ID %d found in dataset %s.", bodyId, dataset));
//                throw new RuntimeException(String.format("proofreader.addGenericCollectionToSegment: No segment with body ID %d found in dataset %s.", bodyId, dataset));
//            }
//            acquireWriteLockForNode(segment);
//
//            // make sure that node label is valid, will be adding :dataset, :label, and :dataset-label to node
//            if (collectionLabel.length() == 0 || collectionLabel.equals(dataset) || isProtectedLabel(collectionLabel)) {
//                log.error();
//                throw new RuntimeException();
//            }

            // use "Contains" relationship (make sure updaters clean up all contains relationships)

            // (segment)-[:Contains]->(label)-[:Contains]->(generic items)
            // Item class/ node type. Item can have location, type, confidence, roi?

//        } catch (Exception e) {
//            log.error("proofreader.addGenericCollectionToSegment: " + e);
//            throw new RuntimeException("proofreader.addGenericCollectionToSegment: " + e);
//        }

//        log.info("proofreader.addGenericCollectionToSegment: exit");

    }

    // Left as example showing how to update the data model incrementally.
    // I would use a query like
    // "CALL apoc.periodic.iterate(“MATCH (n:`hemibrain-ConnectionSet`) RETURN n”, “CALL temp.updateConnectionSetsAndWeightHP(n,'hemibrain') RETURN n.datasetBodyIds”, {batchSize:100, parallel:true}) yield batches, total return batches, total"
    // to complete this in a reasonable amount of time. For this procedure you may have to temporarily turn off the transaction timeout in the database.
    @Procedure(value = "temp.updateConnectionSetsAndWeightHP", mode = Mode.WRITE)
    @Description("temp.updateConnectionSetsAndWeightHP(connectionSetNode, datasetLabel) ")
    public void updateConnectionSetsAndWeightHP(@Name("connectionSetNode") Node connectionSetNode, @Name("datasetLabel") String datasetLabel) {

        log.info("temp.updateConnectionSetsAndWeightHP: entry");

        try {

            Node metaNode = getMetaNode(dbService, datasetLabel);
            if (metaNode == null) {
                log.error("Meta node not found for dataset: " + datasetLabel);
                throw new RuntimeException("Meta node not found for dataset: " + datasetLabel);
            }
            acquireWriteLockForNode(metaNode);
            Set<String> metaNodeRoiSet = getMetaNodeRoiSet(metaNode);
            Map<String, Double> thresholdMap = getPreAndPostHPThresholdFromMetaNode(metaNode);

            Set<Node> synapsesForConnectionSet = org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getSynapsesForConnectionSet(connectionSetNode);

            setConnectionSetRoiInfoWeightAndWeightHP(synapsesForConnectionSet, connectionSetNode, thresholdMap.get(PRE_HP_THRESHOLD), thresholdMap.get(POST_HP_THRESHOLD), metaNodeRoiSet);

        } catch (Exception e) {
            log.error("temp.updateConnectionSetsAndWeightHP: " + e);
            throw new RuntimeException("temp.updateConnectionSetsAndWeightHP: " + e);
        }

        log.info("temp.updateConnectionSetsAndWeightHP: exit");
    }

    @Procedure(value = "temp.removeDuplicateContainsRelForConnectionSet", mode = Mode.WRITE)
    @Description("temp.removeDuplicateContainsRelForConnectionSet(connectionSetNode) ")
    public void removeDuplicateContainsRelForConnectionSet(@Name("connectionSetNode") Node connectionSetNode) {

        log.info("temp.removeDuplicateContainsRelForConnectionSet: entry");

        try {

            Set<Long> synapseIdSet = new HashSet<>();

            for (Relationship containsRel : connectionSetNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                long synapseId = containsRel.getEndNodeId();
                if (!synapseIdSet.contains(synapseId)) {
                    synapseIdSet.add(synapseId);
                } else {
                    // delete this relationship because it's a duplicate
                    containsRel.delete();
                }
            }

        } catch (Exception e) {
            log.error("temp.removeDuplicateContainsRelForConnectionSet: " + e);
            throw new RuntimeException("temp.removeDuplicateContainsRelForConnectionSet: " + e);
        }

        log.info("temp.removeDuplicateContainsRelForConnectionSet: exit");
    }

    private void recomputeSegmentPropertiesFollowingSynapseRemoval(Set<String> synapseRois, String synapseType, Node containingSegment, String dataset, Set<String> metaNodeRoiSet) {
        // set pre and post count
        if (synapseType.equals(PRE)) {
            decrementSegmentPreCount(containingSegment);
        } else {
            decrementSegmentPostCount(containingSegment);
        }

        // set roiInfo
        String roiInfoString = (String) containingSegment.getProperty(ROI_INFO, "{}");

        for (String roi : synapseRois) {
            roiInfoString = removeSynapseFromRoiInfo(roiInfoString, roi, synapseType);
        }

        containingSegment.setProperty(ROI_INFO, roiInfoString);

        // set rois by comparing keys in roiInfo to rois on segment
        Map<String, org.janelia.flyem.neuprintloadprocedures.model.SynapseCounter> roiInfoMap = getRoiInfoAsMap(roiInfoString);
        Set<String> currentSegmentRois = getSegmentRois(containingSegment, metaNodeRoiSet);
        currentSegmentRois.removeAll(roiInfoMap.keySet());
        for (String roiToRemove : currentSegmentRois) {
            containingSegment.removeProperty(roiToRemove);
        }

        // check if should still be a neuron
        if (shouldNotBeLabeledNeuron(containingSegment)) {
            removeNeuronDesignationFromNode(containingSegment, dataset);
        }

    }

    private void recomputeSegmentPropertiesFollowingSynapseAddition(Set<String> synapseRois, String synapseType, Node containingSegment, String dataset, Node metaNode) {
        // set pre and post count
        if (synapseType.equals(PRE)) {
            incrementSegmentPreCount(containingSegment);
        } else {
            incrementSegmentPostCount(containingSegment);
        }

        // make sure segment has both pre and post if it has one
        if (containingSegment.hasProperty(PRE) && !containingSegment.hasProperty(POST)) {
            containingSegment.setProperty(POST, 0L);
        } else if (containingSegment.hasProperty(POST) && !containingSegment.hasProperty(PRE)) {
            containingSegment.setProperty(PRE, 0L);
        }

        // set roiInfo and rois
        String roiInfoString = (String) containingSegment.getProperty(ROI_INFO, "{}");
        for (String roi : synapseRois) {
            roiInfoString = addSynapseToRoiInfo(roiInfoString, roi, synapseType);
            // add synapse rois if not present
            if (!containingSegment.hasProperty(roi)) {
                containingSegment.setProperty(roi, true);
            }
        }

        containingSegment.setProperty(ROI_INFO, roiInfoString);

        // check if should be a neuron
        if (!shouldNotBeLabeledNeuron(containingSegment)) {
            convertSegmentToNeuron(containingSegment, dataset, metaNode);
        }

    }

    private void orphanSynapse(Node synapse, String dataset, Map<String, Double> thresholdMap, Set<String> metaNodeRoiSet) {
        // get list of affected connection sets
        Set<Node> affectedConnectionSets = getConnectionSetsAffectedBySynapse(synapse, dataset);

        // delete relationships to synapse set and connection set
        for (Relationship containsRel : synapse.getRelationships(RelationshipType.withName(CONTAINS), Direction.INCOMING)) {
            containsRel.delete();
        }

        // recompute connection set and ConnectsTo information
        for (Node connectionSet : affectedConnectionSets) {
            computeAndSetConnectionInformation(connectionSet, thresholdMap, metaNodeRoiSet);
        }
    }

    private Node getConnectionSetOrCreateConnectionSetAndConnectsToRelFromSynapses(Long preBodyId, Long postBodyId, Node preBody, Node postBody, Node preSynapse, Node postSynapse, String dataset) {
        // look for connection set from original segment to other segment
        Node connectionSet = getConnectionSetNode(dbService, preBodyId, postBodyId, dataset);
        if (connectionSet == null) {
            // create connection set if it doesn't exist
            connectionSet = createConnectionSetNode(dataset, preBody, postBody);
            // create connects to between neurons
            addConnectsToRelationship(preBody, postBody, 1); // 1 since there is one known post for this connection
        }

        // add synapses to connection set
        boolean hasPreRel = false;
        boolean hasPostRel = false;
        long preId = preSynapse.getId();
        long postId = postSynapse.getId();

        for (Relationship containsRel : connectionSet.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
            Node containedSynapse = containsRel.getEndNode();
            if (containedSynapse.getId() == preId) {
                hasPreRel = true;
            } else if (containedSynapse.getId() == postId) {
                hasPostRel = true;
            }
        }

        if (!hasPreRel) {
            connectionSet.createRelationshipTo(preSynapse, RelationshipType.withName(CONTAINS));
        }
        if (!hasPostRel) {
            connectionSet.createRelationshipTo(postSynapse, RelationshipType.withName(CONTAINS));
        }

        return connectionSet;
    }

    private void computeAndSetConnectionInformation(Node connectionSetNode, Map<String, Double> thresholdMap, Set<String> metaNodeRoiSet) {

        Set<Node> correctedSynapsesForConnectionSet = removeUnconnectedSynapsesFromConnectionSet(connectionSetNode);

        int[] results = setConnectionSetRoiInfoWeightAndWeightHP(correctedSynapsesForConnectionSet, connectionSetNode, thresholdMap.get(PRE_HP_THRESHOLD), thresholdMap.get(POST_HP_THRESHOLD), metaNodeRoiSet);
        int weight = results[0];

        // delete connection set if weight is 0
        if (weight == 0) {
            if (connectionSetNode.hasRelationship(RelationshipType.withName(CONTAINS))) {
                log.error("Attempting to delete a ConnectionSet that still contains a synapse(s).");
                throw new RuntimeException("Attempting to delete a ConnectionSet that still contains a synapse(s).");
            }
            removeAllRelationships(connectionSetNode);
            connectionSetNode.delete();
        }

    }

    private Set<Node> removeUnconnectedSynapsesFromConnectionSet(Node connectionSetNode) {
        Set<Node> synapsesForConnectionSet = org.janelia.flyem.neuprintloadprocedures.GraphTraversalTools.getSynapsesForConnectionSet(connectionSetNode);
        Set<Node> correctedSynapsesForConnectionSet = new HashSet<>(synapsesForConnectionSet);

        // remove synapses from connection set that no longer have a SynapsesTo relationship within the ConnectionSet
        long connectionSetNodeId = connectionSetNode.getId();
        for (Node synapse : synapsesForConnectionSet) {
            boolean shouldBeInConnectionSet = false;
            for (Relationship synapsesToRel : synapse.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                Node connectedSynapse = synapsesToRel.getOtherNode(synapse);
                for (Relationship containsRel : connectedSynapse.getRelationships(RelationshipType.withName(CONTAINS), Direction.INCOMING)) {
                    if (containsRel.getStartNodeId() == connectionSetNodeId) {
                        shouldBeInConnectionSet = true;
                        break;
                    }
                }
                if (shouldBeInConnectionSet) {
                    break;
                }
            }
            if (!shouldBeInConnectionSet) {
                // delete contains relationship between connection set and synapse
                for (Relationship containsRel : synapse.getRelationships(RelationshipType.withName(CONTAINS), Direction.INCOMING)) {
                    if (containsRel.getStartNodeId() == connectionSetNodeId) {
                        containsRel.delete();
                        correctedSynapsesForConnectionSet.remove(synapse);
                    }
                }
            }
        }

        return correctedSynapsesForConnectionSet;
    }

    private Set<Node> getConnectionSetsAffectedBySynapse(final Node synapse, final String dataset) {
        // get all connection sets that this synapse is involved in
        List<Node> connectionSets = getConnectionSetsForSynapse(synapse);

        for (Node connectionSet : connectionSets) {
            long preBodyId;
            long postBodyId;
            try {
                String datasetBodyIds = (String) connectionSet.getProperty(DATASET_BODY_IDs);
                String[] splitDatasetBodyIds = datasetBodyIds.split(":");
                preBodyId = Long.parseLong(splitDatasetBodyIds[1]);
                postBodyId = Long.parseLong(splitDatasetBodyIds[2]);
            } catch (Exception e) {
                log.error("Error retrieving pre and post synaptic body IDs from connection set " + connectionSet.getAllProperties() + ": " + e);
                throw new RuntimeException("Error retrieving pre and post synaptic body IDs from connection set " + connectionSet.getAllProperties() + ": " + e);
            }

            Node preBody = getSegment(dbService, preBodyId, dataset);
            if (preBody == null) {
                log.error("Error retrieving pre body with ID " + preBodyId);
                throw new RuntimeException("Error retrieving pre body with ID " + preBodyId);
            }
            Node postBody = getSegment(dbService, postBodyId, dataset);
            if (postBody == null) {
                log.error("Error retrieving post body with ID " + postBodyId);
                throw new RuntimeException("Error retrieving post body with ID " + postBodyId);
            }

        }

        return new HashSet<>(connectionSets);

    }

    private void incrementMetaNodeTotalPreCount(Node metaNode) {
        if (metaNode != null) {
            if (metaNode.hasProperty(TOTAL_PRE_COUNT)) {
                Long currentTotalPreCount = (Long) metaNode.getProperty(TOTAL_PRE_COUNT);
                metaNode.setProperty(TOTAL_PRE_COUNT, ++currentTotalPreCount);
            } else {
                log.warn("No totalPreCount property found on Meta node. This property will not be updated.");
            }
        } else {
            log.warn("No Meta node found. totalPreCount will not be updated.");
        }
    }

    private void decrementMetaNodeTotalPreCount(Node metaNode) {
        if (metaNode != null) {
            if (metaNode.hasProperty(TOTAL_PRE_COUNT)) {
                Long currentTotalPreCount = (Long) metaNode.getProperty(TOTAL_PRE_COUNT);
                metaNode.setProperty(TOTAL_PRE_COUNT, --currentTotalPreCount);
            } else {
                log.warn("No totalPreCount property found on Meta node. This property will not be updated.");
            }
        } else {
            log.warn("No Meta node found. totalPreCount will not be updated.");
        }
    }

    private void decrementConnectsToWeight(Relationship connectsToRel) {
        Long currentWeight;
        if (connectsToRel.hasProperty(WEIGHT)) {
            currentWeight = (Long) connectsToRel.getProperty(WEIGHT);
        } else {
            log.error(String.format("No weight found on ConnectsTo relationship between nodes with neo4j ids %d and %d.", connectsToRel.getStartNodeId(), connectsToRel.getEndNodeId()));
            throw new RuntimeException(String.format("No weight found on ConnectsTo relationship between nodes with neo4j ids %d and %d.", connectsToRel.getStartNodeId(), connectsToRel.getEndNodeId()));
        }
        connectsToRel.setProperty(WEIGHT, --currentWeight);
    }

    private void decrementConnectsToWeightHP(Relationship connectsToRel) {
        Long currentWeightHP;
        if (connectsToRel.hasProperty(WEIGHT_HP)) {
            currentWeightHP = (Long) connectsToRel.getProperty(WEIGHT_HP);
        } else {
            log.error(String.format("No weightHP found on ConnectsTo relationship between nodes with neo4j ids %d and %d.", connectsToRel.getStartNodeId(), connectsToRel.getEndNodeId()));
            throw new RuntimeException(String.format("No weightHP found on ConnectsTo relationship between nodes with neo4j ids %d and %d.", connectsToRel.getStartNodeId(), connectsToRel.getEndNodeId()));
        }
        connectsToRel.setProperty(WEIGHT_HP, --currentWeightHP);
    }

    private void incrementMetaNodeTotalPostCount(Node metaNode) {
        if (metaNode != null) {
            Long currentTotalPostCount;
            if (metaNode.hasProperty(TOTAL_POST_COUNT)) {
                currentTotalPostCount = (Long) metaNode.getProperty(TOTAL_POST_COUNT);
                metaNode.setProperty(TOTAL_POST_COUNT, ++currentTotalPostCount);
            } else {
                log.warn("No totalPostCount property found on Meta node. This property will not be updated.");
            }
        } else {
            log.warn("No Meta node found. totalPostCount will not be updated.");
        }
    }

    private void decrementMetaNodeTotalPostCount(Node metaNode) {
        if (metaNode != null) {
            Long currentTotalPostCount;
            if (metaNode.hasProperty(TOTAL_POST_COUNT)) {
                currentTotalPostCount = (Long) metaNode.getProperty(TOTAL_POST_COUNT);
                metaNode.setProperty(TOTAL_POST_COUNT, --currentTotalPostCount);
            } else {
                log.warn("No totalPostCount property found on Meta node. This property will not be updated.");
            }
        } else {
            log.warn("No Meta node found. totalPostCount will not be updated.");
        }
    }

    private void decrementSegmentPreCount(Node segment) {
        if (segment.hasProperty(PRE)) {
            Long currentTotalPreCount = (Long) segment.getProperty(PRE);
            segment.setProperty(PRE, --currentTotalPreCount);
        } else {
            log.error("Segment pre count is absent: " + segment.getAllProperties());
            throw new RuntimeException("Segment pre count is absent: " + segment.getAllProperties());
        }
    }

    private void decrementSegmentPostCount(Node segment) {
        if (segment.hasProperty(POST)) {
            Long currentTotalPostCount = (Long) segment.getProperty(POST);
            segment.setProperty(POST, --currentTotalPostCount);
        } else {
            log.error("Segment post count is absent: " + segment.getAllProperties());
            throw new RuntimeException("Segment post count is absent: " + segment.getAllProperties());
        }
    }

    private void addSynapseToMetaRoiInfo(Node metaNode, String roiName, String synapseType) {
        if (metaNode != null) {
            if (metaNode.hasProperty(ROI_INFO)) {
                String metaRoiInfoString = (String) metaNode.getProperty(ROI_INFO);
                String roiInfoJsonString = addSynapseToRoiInfo(metaRoiInfoString, roiName, synapseType);
                metaNode.setProperty(ROI_INFO, roiInfoJsonString);
            } else {
                log.warn("No roiInfo property found on Meta node. roiInfo will not be updated.");
            }
        } else {
            log.warn("No Meta node found. roiInfo will not be updated.");
        }
    }

    private String removeSynapseFromRoiInfo(String roiInfoString, String roiName, String synapseType) {
        Map<String, SynapseCounter> roiInfoMap = getRoiInfoAsMap(roiInfoString);
        RoiInfo roiInfo = new RoiInfo(roiInfoMap);

        if (synapseType.equals(PRE)) {
            roiInfo.decrementPreForRoi(roiName);
        } else if (synapseType.equals(POST)) {
            roiInfo.decrementPostForRoi(roiName);
        }

        return roiInfo.getAsJsonString();

    }

    private void removeSynapseFromMetaRoiInfo(Node metaNode, String roiName, String synapseType) {
        if (metaNode != null) {
            if (metaNode.hasProperty(ROI_INFO)) {
                String metaRoiInfoString = (String) metaNode.getProperty(ROI_INFO);
                String roiInfoJsonString = removeSynapseFromRoiInfo(metaRoiInfoString, roiName, synapseType);
                metaNode.setProperty(ROI_INFO, roiInfoJsonString);
            } else {
                log.warn("No roiInfo property found on Meta node. roiInfo will not be updated.");
            }
        } else {
            log.warn("No Meta node found. roiInfo will not be updated.");
        }
    }

    private boolean roiInfoContainsRoi(String roiInfoString, String queriedRoi) {
        Map<String, org.janelia.flyem.neuprintloadprocedures.model.SynapseCounter> roiInfoMap = getRoiInfoAsMap(roiInfoString);
        return roiInfoMap.containsKey(queriedRoi);
    }

    private boolean shouldNotBeLabeledNeuron(Node neuronNode) {
        long preCount = 0;
        long postCount = 0;
        if (neuronNode.hasProperty(PRE)) {
            preCount = (long) neuronNode.getProperty(PRE);
        }
        if (neuronNode.hasProperty(POST)) {
            postCount = (long) neuronNode.getProperty(POST);
        }

        // returns true if meets the definition for a neuron
        return !(preCount >= 2 || postCount >= 10 || neuronNode.hasProperty(NAME) || neuronNode.hasProperty(INSTANCE) ||
                 neuronNode.hasProperty(PRIMARY_NEURITE) || neuronNode.hasProperty(MAJOR_INPUT) ||
                 neuronNode.hasProperty(MAJOR_OUTPUT) || neuronNode.hasProperty(CLONAL_UNIT) ||
                 neuronNode.hasProperty(NEUROTRANSMITTER) ||
                 neuronNode.hasProperty(SOMA_RADIUS) || neuronNode.hasProperty(STATUS));
    }

    private void removeNeuronDesignationFromNode(Node neuronNode, String datasetLabel) {
        // remove neuron labels
        neuronNode.removeLabel(Label.label(NEURON));
        neuronNode.removeLabel(Label.label(datasetLabel + "-" + NEURON));
        // remove cluster name
        neuronNode.removeProperty(CLUSTER_NAME);
    }

    private void deleteSegment(long bodyId, String datasetLabel) {

        final Node neuron = GraphTraversalTools.getSegment(dbService, bodyId, datasetLabel);

        if (neuron == null) {
            log.info("Segment with body ID " + bodyId + " not found in database. Aborting deletion...");
        } else {
            acquireWriteLockForSegmentSubgraph(neuron);

            if (neuron.hasRelationship(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                for (Relationship neuronContainsRel : neuron.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                    final Node containedNode = neuronContainsRel.getEndNode();
                    if (containedNode.hasLabel(Label.label(SYNAPSE_SET))) {
                        // delete the connection to the current node
                        neuronContainsRel.delete();
                        // delete relationships to synapses
                        containedNode.getRelationships().forEach(Relationship::delete);
                        //delete synapse set
                        containedNode.delete();
                    } else if (containedNode.hasLabel(Label.label(SKELETON))) {
                        // delete neuron relationship to skeleton
                        neuronContainsRel.delete();
                        // delete skeleton and skelnodes
                        deleteSkeleton(containedNode);
                    }
                }
            }

            //delete connection sets
            deleteConnectionSetsAndRelationships(neuron, FROM);
            deleteConnectionSetsAndRelationships(neuron, TO);

            // delete ConnectsTo relationships
            if (neuron.hasRelationship(RelationshipType.withName(CONNECTS_TO))) {
                neuron.getRelationships(RelationshipType.withName(CONNECTS_TO)).forEach(Relationship::delete);
            }

            // delete Neuron/Segment node
            neuron.delete();
            log.info("Deleted segment with body Id: " + bodyId);

        }

    }

    private void deleteConnectionSetsAndRelationships(final Node neuron, String type) {
        if (neuron.hasRelationship(RelationshipType.withName(type), Direction.INCOMING)) {
            for (Relationship fromRelationship : neuron.getRelationships(RelationshipType.withName(type))) {
                final Node connectionSet = fromRelationship.getStartNode();
                // delete relationships of connection set
                connectionSet.getRelationships().forEach(Relationship::delete);
                // delete connection set
                connectionSet.delete();
            }
        }
    }

    private void deleteSkeleton(final Node skeletonNode) {

        Set<Node> skelNodesToDelete = new HashSet<>();
        for (Relationship skeletonRelationship : skeletonNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
            Node skelNode = skeletonRelationship.getEndNode();
            //delete LinksTo relationships and
            skelNode.getRelationships(RelationshipType.withName(LINKS_TO)).forEach(Relationship::delete);
            //delete SkelNode Contains relationship to Skeleton
            skeletonRelationship.delete();
            skelNodesToDelete.add(skelNode);
        }

        //delete SkelNodes at end to avoid missing node errors
        skelNodesToDelete.forEach(Node::delete);

        //delete Skeleton
        skeletonNode.delete();
        log.info("Successfully deleted skeleton.");
    }

    private Node createSynapseSetForSegment(final Node segment, final String datasetLabel) {
        final Node newSynapseSet = dbService.createNode(Label.label(SYNAPSE_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + SYNAPSE_SET));
        long segmentBodyId;
        try {
            segmentBodyId = (long) segment.getProperty(BODY_ID);
        } catch (Exception e) {
            log.error("Error retrieving body ID from segment with Neo4j ID " + segment.getId() + ": " + e);
            throw new RuntimeException("Error retrieving body ID from segment with Neo4j ID " + segment.getId() + ": " + e);
        }
        newSynapseSet.setProperty(DATASET_BODY_ID, datasetLabel + ":" + segmentBodyId);
        segment.createRelationshipTo(newSynapseSet, RelationshipType.withName(CONTAINS));
        return newSynapseSet;
    }

    private Node getSegmentThatContainsSynapse(Node synapseNode) {
        Node connectedSegment = GraphTraversalTools.getSegmentThatContainsSynapse(synapseNode);
        if (connectedSegment == null) {
            log.info("Synapse does not belong to a body: " + synapseNode.getAllProperties());
        }
        return connectedSegment;
    }

    private void createConnectionSetsAndConnectsToRelationships(ConnectsToRelationshipMap connectsToRelationshipMap, String datasetLabel, Node metaNode, Set<String> metaNodeRoiSet) {

        for (String connectionKey : connectsToRelationshipMap.getSetOfConnectionKeys()) {
            final ConnectsToRelationship connectsToRelationship = connectsToRelationshipMap.getConnectsToRelationshipByKey(connectionKey);
            final Node startNode = connectsToRelationship.getStartNode();
            final Node endNode = connectsToRelationship.getEndNode();
            final long weight = connectsToRelationship.getWeight();
            // create a ConnectsTo relationship
            Relationship connectsToRel = addConnectsToRelationship(startNode, endNode, weight);

            Node connectionSet = createConnectionSetNode(datasetLabel, startNode, endNode);

            final Set<Node> synapsesForConnectionSet = connectsToRelationship.getSynapsesInConnectionSet();

            // add synapses to ConnectionSet
            for (final Node synapse : synapsesForConnectionSet) {
                // connection set Contains synapse
                connectionSet.createRelationshipTo(synapse, RelationshipType.withName(CONTAINS));
            }

            // add roi info to connection sets and weight hp to connections
            // get pre and post thresholds from meta node (if not present use 0.0)
            Map<String, Double> thresholdMap = getPreAndPostHPThresholdFromMetaNode(metaNode);

            int postHPCount = setConnectionSetRoiInfoAndGetWeightAndWeightHP(synapsesForConnectionSet, connectionSet, thresholdMap.get(PRE_HP_THRESHOLD), thresholdMap.get(POST_HP_THRESHOLD), metaNodeRoiSet)[1];
            connectsToRel.setProperty(WEIGHT_HP, postHPCount);

        }

    }

    private Node createConnectionSetNode(String datasetLabel, Node startSegment, Node endSegment) {
        // create a ConnectionSet node
        final Node connectionSet = dbService.createNode(Label.label(CONNECTION_SET), Label.label(datasetLabel), Label.label(datasetLabel + "-" + CONNECTION_SET));
        long startNodeBodyId;
        long endNodeBodyId;
        try {
            startNodeBodyId = (long) startSegment.getProperty(BODY_ID);
        } catch (Exception e) {
            log.error("Error retrieving body ID from segment with Neo4j ID " + startSegment.getId() + ": " + e);
            throw new RuntimeException("Error retrieving body ID from segment with Neo4j ID " + startSegment.getId() + ": " + e);
        }
        try {
            endNodeBodyId = (long) endSegment.getProperty(BODY_ID);
        } catch (Exception e) {
            log.error("Error retrieving body ID from segment with Neo4j ID " + endSegment.getId() + ": " + e);
            throw new RuntimeException("Error retrieving body ID from segment with Neo4j ID " + endSegment.getId() + ": " + e);
        }
        connectionSet.setProperty(DATASET_BODY_IDs, datasetLabel + ":" + startNodeBodyId + ":" + endNodeBodyId);

        // connect it to start and end bodies
        connectionSet.createRelationshipTo(startSegment, RelationshipType.withName(FROM));
        connectionSet.createRelationshipTo(endSegment, RelationshipType.withName(TO));

        return connectionSet;
    }

    private void addRoiPropertiesToSegmentGivenSynapseCountsPerRoi(Node segment, RoiInfo roiInfo) {
        for (String roi : roiInfo.getSetOfRois()) {
            segment.setProperty(roi, true);
        }
    }

    private Node addSkeletonNodes(final String dataset, final Skeleton skeleton, final Node segmentNode) {

        // create a skeleton node and connect it to the body
        Node skeletonNode = dbService.createNode(Label.label(SKELETON), Label.label(dataset + "-" + SKELETON), Label.label(dataset));
        skeletonNode.setProperty(SKELETON_ID, dataset + ":" + skeleton.getAssociatedBodyId());
        skeletonNode.setProperty(MUTATION_UUID_ID, skeleton.getMutationUuid().orElse("none") + ":" + skeleton.getMutationId().orElse(0L));
        segmentNode.createRelationshipTo(skeletonNode, RelationshipType.withName(CONTAINS));

        //add root nodes / other nodes to skeleton node
        List<SkelNode> skelNodeList = skeleton.getSkelNodeList();

        for (SkelNode skelNode : skelNodeList) {

            // create skelnode with id
            //try to get the node first, if it doesn't exist create it
            String skelNodeId = skelNode.getSkelNodeId(dataset);
            Node skelNodeNode = GraphTraversalTools.getSkelNode(dbService, skelNodeId, dataset);

            if (skelNodeNode == null) {
                skelNodeNode = createSkelNode(skelNodeId, dataset, skelNode);
            }

            //connect the skelnode to the skeleton
            skeletonNode.createRelationshipTo(skelNodeNode, RelationshipType.withName(CONTAINS));

            // add the children
            for (SkelNode childSkelNode : skelNode.getChildren()) {

                String childNodeId = childSkelNode.getSkelNodeId(dataset);
                Node childSkelNodeNode = dbService.findNode(Label.label(dataset + "-" + SKEL_NODE), SKEL_NODE_ID, childNodeId);
                if (childSkelNodeNode == null) {
                    childSkelNodeNode = createSkelNode(childNodeId, dataset, childSkelNode);
                }

                // add a link to the parent
                skelNodeNode.createRelationshipTo(childSkelNodeNode, RelationshipType.withName(LINKS_TO));

            }
        }

        return skeletonNode;
    }

    private Node createSkelNode(String skelNodeId, String dataset, SkelNode skelNode) {
        Node skelNodeNode = dbService.createNode(Label.label(SKEL_NODE), Label.label(dataset + "-" + SKEL_NODE), Label.label(dataset));
        skelNodeNode.setProperty(SKEL_NODE_ID, skelNodeId);

        //set location
        List<Integer> skelNodeLocation = skelNode.getLocation();
        Point skelNodeLocationPoint = new Location((long) skelNodeLocation.get(0), (long) skelNodeLocation.get(1), (long) skelNodeLocation.get(2));
        skelNodeNode.setProperty(LOCATION, skelNodeLocationPoint);

        //set radius, row number, type
        skelNodeNode.setProperty(RADIUS, skelNode.getRadius());
        skelNodeNode.setProperty(ROW_NUMBER, skelNode.getRowNumber());
        skelNodeNode.setProperty(TYPE, skelNode.getType());

        return skelNodeNode;
    }

    // Generic methods for removing features from graph and acquiring write locks

    private void removeAllLabels(Node node) {
        Iterable<Label> nodeLabels = node.getLabels();
        for (Label label : nodeLabels) {
            node.removeLabel(label);
        }
    }

    private void removeAllRelationshipsExceptTypesWithName(Node node, List<String> except) {
        Iterable<Relationship> nodeRelationships = node.getRelationships();
        for (Relationship relationship : nodeRelationships) {
            if (!except.contains(relationship.getType().name())) {
                relationship.delete();
            }
        }
    }

    private void removeAllRelationships(Node node) {
        Iterable<Relationship> nodeRelationships = node.getRelationships();
        for (Relationship relationship : nodeRelationships) {
            relationship.delete();
        }
    }

    private void acquireWriteLockForSegmentSubgraph(Node segment) {
        // neuron
        if (segment != null) {
            acquireWriteLockForNode(segment);
            // connects to relationships and 1-degree connections
            for (Relationship connectsToRelationship : segment.getRelationships(RelationshipType.withName(CONNECTS_TO))) {
                acquireWriteLockForRelationship(connectsToRelationship);
                acquireWriteLockForNode(connectsToRelationship.getOtherNode(segment));
            }
            // skeleton and synapse set
            for (Relationship containsRelationship : segment.getRelationships(RelationshipType.withName(CONTAINS))) {
                acquireWriteLockForRelationship(containsRelationship);
                Node skeletonOrSynapseSetNode = containsRelationship.getEndNode();
                acquireWriteLockForNode(skeletonOrSynapseSetNode);
                // skel nodes and synapses
                for (Relationship skelNodeOrSynapseRelationship : skeletonOrSynapseSetNode.getRelationships(RelationshipType.withName(CONTAINS), Direction.OUTGOING)) {
                    acquireWriteLockForRelationship(skelNodeOrSynapseRelationship);
                    Node skelNodeOrSynapseNode = skelNodeOrSynapseRelationship.getEndNode();
                    acquireWriteLockForNode(skelNodeOrSynapseNode);
                    // first degree relationships to synapses
                    for (Relationship synapsesToRelationship : skelNodeOrSynapseNode.getRelationships(RelationshipType.withName(SYNAPSES_TO))) {
                        acquireWriteLockForRelationship(synapsesToRelationship);
                        acquireWriteLockForNode(synapsesToRelationship.getOtherNode(skelNodeOrSynapseNode));
                    }
                    // links to relationships for skel nodes
                    for (Relationship linksToRelationship : skelNodeOrSynapseNode.getRelationships(RelationshipType.withName(LINKS_TO), Direction.OUTGOING)) {
                        acquireWriteLockForRelationship(linksToRelationship);
                    }
                }
            }
            // connection sets
            for (Relationship toRelationship : segment.getRelationships(RelationshipType.withName(TO))) {
                acquireWriteLockForRelationship(toRelationship);
                Node connectionSetNode = toRelationship.getStartNode();
                acquireWriteLockForNode(connectionSetNode);
                Relationship fromRelationship = connectionSetNode.getSingleRelationship(RelationshipType.withName(FROM), Direction.OUTGOING);
                acquireWriteLockForRelationship(fromRelationship);
            }
            for (Relationship fromRelationship : segment.getRelationships(RelationshipType.withName(FROM))) {
                acquireWriteLockForRelationship(fromRelationship);
                Node connectionSetNode = fromRelationship.getStartNode();
                Relationship toRelationship = connectionSetNode.getSingleRelationship(RelationshipType.withName(TO), Direction.OUTGOING);
                acquireWriteLockForRelationship(toRelationship);
            }
        }
    }

    private void acquireWriteLockForNode(Node node) {
        if (node != null) {
            try (Transaction tx = dbService.beginTx()) {
                tx.acquireWriteLock(node);
                tx.success();
            }
        }
    }

    private void acquireWriteLockForRelationship(Relationship relationship) {
        if (relationship != null) {
            try (Transaction tx = dbService.beginTx()) {
                tx.acquireWriteLock(relationship);
                tx.success();
            }
        }
    }
}

//    Left in case there is a desire to switch back to having a "mergeNeurons" API
//        private void mergeSynapseSets(Node synapseSet1, Node synapseSet2) {
//
//        //add both synapse sets to the new node, collect them for adding to apoc merge procedure
//        Map<String, Object> parametersMap = new HashMap<>();
//        if (node1SynapseSetNode != null) {
//            newNode.createRelationshipTo(node1SynapseSetNode, RelationshipType.withName(CONTAINS));
//            parametersMap.put("ssnode1", node1SynapseSetNode);
//        }
//        if (node2SynapseSetNode != null) {
//            newNode.createRelationshipTo(node2SynapseSetNode, RelationshipType.withName(CONTAINS));
//            parametersMap.put("ssnode2", node2SynapseSetNode);
//        }
//
//        // merge the two synapse nodes using apoc if there are two synapseset nodes. inherits the datasetBodyId of the first node
//        if (parametersMap.containsKey("ssnode1") && parametersMap.containsKey("ssnode2")) {
//            dbService.execute("CALL apoc.refactor.mergeNodes([$ssnode1, $ssnode2], {properties:{datasetBodyId:\"discard\"}}) YIELD node RETURN node", parametersMap).next().get("node");
//            //delete the extra relationship between new node and new synapse set node
//            newNode.getRelationships(RelationshipType.withName(CONTAINS)).iterator().next().delete();
//        }
//    }


