package org.janelia.flyem.neuprinter.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.janelia.flyem.neuprinter.json.JsonUtils;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * A class representing a body that contains synapses, as read from a
 * <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">synapses JSON file</a>.
 * Each body has a bodyId and a set of synapses. (cf. the {@link Neuron} class
 * that represents neurons without synapses and their properties)
 */
public class BodyWithSynapses {

    //TODO: figure out how to add optional properties

    @SerializedName("BodyId")
    private final Long bodyId;

    @SerializedName("SynapseSet")
    private final Set<Synapse> synapseSet;
    // TODO: check for attempts to add duplicate synapses

    private transient HashMap<Long, SynapseCounter> connectsTo; //Map of body IDs and weights
    private transient HashMap<Long, SynapseCounter> connectsFrom; //Map of body IDs and weights
    private transient Integer numberOfPreSynapses;
    private transient Integer numberOfPostSynapses;

    private transient SynapseCountsPerRoi synapseCountsPerRoi;

    private static transient String PRE = "pre";
    private static transient String POST = "post";

    /**
     * Class constructor.
     *
     * @param bodyId     bodyId for this neuron
     * @param synapseSet this neuron's set of synapses
     */
    public BodyWithSynapses(Long bodyId, Set<Synapse> synapseSet) {
        this.bodyId = bodyId;
        this.synapseSet = synapseSet; // LinkedHashSet ? if want to preserve order
    }

    /**
     * @return the bodyId associated with this body
     */
    public Long getBodyId() {
        return this.bodyId;
    }

    /**
     * @return the total number presynaptic densities associated with this body
     */
    public Integer getNumberOfPreSynapses() {
        return this.numberOfPreSynapses;
    }

    /**
     * @return the total number postsynaptic densities associated with this body
     */
    public Integer getNumberOfPostSynapses() {
        return this.numberOfPostSynapses;
    }

    /**
     * @return number of synaptic densities (pre and post) for each ROI associated with this body as SynapseCountsPerRoi object
     */
    public SynapseCountsPerRoi getSynapseCountsPerRoi() {
        return this.synapseCountsPerRoi;
    }

    /**
     * @return set of synapses associated with this body
     */
    public Set<Synapse> getSynapseSet() {
        return synapseSet;
    }

    /**
     * @return map of postsynaptic bodyIds to ConnectsTo weights for this body
     */
    public HashMap<Long, SynapseCounter> getConnectsTo() {
        return this.connectsTo;
    }

    /**
     * @return map of presynaptic bodyIds to weights for this body
     */
    public HashMap<Long, SynapseCounter> getConnectsFrom() {
        return connectsFrom;
    }

    /**
     * @return list of presynaptic density locations for this body as strings ("x:y:z")
     */
    List<String> getPreLocations() {
        return this.synapseSet.stream()
                .filter(synapse -> synapse.getType().equals(PRE))
                .map(Synapse::getLocationString)
                .collect(Collectors.toList());
    }

    /**
     * @return list of postsynaptic density locations for this body as strings ("x:y:z")
     */
    List<String> getPostLocations() {
        return this.synapseSet.stream()
                .filter(synapse -> synapse.getType().equals(POST))
                .map(Synapse::getLocationString)
                .collect(Collectors.toList());
    }

    /**
     * @return rois that this body has synapses in
     */
    List<String> getBodyRois() {
        return this.synapseSet.stream()
                .map(Synapse::getRois)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    /**
     * Adds synapses for this body to a {@link SynapseLocationToBodyIdMap}, which
     * maps three-dimensional synapse locations to a bodyId. For use with
     * {@link org.janelia.flyem.neuprinter.SynapseMapper}. To map presynaptic
     * or postsynaptic locations to a bodyId, mapType should be "pre" or "post",
     * respectively.
     *
     * @param mapType                    type of SynapseLocationToBodyIdMap
     * @param synapseLocationToBodyIdMap map of x,y,z synapse locations to bodyIds
     * @throws IllegalArgumentException if mapType is not "pre" or "post"
     */
    public void addSynapsesToBodyIdMapAndSetSynapseCounts(String mapType, SynapseLocationToBodyIdMap synapseLocationToBodyIdMap) throws IllegalArgumentException {

        int countPre = 0;
        int countPost = 0;
        switch (mapType) {

            case "pre":
                for (Synapse synapse : this.synapseSet) {
                    if (synapse.getType().equals("pre")) {
                        String preLocation = synapse.getLocationString();
                        synapseLocationToBodyIdMap.mapLocationToBodyId(preLocation, this.bodyId);
                        countPre++;
                    } else if (synapse.getType().equals("post")) {
                        countPost++;
                    }
                }
                break;
            case "post":
                for (Synapse synapse : this.synapseSet) {
                    if (synapse.getType().equals("post")) {
                        String postLocation = synapse.getLocationString();
                        synapseLocationToBodyIdMap.mapLocationToBodyId(postLocation, this.bodyId);
                        countPost++;
                    } else if (synapse.getType().equals("pre")) {
                        countPre++;
                    }

                }
                break;
            default:
                throw new IllegalArgumentException("Incorrect input to function; use pre or post");
        }

        this.numberOfPreSynapses = countPre;
        this.numberOfPostSynapses = countPost;

    }

    /**
     * Uses a {@link SynapseLocationToBodyIdMap} mapping postsynaptic density
     * locations to bodyIds and this body's synapse set to generate a map of
     * postsynaptic bodyIds to weights* for this body and set this map as the
     * connectsTo attribute. This is used to set ConnectsTo relationships between
     * Neuron nodes in the neo4j database. Complete SynapseLocationToBodyIdMap must
     * be generated prior to using this method. (*Note: weight is equal to the number
     * of SynapsesTo relationships for this connection. Assuming that the ratio of
     * pre to post for each synaptic connection can be 1:1 or 1:many but never many:1,
     * this is equal to number of postsynaptic densities for a connection.)
     *
     * @param postToBody map of postsynaptic locations to bodyIds
     */
    public void setConnectsTo(SynapseLocationToBodyIdMap postToBody) {
        this.connectsTo = new HashMap<>();
        // weight is the number of postsynaptic densities
        this.synapseSet
                .stream()
                .filter(synapse -> synapse.getType().equals(PRE))
                .collect(Collectors.toSet())
                .forEach(preSynapse -> {

                    // get list of body ids that connect to this presynaptic density for counting post per connection
                    List<Long> postSynapticBodyIdsForSynapse = getPostSynapticBodyIdsForSynapse(preSynapse, postToBody);
                    // get set of body ids that connect to this presynaptic density for counting pre per connection
                    Set<Long> postSynapticBodyIdsForSynapseSet = new HashSet<>(postSynapticBodyIdsForSynapse);

                    for (Long partnerId : postSynapticBodyIdsForSynapse) {
                        if (partnerId != null) {
                            SynapseCounter synapseCounter = this.connectsTo.getOrDefault(partnerId, new SynapseCounter());
                            synapseCounter.incrementPost();
                            this.connectsTo.put(partnerId, synapseCounter);
                        } else {
                            LOG.warning(preSynapse.getLocationString() + " on " + this.bodyId + " has no bodyId for postsynaptic partner.");
                        }
                    }

                    for (Long partnerId : postSynapticBodyIdsForSynapseSet) {
                        if (partnerId != null) {
                            this.connectsTo.get(partnerId).incrementPre();
                        }
                    }
                });
    }

    /**
     * Adds synapses in this body's synapse set to the provided preToPost map, which
     * maps presynaptic locations to postsynaptic locations. The preToPost map is used
     * to generate SynapsesTo relationships between Synapse nodes in the neo4j database.
     *
     * @param preToPost map of presynaptic locations to postsynaptic locations
     */
    public void addSynapsesToPreToPostMap(HashMap<String, Set<String>> preToPost) {
        this.synapseSet
                .stream()
                .filter(synapse -> synapse.getType().equals(PRE))
                .collect(Collectors.toSet())
                .forEach(preSynapse -> preToPost.put(preSynapse.getLocationString(), preSynapse.getConnectionLocationStrings()));
    }

    private List<Long> getPostSynapticBodyIdsForSynapse(Synapse synapse, SynapseLocationToBodyIdMap postToBody) {
        return synapse
                .getConnectionLocationStrings()
                .stream()
                .map(s -> {
                    if (postToBody.getBodyId(s) == null) {
                        LOG.warning(s + " not in postToBody.");
                    }
                    return postToBody.getBodyId(s);
                })
                .collect(Collectors.toList());
    }

    /**
     * Sets the {@link #synapseCountsPerRoi} attribute using this body's synapse set.
     */
    public void setSynapseCountsPerRoi() {
        this.synapseCountsPerRoi = getSynapseCountersPerRoiFromSynapseSet(this.synapseSet);
    }

    public static SynapseCountsPerRoi getSynapseCountersPerRoiFromSynapseSet(Set<Synapse> synapseSet) {
        SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();
        synapseSet.forEach(synapse -> {
            if (synapse.getType().equals(PRE)) {
                synapse.getRois().forEach(synapseCountsPerRoi::incrementPreForRoi);
            } else if (synapse.getType().equals(POST)) {
                synapse.getRois().forEach(synapseCountsPerRoi::incrementPostForRoi);
            }
        });
        return synapseCountsPerRoi;
    }

    @Override
    public String toString() {
        return "BodyWithSynapses { " + "bodyid= " + this.bodyId +
                ", synapseSet= " + this.synapseSet + " }";
    }

    @Override
    public boolean equals(final Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof BodyWithSynapses) {
            final BodyWithSynapses that = (BodyWithSynapses) o;
            isEqual = Objects.equals(this.bodyId, that.bodyId);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + bodyId.hashCode();
        return result;
    }

    /**
     * Returns a list of BodyWithSynapses deserialized from a synapses JSON string.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">synapses JSON format</a>.
     *
     * @param jsonString string containing synapses JSON
     * @return list of BodyWithSynapses
     */
    public static List<BodyWithSynapses> fromJson(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, BODY_LIST_TYPE);
    }

    /**
     * Returns a list of BodyWithSynapses deserialized from a {@link BufferedReader} reading from a synapse JSON file.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">synapse JSON format</a>.
     *
     * @param reader {@link BufferedReader}
     * @return list of BodyWithSynapses
     */
    public static List<BodyWithSynapses> fromJson(final BufferedReader reader) {
        return JsonUtils.GSON.fromJson(reader, BODY_LIST_TYPE);
    }

    /**
     * Returns a BodyWithSynapse deserialized from a single JSON object from a synapse JSON file.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">synapse JSON format</a>.
     *
     * @param reader {@link JsonReader}
     * @return BodyWithSynapse
     */
    public static BodyWithSynapses fromJsonSingleObject(final JsonReader reader) {
        return JsonUtils.GSON.fromJson(reader, BodyWithSynapses.class);
    }

    private static Type BODY_LIST_TYPE = new TypeToken<List<BodyWithSynapses>>() {
    }.getType();

    private static final Logger LOG = Logger.getLogger("BodyWithSynapses.class");

}


