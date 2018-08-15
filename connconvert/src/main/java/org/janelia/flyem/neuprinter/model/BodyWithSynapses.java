package org.janelia.flyem.neuprinter.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.janelia.flyem.neuprinter.json.JsonUtils;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class BodyWithSynapses {

    //TODO: figure out how to add optional properties

    @SerializedName("BodyId")
    private final Long bodyId;

    @SerializedName("SynapseSet")
    private final Set<Synapse> synapseSet;
    // TODO: check for attempts to add duplicate synapses

    private transient HashMap<Long, Integer> connectsTo; //Map of body IDs and weights
    private transient HashMap<Long, Integer> connectsFrom; //Map of body IDs and weights
    private transient Integer numberOfPreSynapses;
    private transient Integer numberOfPostSynapses;

    private transient SynapseCountsPerRoi synapseCountsPerRoi;

    private static transient String PRE = "pre";
    private static transient String POST = "post";

    public BodyWithSynapses(Long bodyId, Set<Synapse> synapseSet) {
        this.bodyId = bodyId;
        this.synapseSet = synapseSet; // LinkedHashSet ? if want to preserve order

    }

    /**
     * @return the bodyId associated with this body.
     */
    public Long getBodyId() {
        return this.bodyId;
    }

    /**
     * @return the total number presynaptic densities associated with this body.
     */
    public Integer getNumberOfPreSynapses() {
        return this.numberOfPreSynapses;
    }

    /**
     * @return the total number postsynaptic densities associated with this body.
     */
    public Integer getNumberOfPostSynapses() {
        return this.numberOfPostSynapses;
    }

    /**
     * @return number of synaptic densities (pre and post) for each ROI associated with this body as SynapseCountsPerRoi object.
     */
    public SynapseCountsPerRoi getSynapseCountsPerRoi() {
        return this.synapseCountsPerRoi;
    }

    /**
     * @return set of synapses associated with this body.
     */
    public Set<Synapse> getSynapseSet() {
        return synapseSet;
    }

    /**
     * @return map of postsynaptic bodyIds to weights for this body.
     */
    public HashMap<Long, Integer> getConnectsTo() {
        return connectsTo;
    }

    /**
     * @return map of presynaptic bodyIds to weights for this body.
     */
    public HashMap<Long, Integer> getConnectsFrom() {
        return connectsFrom;
    }

    /**
     * @return list of presynaptic density locations for this body as strings ("x:y:z").
     */
    public List<String> getPreLocations() {
        return this.synapseSet.stream()
                .filter(synapse -> synapse.getType().equals(PRE))
                .map(Synapse::getLocationString)
                .collect(Collectors.toList());
    }

    /**
     * @return list of postsynaptic density locations for this body as strings ("x:y:z").
     */
    public List<String> getPostLocations() {
        return this.synapseSet.stream()
                .filter(synapse -> synapse.getType().equals(POST))
                .map(Synapse::getLocationString)
                .collect(Collectors.toList());
    }

    /**
     * @return List of postsynaptic density locations for this body as strings ("x:y:z").
     */
    public List<String> getBodyRois() {
        return this.synapseSet.stream()
                .map(Synapse::getRois)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

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

    public void setConnectsTo(SynapseLocationToBodyIdMap postToBody) {
        this.connectsTo = new HashMap<>();
        this.synapseSet
                .stream()
                .filter(synapse -> synapse.getType().equals(PRE))
                .collect(Collectors.toSet())
                .forEach(preSynapse -> {
                    List<Long> postSynapticBodyIdsForSynapse = getPostSynapticBodyIdsForSynapse(preSynapse, postToBody);
                    for (Long partnerId : postSynapticBodyIdsForSynapse) {
                        if (partnerId != null) {
                            int count = this.connectsTo.getOrDefault(partnerId, 0);
                            this.connectsTo.put(partnerId, count + 1);
                        } else {
                            LOG.warning(preSynapse.getLocationString() + " on " + this.bodyId + " has no bodyId for postsynaptic partner.");
                        }
                    }
                });
    }

    public void addSynapsesToPreToPostMap(HashMap<String, List<String>> preToPost) {
        this.synapseSet
                .stream()
                .filter(synapse -> synapse.getType().equals(PRE))
                .collect(Collectors.toSet())
                .forEach(preSynapse -> preToPost.put(preSynapse.getLocationString(),preSynapse.getConnectionLocationStrings()));
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

    public void setSynapseCountsPerRoi() {
        this.synapseCountsPerRoi = getSynapseCountersPerRoiFromSynapseSet(this.synapseSet);
    }

    public static SynapseCountsPerRoi getSynapseCountersPerRoiFromSynapseSet(Set<Synapse> synapseSet) {
        SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();
        synapseSet.forEach(synapse -> {
            if (synapse.getType().equals(PRE)) {
                synapse.getRois().forEach(roi -> synapseCountsPerRoi.incrementPreForRoi(roi.replace("-pt","")));
            } else if (synapse.getType().equals(POST)) {
                synapse.getRois().forEach(roi -> synapseCountsPerRoi.incrementPostForRoi(roi.replace("-pt","")));
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

    public static List<BodyWithSynapses> fromJson(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, BODY_LIST_TYPE);
    }

    public static List<BodyWithSynapses> fromJson(final BufferedReader reader) {
        return JsonUtils.GSON.fromJson(reader, BODY_LIST_TYPE);
    }

    public static BodyWithSynapses fromJsonSingleObject(final JsonReader reader) {
        return JsonUtils.GSON.fromJson(reader, BodyWithSynapses.class);
    }

    private static Type BODY_LIST_TYPE = new TypeToken<List<BodyWithSynapses>>() {
    }.getType();

    private static final Logger LOG = Logger.getLogger("BodyWithSynapses.class");

}
