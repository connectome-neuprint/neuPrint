package org.janelia.flyem.neuprinter.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.janelia.flyem.neuprinter.json.JsonUtils;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

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
     * @return the total number pre-synapses associated with this body.
     */
    public Integer getNumberOfPreSynapses() {
        return this.numberOfPreSynapses;
    }

    /**
     * @return the total number post-synapses associated with this body.
     */
    public Integer getNumberOfPostSynapses() {
        return this.numberOfPostSynapses;
    }

    public SynapseCountsPerRoi getSynapseCountsPerRoi() {
        return this.synapseCountsPerRoi;
    }

    public Set<Synapse> getSynapseSet() {
        return synapseSet;
    }

    public HashMap<Long, Integer> getConnectsTo() {
        return connectsTo;
    }

    public HashMap<Long, Integer> getConnectsFrom() {
        return connectsFrom;
    }

    @Override
    public String toString() {
        return "BodyWithSynapses { " + "bodyid= " + this.bodyId +
                ", SynapseSet= " + this.synapseSet + " }";
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

    public List<String> getPreLocations() {
        List<String> preLocations = new ArrayList<>();
        for (Synapse synapse : this.synapseSet) {
            if (synapse.getType().equals("pre")) {
                preLocations.add(synapse.getLocationString());
            }
        }
        return preLocations;
    }

    public List<String> getPostLocations() {
        List<String> postLocations = new ArrayList<>();
        for (Synapse synapse : this.synapseSet) {
            if (synapse.getType().equals("post")) {
                postLocations.add(synapse.getLocationString());
            }
        }
        return postLocations;
    }

    public List<String> getBodyRois() {
        List<String> bodyRois = new ArrayList<>();
        for (Synapse synapse : this.synapseSet) {
            bodyRois.addAll(synapse.getRois());
        }
        return bodyRois;
    }

    public void addSynapseToBodyIdMapAndSetSynapseCounts(String mapType, SynapseLocationToBodyIdMap synapseLocationToBodyIdMap) throws IllegalArgumentException {

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
        for (Synapse synapse : this.synapseSet) {
            if (synapse.getType().equals("pre")) {
                List<String> postsynapticPartners = synapse.getConnectionLocationStrings();
                List<Long> postsynapticPartnerIds = postLocsToBodyIds(postsynapticPartners, postToBody);
                for (Long partnerId : postsynapticPartnerIds) {
                    if (partnerId != null) {
                        int count = this.connectsTo.containsKey(partnerId) ? this.connectsTo.get(partnerId) : 0;
                        this.connectsTo.put(partnerId, count + 1);
                    } else {
                        LOG.info(synapse.getLocationString() + " on " + this.bodyId + " has no bodyId for postsynaptic partner.");

                    }
                }
            }
        }
    }

    public void addSynapsesToPreToPostMap(HashMap<String, List<String>> preToPost) {

        for (Synapse synapse : this.synapseSet) {
            if (synapse.getType().equals("pre")) {
                List<String> postsynapticPartners = synapse.getConnectionLocationStrings();
                preToPost.put(synapse.getLocationString(), postsynapticPartners);
            }
        }
    }

    private List<Long> postLocsToBodyIds(List<String> postsynapticPartners, SynapseLocationToBodyIdMap postToBody) {
        List<Long> postsynapticPartnerIds = new ArrayList<>();
        for (String psdLocation : postsynapticPartners) {
            postsynapticPartnerIds.add(postToBody.getBodyId(psdLocation));
            if (postToBody.getBodyId(psdLocation) == null) {
                System.out.println(psdLocation + " not in postToBody.");
            }
        }
        return postsynapticPartnerIds;
    }

    public void setSynapseCountsPerRoi() {
        this.synapseCountsPerRoi = getSynapseCountersPerRoiFromSynapseSet(this.synapseSet);
    }

    public static SynapseCountsPerRoi getSynapseCountersPerRoiFromSynapseSet(Set<Synapse> synapseSet) {
        SynapseCountsPerRoi synapseCountsPerRoi = new SynapseCountsPerRoi();
        for (Synapse synapse : synapseSet) {
            for (String roi : synapse.getRois()) {
                if (synapse.getType().equals("pre")) {
                    synapseCountsPerRoi.incrementPreForRoi(roi);
                } else if (synapse.getType().equals("post")) {
                    synapseCountsPerRoi.incrementPostForRoi(roi);
                }
            }
        }
        return synapseCountsPerRoi;
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
