package org.janelia.flyem.connconvert.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.janelia.flyem.connconvert.json.JsonUtils;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.util.*;


public class BodyWithSynapses {

    //TODO: figure out how to add optional properties

    @SerializedName("BodyId")
    private final Long bodyId;

    @SerializedName("SynapseSet")
    private final Set<Synapse> synapseSet;
    // TODO: check for attempts to add duplicate synapses

    private transient HashMap<Long,Integer> connectsTo; //Map of body IDs and weights
    private transient HashMap<Long,Integer> connectsFrom; //Map of body IDs and weights
    private transient Integer numberOfPreSynapses;
    private transient Integer numberOfPostSynapses;

    // body divided into multiple neuron parts based on roi
    private transient List<NeuronPart> neuronParts;

    public BodyWithSynapses(Long bodyId, Set<Synapse> synapseSet) {
        this.bodyId = bodyId;
        this.synapseSet = synapseSet; // LinkedHashSet ? if want to preserve order


    }
    
    /**
     * @return the bodyId associated with this body.
     */
    public Long getBodyId(){
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

    public List<NeuronPart> getNeuronParts() {
        return this.neuronParts;
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
        //noinspection ConstantConditions
        return bodyId.hashCode();
    }

    public List<String> getPreLocations() {
        List<String> preLocations = new ArrayList<>();
        for (Synapse synapse: this.synapseSet) {
            if (synapse.getType().equals("pre")) {
                preLocations.add(synapse.getLocationString());
            }
        }
        return preLocations;
    }

    public List<String> getPostLocations() {
        List<String> postLocations = new ArrayList<>();
        for (Synapse synapse: this.synapseSet) {
            if (synapse.getType().equals("post")) {
                postLocations.add(synapse.getLocationString());
            }
        }
        return postLocations;
    }

    public List<String> getBodyRois() {
        List<String> bodyRois = new ArrayList<>();
        for (Synapse synapse: this.synapseSet) {
            bodyRois.addAll(synapse.getRois());
        }
        return bodyRois;
    }

    public void addSynapseToBodyIdMapAndSetSynapseCounts(String mapType, SynapseLocationToBodyIdMap synapseLocationToBodyIdMap ) throws IllegalArgumentException {

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
        for (Synapse synapse: this.synapseSet) {
            if (synapse.getType().equals("pre")) {
                List<String> postsynapticPartners = synapse.getConnectionLocationStrings();
                List<Long> postsynapticPartnerIds = postLocsToBodyIds(postsynapticPartners, postToBody);
                for (Long partnerId : postsynapticPartnerIds) {
                    if (partnerId!=null) {
                        int count = this.connectsTo.containsKey(partnerId) ? this.connectsTo.get(partnerId) : 0;
                        this.connectsTo.put(partnerId, count + 1);
                    } else {
                        // TODO: change this to log
                        System.out.println(synapse.getLocationString() + " on " + this.bodyId + " has no bodyId for postsynaptic partner.");

                    }
                }
            }
        }
    }

    public HashMap<String,List<String>> getPreToPostForBody() {
        HashMap<String,List<String>> preToPost = new HashMap<>();
        for (Synapse synapse: this.synapseSet) {
            if (synapse.getType().equals("pre")) {
                List<String> postsynapticPartners = synapse.getConnectionLocationStrings();
                preToPost.put(synapse.getLocationString(), postsynapticPartners);
            }
        }
        return preToPost;
    }

    public void setConnectsFrom(SynapseLocationToBodyIdMap preToBody) {
        for (Synapse synapse: this.synapseSet) {
            if (synapse.getType().equals("post")) {
                List<String> postsynapticPartners = synapse.getConnectionLocationStrings();
                List<Long> postsynapticPartnerIds = postLocsToBodyIds(postsynapticPartners, preToBody);
                for (Long partnerId : postsynapticPartnerIds) {
                    int count = this.connectsFrom.containsKey(partnerId) ? this.connectsFrom.get(partnerId) : 0;
                    this.connectsFrom.put(partnerId, count+1);

                }
            }
        }
    }

    private List<Long> postLocsToBodyIds (List<String> postsynapticPartners, SynapseLocationToBodyIdMap postToBody) {
        List<Long> postsynapticPartnerIds = new ArrayList<>();
        for (String psdLocation : postsynapticPartners) {
            postsynapticPartnerIds.add(postToBody.getBodyId(psdLocation));
            if (postToBody.getBodyId(psdLocation)==null){
                System.out.println(psdLocation + " not in postToBody.");
            }
        }
        return postsynapticPartnerIds;
    }



    public void setNeuronParts() {
        neuronParts = new ArrayList<>();
        HashMap<String,SynapseCounter> roiToPrePostCount = new HashMap<>();
        for (Synapse synapse : this.synapseSet) {

            for (String roi : synapse.getRois()) {
                if (synapse.getType().equals("pre")) {
                    if (roiToPrePostCount.containsKey(roi)) {
                        roiToPrePostCount.get(roi).incrementPreCount();
                    } else {
                        roiToPrePostCount.put(roi,new SynapseCounter());
                        roiToPrePostCount.get(roi).incrementPreCount();
                    }
                } else if (synapse.getType().equals("post")) {
                    if (roiToPrePostCount.containsKey(roi)) {
                        roiToPrePostCount.get(roi).incrementPostCount();
                    } else {
                        roiToPrePostCount.put(roi,new SynapseCounter());
                        roiToPrePostCount.get(roi).incrementPostCount();
                    }
                }
            }


        }

        for (String roi : roiToPrePostCount.keySet()) {
            neuronParts.add(new NeuronPart(roi,roiToPrePostCount.get(roi).getPreCount(), roiToPrePostCount.get(roi).getPostCount()));
        }

    }

    public static List<BodyWithSynapses> fromJson(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, BODY_LIST_TYPE);
    }

    public static List<BodyWithSynapses> fromJson(final BufferedReader reader) {
        return JsonUtils.GSON.fromJson(reader,BODY_LIST_TYPE);
    }

    public static BodyWithSynapses fromJsonSingleObject(final JsonReader reader) {
        return JsonUtils.GSON.fromJson(reader, BodyWithSynapses.class);
    }


    private static Type BODY_LIST_TYPE = new TypeToken<List<BodyWithSynapses>>(){}.getType();












}
