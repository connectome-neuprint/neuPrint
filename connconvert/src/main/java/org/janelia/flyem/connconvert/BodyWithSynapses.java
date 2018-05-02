package org.janelia.flyem.connconvert;

import com.google.gson.annotations.SerializedName;

import java.util.*;


public class BodyWithSynapses {

    @SerializedName("BodyId")
    private final Long bodyId;

    @SerializedName("SynapseSet")
    private final Set<Synapse> synapseSet;
    // TODO: check for attempts to add duplicate synapses

    public HashMap<Long,Integer> connectsTo = new HashMap<>(); //Map of body IDs and weights
    public HashMap<Long,Integer> connectsFrom = new HashMap<>(); //Map of body IDs and weights
    private Integer pre; //number of presyn terminals
    private Integer post; //number of postsyn terminals
    // body divided into multiple neuron parts based on roi
    private List<NeuronPart> neuronParts;

    public BodyWithSynapses() {
        this.bodyId = null;
        this.synapseSet = new HashSet<>(); // LinkedHashSet ? if want to preserve order
    }

/*//


    private transient int totalNumberOfPreSynapticTerminals;
    private transient int totalNumberOfPostSynapticTerminals;

    private transient Map<Body, Integer> connectsToBodyCounts;


    //*/

    public Long getBodyId(){
        return this.bodyId;
    }

    public Integer getPre() {
        return this.pre;
    }

    public Integer getPost() {
        return this.post;
    }

    public List<NeuronPart> getNeuronParts() {
        return this.neuronParts;
    }

    public Set<Synapse> getSynapseSet() {
        return synapseSet;
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


    public void setConnectsTo(HashMap<String,Long> postToBody) {
        for (Synapse synapse: this.synapseSet) {
            if (synapse.getType().equals("pre")) {
                List<String> postsynapticPartners = synapse.getConnectionLocationStrings();
                List<Long> postsynapticPartnerIds = postLocsToBodyIds(postsynapticPartners, postToBody);
                for (Long partnerId : postsynapticPartnerIds) {
                    if (partnerId!=null) {
                        int count = this.connectsTo.containsKey(partnerId) ? this.connectsTo.get(partnerId) : 0;
                        this.connectsTo.put(partnerId, count + 1);
                    } else {
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

    public void setConnectsFrom(HashMap<String,Long> preToBody) {
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

    private List<Long> postLocsToBodyIds (List<String> postsynapticPartners, HashMap<String,Long> postToBody) {
        List<Long> postsynapticPartnerIds = new ArrayList<>();
        for (String psd : postsynapticPartners) {
            postsynapticPartnerIds.add(postToBody.get(psd));
            if (postToBody.get(psd)==null){
                System.out.println(psd + " not in postToBody.");
            }
        }
        return postsynapticPartnerIds;
    }

    private void setPre(){
        int countPre = 0;
        for (Synapse synapse: this.synapseSet) {
            if (synapse.getType().equals("pre")) {
                countPre++;
            }
        }
        this.pre = countPre;
    }

    private void setPost(){
        int countPost = 0;
        for (Synapse synapse: this.synapseSet) {
            if (synapse.getType().equals("post")) {
                countPost++;
            }
        }
        this.post = countPost;
    }

    public void setSynapseCounts(){
        this.setPost();
        this.setPre();
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








}
