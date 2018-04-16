package connconvert;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

public class BodyWithSynapses {
    public Integer bodyId;
    public List<Synapse> synapseSet;
    public HashMap<Integer,Integer> connectsTo = new HashMap<>(); //Map of body IDs and weights
    public HashMap<Integer,Integer> connectsFrom = new HashMap<>(); //Map of body IDs and weights
    public int pre; //number of presyn terminals
    public int post; //number of postsyn terminals
    //public List<Label> labels = new ArrayList<>();


    @Override
    public String toString() {
        return "BodyWithSynapses{" + "bodyid= " + bodyId +
                ", SynapseSet= " + synapseSet + "}";
    }

    // for this body, what are the presynaptic terminal locations
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

    public int getBodyId(){
        return this.bodyId;
    }

    public void setConnectsTo(HashMap<String,Integer> postToBody) {
        for (Synapse synapse: this.synapseSet) {
            if (synapse.getType().equals("pre")) {
                List<String> postsynapticPartners = synapse.getConnectionLocationStrings();
                List<Integer> postsynapticPartnerIds = postLocsToBodyIds(postsynapticPartners, postToBody);
                for (Integer partnerId : postsynapticPartnerIds) {
                    int count = this.connectsTo.containsKey(partnerId) ? this.connectsTo.get(partnerId) : 0;
                    this.connectsTo.put(partnerId, count+1);

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

    public void setConnectsFrom(HashMap<String,Integer> preToBody) {
        for (Synapse synapse: this.synapseSet) {
            if (synapse.getType().equals("post")) {
                List<String> postsynapticPartners = synapse.getConnectionLocationStrings();
                List<Integer> postsynapticPartnerIds = postLocsToBodyIds(postsynapticPartners, preToBody);
                for (Integer partnerId : postsynapticPartnerIds) {
                    int count = this.connectsFrom.containsKey(partnerId) ? this.connectsFrom.get(partnerId) : 0;
                    this.connectsFrom.put(partnerId, count+1);

                }
            }
        }
    }

    private List<Integer> postLocsToBodyIds (List<String> postsynapticPartners, HashMap<String,Integer> postToBody) {
        List<Integer> postsynapticPartnerIds = new ArrayList<>();
        for (String psd : postsynapticPartners) {
            postsynapticPartnerIds.add(postToBody.get(psd));
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

    public int getPre() {
        return this.pre;
    }

    public int getPost() {
        return this.post;
    }

    public List<Synapse> getSynapseSet() {
        return this.synapseSet;
    }


}
