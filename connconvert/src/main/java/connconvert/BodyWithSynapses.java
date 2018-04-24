package connconvert;

import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;

public class BodyWithSynapses {
    public Integer bodyId;
    public List<Synapse> synapseSet;
    public HashMap<Integer,Integer> connectsTo = new HashMap<>(); //Map of body IDs and weights
    public HashMap<Integer,Integer> connectsFrom = new HashMap<>(); //Map of body IDs and weights
    public Integer pre; //number of presyn terminals
    public Integer post; //number of postsyn terminals
    // body divided into multiple neuron parts based on roi
    public List<NeuronPart> neuronParts;

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

    public int getPre() {
        return this.pre;
    }

    public int getPost() {
        return this.post;
    }

    public List<Synapse> getSynapseSet() {
        return this.synapseSet;
    }

    public void setNeuronParts() {
        neuronParts = new ArrayList<>();
        HashMap<String,SynapseCounter> roiToPrePostCount = new HashMap<>();
        for (Synapse synapse : this.synapseSet) {

            // set distal and proximal rois for fib25 dataset
//            if (dataset.equals("fib25")) {
//                String proxdistRoi;
//                if (synapse.getLocation().get(2)>=4600) {
//                    proxdistRoi = "prox_medulla";
//                } else { proxdistRoi = "dist_medulla"; }
//
//                if (synapse.getNeuronType().equals("pre")) {
//                    if (roiToPrePostCount.containsKey(proxdistRoi)) {
//                            roiToPrePostCount.get(proxdistRoi).incrementPreCount();
//                        } else {
//                            roiToPrePostCount.put(proxdistRoi, new SynapseCounter());
//                            roiToPrePostCount.get(proxdistRoi).incrementPreCount();
//                        }
//                    } else if (synapse.getNeuronType().equals("post")) {
//                        if (roiToPrePostCount.containsKey(proxdistRoi)) {
//                            roiToPrePostCount.get(proxdistRoi).incrementPostCount();
//                        } else {
//                            roiToPrePostCount.put(proxdistRoi,new SynapseCounter());
//                            roiToPrePostCount.get(proxdistRoi).incrementPostCount();
//                        }
//
//                        }
//            }


            //add rest of rois.
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



    public List<NeuronPart> getNeuronParts() {
        return this.neuronParts;
    }




}
