package org.janelia.flyem.neuprintprocedures.analysis;

import com.google.gson.Gson;

import java.util.Arrays;
import java.util.Set;

public class ClusteringFeatureVector {

    private Long bodyId;
    private long[] inputFeatureVector;
    private long[] outputFeatureVector;
    private Set<Long> inputIds;
    private Set<Long> outputIds;
    private boolean isPrimaryNeuron = false;

    private final static Gson gson = new Gson();

    ClusteringFeatureVector(Long bodyId, long[] inputFeatureVector, long[] outputFeatureVector) {
        this.bodyId = bodyId;
        this.inputFeatureVector = inputFeatureVector;
        this.outputFeatureVector = outputFeatureVector;
    }

    public void setInputAndOutputIds(Set<Long> inputIds, Set<Long> outputIds) {
        this.inputIds = inputIds;
        this.outputIds = outputIds;
        this.isPrimaryNeuron = true;
    }

    public static String getClusteringFeatureVectorSetJson(Set<ClusteringFeatureVector> clusteringFeatureVectors) {
            return gson.toJson(clusteringFeatureVectors);
    }

    public Long getBodyId() {
        return this.bodyId;
    }


    public long[] getInputFeatureVector() {
        return this.inputFeatureVector;
    }

    public long[] getOutputFeatureVector() {
        return this.outputFeatureVector;
    }

    @Override
    public String toString() {
        return gson.toJson(this);
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof ClusteringFeatureVector) {
            final ClusteringFeatureVector that = (ClusteringFeatureVector) o;
            isEqual = this.bodyId.equals(that.bodyId);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.bodyId.hashCode();
        return result;
    }
}
