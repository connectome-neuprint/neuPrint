package org.janelia.flyem.neuprintprocedures;

import com.google.gson.Gson;

import java.util.Set;

public class ClusteringFeatureVector {

    private Long bodyId;
    private long[] inputFeatureVector;
    private long[] outputFeatureVector;

    public ClusteringFeatureVector(Long bodyId, long[] inputFeatureVector, long[] outputFeatureVector) {
        this.bodyId = bodyId;
        this.inputFeatureVector = inputFeatureVector;
        this.outputFeatureVector = outputFeatureVector;
    }

    public static String getClusteringFeatureVectorSetJson(Set<ClusteringFeatureVector> clusteringFeatureVectors) {
        final Gson gson = new Gson();
        String json = gson.toJson(clusteringFeatureVectors);
        return json;
    }

    public Long getBodyId() {
        return bodyId;
    }

    public long[] getInputFeatureVector() {
        return inputFeatureVector;
    }

    public long[] getOutputFeatureVector() {
        return outputFeatureVector;
    }

    @Override
    public String toString() {
        return this.bodyId + ":" + this.inputFeatureVector + " ; " + this.outputFeatureVector;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof ClusteringFeatureVector) {
            final ClusteringFeatureVector that = (ClusteringFeatureVector) o;
            isEqual = this.bodyId.equals(that.bodyId) ;
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
