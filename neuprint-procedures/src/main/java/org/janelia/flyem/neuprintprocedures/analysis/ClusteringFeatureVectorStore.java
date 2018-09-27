package org.janelia.flyem.neuprintprocedures.analysis;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ClusteringFeatureVectorStore {

    private Map<Long, ClusteringFeatureVector> clusteringFeatureVectorStore = new HashMap<>();

    public void addClusteringFeatureVector(Long bodyId, long[] inputFeatureVector, long[] outputFeatureVector) {
        if (!this.clusteringFeatureVectorStore.containsKey(bodyId)) {
            this.clusteringFeatureVectorStore.put(bodyId, new ClusteringFeatureVector(bodyId, inputFeatureVector, outputFeatureVector));
        }
    }

    public void addInputAndOutputIds(Long bodyId, Set<Long> inputIds, Set<Long> outputIds) throws Exception {
        if (this.clusteringFeatureVectorStore.containsKey(bodyId)) {
            this.clusteringFeatureVectorStore.get(bodyId).setInputAndOutputIds(inputIds,outputIds);
        } else {
            throw new Exception("bodyId not found in clustering feature vector store.");
        }
    }

    public Set<ClusteringFeatureVector> getClusteringFeatureVectorStoreAsSet() {
        return new HashSet<ClusteringFeatureVector>(this.clusteringFeatureVectorStore.values());
    }
}
