package org.janelia.flyem.neuprintprocedures.proofreading;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.janelia.flyem.neuprinter.model.Soma;
import org.janelia.flyem.neuprinter.model.Synapse;

import java.util.Set;

public class NeuronUpdate {

    @SerializedName("Id")
    private long bodyId;

    @SerializedName("Size")
    private long size;

    @SerializedName("MutationUUID")
    private String mutationUuid;

    @SerializedName("MutationID")
    private Long mutationId;

    @SerializedName("Status")
    private String status; // optional

    @SerializedName("Name")
    private String name; // optional

    @SerializedName("Soma")
    private Soma soma; // optional

    @SerializedName("SynapseSources")
    private Set<Long> synapseSources;

    @SerializedName("CurrentSynapses")
    private Set<Synapse> currentSynapses;

    public long getBodyId() {
        return bodyId;
    }

    public long getSize() {
        return size;
    }

    public String getMutationUuid() {
        return mutationUuid;
    }

    public long getMutationId() {
        return mutationId;
    }

    public String getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }

    public Soma getSoma() {
        return soma;
    }

    public Set<Long> getSynapseSources() {
        return synapseSources;
    }

    public Set<Synapse> getCurrentSynapses() {
        return currentSynapses;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this, NeuronUpdate.class);
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof NeuronUpdate) {
            final NeuronUpdate that = (NeuronUpdate) o;
            isEqual = this.mutationUuid.equals(that.mutationUuid)
                    && this.mutationId.equals(that.mutationId);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + this.mutationUuid.hashCode();
        result = 31 * result + this.mutationId.hashCode();
        return result;
    }
}
