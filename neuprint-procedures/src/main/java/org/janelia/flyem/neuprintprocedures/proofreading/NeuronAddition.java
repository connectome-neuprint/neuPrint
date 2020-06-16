package org.janelia.flyem.neuprintprocedures.proofreading;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import org.janelia.flyem.neuprint.model.Soma;
import org.janelia.flyem.neuprint.model.Synapse;

import java.util.Set;

public class NeuronAddition {

    @SerializedName("id")
    private Long bodyId;

    @SerializedName("size")
    private Long size;

    @SerializedName("mutationUUID")
    private String mutationUuid;

    @SerializedName("mutationID")
    private Long mutationId;

    @SerializedName("status")
    private String status; // optional

    @SerializedName("name")
    private String name; // optional

    @SerializedName("instance")
    private String instance; // optional

    @SerializedName("primaryNeurite")
    private String primaryNeurite; // optional

    @SerializedName("majorInput")
    private String majorInput; // optional

    @SerializedName("majorOutput")
    private String majorOutput; // optional

    @SerializedName("clonalUnit")
    private String clonalUnit; // optional

    @SerializedName("neurotransmitter")
    private String neurotransmitter; // optional

    @SerializedName("property")
    private String property; // optional

    @SerializedName("soma")
    private Soma soma; // optional

    @SerializedName("currentSynapses")
    private Set<Synapse> currentSynapses;

    public Long getBodyId() {
        return bodyId;
    }

    public Long getSize() {
        return size;
    }

    public String getMutationUuid() {
        return mutationUuid;
    }

    public Long getMutationId() {
        return mutationId;
    }

    public String getStatus() {
        return status;
    }

    public String getName() {
        return name;
    }

    public String getInstance() {
        return instance;
    }

    public String getPrimaryNeurite() {
        return primaryNeurite;
    }

    public String getMajorInput() {
        return majorInput;
    }

    public String getMajorOutput() {
        return majorOutput;
    }

    public String getClonalUnit() {
        return clonalUnit;
    }

    public String getNeurotransmitter() {
        return neurotransmitter;
    }

    public String getProperty() {
        return property;
    }

    public Soma getSoma() {
        return soma;
    }

    public Set<Synapse> getCurrentSynapses() {
        return currentSynapses;
    }

    public void setToInitialMutationId() {
        this.mutationId = 0L;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this, NeuronAddition.class);
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof NeuronAddition) {
            final NeuronAddition that = (NeuronAddition) o;
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
