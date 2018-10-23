package org.janelia.flyem.neuprintprocedures.proofreading;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

import java.util.Set;
import java.util.stream.Collectors;

public class UpdateNeuronsAction {

    @SerializedName("Deleted Neurons")
    private Set<Long> deletedNeurons;

    @SerializedName("Updated Neurons")
    private Set<NeuronUpdate> updatedNeurons;

    public Set<NeuronUpdate> getUpdatedNeurons() {
        return this.updatedNeurons;
    }

    public Set<Long> getUpdatedNeuronsBodyIds() {
        return this.updatedNeurons
                .stream()
                .map(NeuronUpdate::getBodyId)
                .collect(Collectors.toSet());
    }

    public Set<Long> getDeletedNeurons() {
        return this.deletedNeurons;
    }

    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson(this, UpdateNeuronsAction.class);
    }
}
