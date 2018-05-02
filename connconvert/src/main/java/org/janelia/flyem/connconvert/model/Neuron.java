package org.janelia.flyem.connconvert.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.connconvert.json.JsonUtils;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.util.List;

public class Neuron {

    @SerializedName("Id")
    private final Long id;

    @SerializedName("Status")
    private final String status;

    @SerializedName("Name")
    private final String name;

    @SerializedName("Type")
    private final String neuronType; // TODO: so far not in datasets, ask Lowell

    @SerializedName("Size")
    private final Long size;

    public Neuron(final Long id,
                  final String status,
                  final String name,
                  final String neuronType,
                  final Long size) {
        this.id = id;
        this.status = status;
        this.name = name;
        this.neuronType = neuronType;
        this.size = size;
    }


    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getNeuronType() {
        return neuronType;
    }

    public String getStatus() {
        return status;
    }

    public Long getSize() {
        return size;
    }

    @Override
    public boolean equals(Object o) {
        boolean isEqual = false;
        if (this == o) {
            isEqual = true;
        } else if (o instanceof Neuron) {
            final Neuron that = (Neuron) o;
            isEqual = this.id.equals(that.id);
        }
        return isEqual;
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    @Override
    public String toString() {
        return "Neuron { " + "bodyid= " + id +
                ", status= " + status +
                ", name= " + name +
                ", neuronType= " + neuronType +
                ", size= " + size
                + " }";
    }

    public static List<Neuron> fromJson(final BufferedReader reader) {
        return JsonUtils.GSON.fromJson(reader,NEURON_LIST_TYPE);
    }

    private static Type NEURON_LIST_TYPE = new TypeToken<List<Neuron>>(){}.getType();


}
