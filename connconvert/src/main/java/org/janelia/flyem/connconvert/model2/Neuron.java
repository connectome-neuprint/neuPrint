package org.janelia.flyem.connconvert.model2;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;

import java.io.Reader;
import java.lang.reflect.Type;
import java.util.List;

import org.janelia.flyem.connconvert.json.JsonUtils;

/**
 * Neuron data.
 */
public class Neuron {

    @SerializedName("Id")
    private final Long id;

    @SerializedName("Status")
    private final String status;

    @SerializedName("Name")
    private final String name;

    @SerializedName("Type")
    private final String neuronType; // TODO: ask Nicole why this is missing from fib25 data

    @SerializedName("Size")
    private final Integer size;

    public Neuron(final Long id,
                  final String status,
                  final String name,
                  final String neuronType,
                  final Integer size) {
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

    public Integer getSize() {
        return size;
    }

    public String getStatus() {
        return status;
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
        return "{ neuron: " + id + "} ";
    }

    public static List<Neuron> fromJsonArray(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, NEURON_LIST_TYPE);
    }

    public static List<Neuron> fromJsonArray(final Reader jsonReader) {
        return JsonUtils.GSON.fromJson(jsonReader, NEURON_LIST_TYPE);
    }

    private static Type NEURON_LIST_TYPE = new TypeToken<List<Neuron>>(){}.getType();

}
