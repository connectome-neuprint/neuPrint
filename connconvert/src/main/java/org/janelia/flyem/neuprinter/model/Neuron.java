package org.janelia.flyem.neuprinter.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.janelia.flyem.neuprinter.json.JsonUtils;
import org.neo4j.driver.v1.types.Point;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Neuron {

    //TODO: figure out how to add optional properties

    @SerializedName("Id")
    private final Long id;

    @SerializedName("Status")
    private final String status;

    @SerializedName("Size")
    private final Long size;

    @SerializedName("Name")
    private final String name;

    @SerializedName("NeuronType")
    private final String neuronType;

    @SerializedName("rois")
    private final List<String> rois;

    @SerializedName("Soma")
    private final Soma soma;

    public Neuron(final Long id,
                  final String status,
                  final String name,
                  final String neuronType,
                  final Long size,
                  final List<String> rois,
                  final Soma soma) {
        this.id = id;
        this.status = status;
        this.name = name;
        this.neuronType = neuronType;
        this.size = size;
        this.rois = rois;
        this.soma = soma;
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

    public List<String> getRois() {
        // remove -lm tag on rois
        if (this.rois!=null) {
            List<String> newRoiList = new ArrayList<>();
            for (String roi : rois) {
                if (roi.endsWith("-lm")) {
                    newRoiList.add(roi.replace("-lm", ""));
                } else {
                    newRoiList.add(roi);
                }
            }
            return newRoiList;
        } else {
            return rois;
        }
    }

    public List<String> getRoisWithAndWithoutDatasetPrefix(String dataset) {
        List<String> rois = getRois();
        if (rois!=null) {
            rois.addAll(rois.stream().map(r -> dataset + "-" + r).collect(Collectors.toList()));
            return rois;
        } else {
            return rois;
        }
    }

    public Soma getSoma() {
        return soma;
    }

    public Point getSomaLocation() {
        if (soma != null) {
            return soma.getLocationAsPoint();
        } else {
            return null;
        }
    }

    public Float getSomaRadius() {
        if (soma != null) {
            return soma.getRadius();
        } else {
            return null;
        }
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
        int result = 17;
        result = 31 * result + id.hashCode();
        return result;
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

    public static List<Neuron> fromJson(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, NEURON_LIST_TYPE);
    }

    public static List<Neuron> fromJson(final BufferedReader reader) {
        return JsonUtils.GSON.fromJson(reader,NEURON_LIST_TYPE);
    }

    public static Neuron fromJsonSingleObject(final JsonReader reader) {
        return JsonUtils.GSON.fromJson(reader,Neuron.class);
    }

    private static Type NEURON_LIST_TYPE = new TypeToken<List<Neuron>>(){}.getType();


}
