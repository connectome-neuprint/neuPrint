package org.janelia.flyem.neuprinter.model;

import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import org.janelia.flyem.neuprinter.json.JsonUtils;
import org.neo4j.driver.v1.types.Point;

import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class representing a neuron, as read from a
 * <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">neuron JSON file</a>.
 * Each neuron has an id, size, name, and status,
 * but no synapse information. (cf. the {@link org.janelia.flyem.neuprinter.model.BodyWithSynapses} class
 * that represents bodies with synapses)
 */
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

    /**
     * Class constructor.
     *
     * @param id         bodyId
     * @param status     status
     * @param name       name
     * @param neuronType neuron type
     * @param size       size (in voxels)
     * @param rois       rois associated with this neuron
     * @param soma       soma for this neuron
     */
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

    /**
     * @return bodyId
     */
    public Long getId() {
        return id;
    }

    /**
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * @return type of neuron
     */
    public String getNeuronType() {
        return neuronType;
    }

    /**
     * @return status
     */
    public String getStatus() {
        return status;
    }

    /**
     * @return voxel size
     */
    public Long getSize() {
        return size;
    }

    /**
     * @return rois (without "-lm" suffix if present)
     */
    public List<String> getRois() {
        return removeUnwantedRois(this.rois);
    }

    static List<String> removeUnwantedRois(List<String> rois) {
        List<String> newRoiList;
        if (rois != null) {
            newRoiList = rois.stream()
                    .filter(r -> !(r.equals("seven_column_roi") || r.equals("kc_alpha_roi")))
                    .collect(Collectors.toList());
            return newRoiList;
        } else {
            return null;
        }
    }

    /**
     * Returns a list of rois in which this neuron is located with and without
     * a "dataset-" prefix.
     *
     * @param dataset the dataset in which this neuron exists
     * @return set of rois and rois prefixed with "dataset-"
     */
    public List<String> getRoisWithAndWithoutDatasetPrefix(String dataset) {
        List<String> rois = getRois();
        if (rois != null) {
            rois.addAll(rois.stream().map(r -> dataset + "-" + r).collect(Collectors.toSet()));
            return rois;
        } else {
            return null;
        }
    }

    /**
     * @return the {@link Soma} for this neuron
     */
    public Soma getSoma() {
        return soma;
    }

    /**
     * @return the {@link Soma} location as a neo4j {@link Point}
     */
    public Point getSomaLocation() {
        if (soma != null) {
            return soma.getLocationAsPoint();
        } else {
            return null;
        }
    }

    /**
     * @return radius of {@link Soma}
     */
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

    /**
     * Returns a list of Neurons deserialized from a neuron JSON string.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">neuron JSON format</a>.
     *
     * @param jsonString string containing neuron JSON
     * @return list of Neurons
     */
    public static List<Neuron> fromJson(final String jsonString) {
        return JsonUtils.GSON.fromJson(jsonString, NEURON_LIST_TYPE);
    }

    /**
     * Returns a list of Neurons deserialized from a {@link BufferedReader} reading from a neuron JSON file.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">neuron JSON format</a>.
     *
     * @param reader {@link BufferedReader}
     * @return list of Neurons
     */
    public static List<Neuron> fromJson(final BufferedReader reader) {
        return JsonUtils.GSON.fromJson(reader, NEURON_LIST_TYPE);
    }

    /**
     * Returns a Neuron deserialized from a single JSON object from a neuron JSON file.
     * See <a href="http://github.com/janelia-flyem/neuPrint/blob/master/jsonspecs.md" target="_blank">neuron JSON format</a>.
     *
     * @param reader {@link JsonReader}
     * @return Neuron
     */
    public static Neuron fromJsonSingleObject(final JsonReader reader) {
        return JsonUtils.GSON.fromJson(reader, Neuron.class);
    }

    private static Type NEURON_LIST_TYPE = new TypeToken<List<Neuron>>() {
    }.getType();

}
