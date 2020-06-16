package org.janelia.flyem.neuprint.json;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import org.janelia.flyem.neuprint.model.Location;
import org.janelia.flyem.neuprint.model.Neuron;
import org.janelia.flyem.neuprint.model.Soma;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

/**
 * Gson adapter for json files describing neurons/segments.
 */
public class NeuronAdapter implements JsonDeserializer<Neuron>, JsonSerializer<Neuron> {

    private static final String ID_KEY = "id";
    private static final String STATUS_KEY = "status";
    private static final String NAME_KEY = "name";
    private static final String TYPE_KEY = "type";
    private static final String INSTANCE_KEY = "instance";
    private static final String PRIMARY_NEURITE_KEY = "primaryNeurite";
    private static final String MAJOR_INPUT_KEY = "majorInput";
    private static final String MAJOR_OUTPUT_KEY = "majorOutput";
    private static final String CLONAL_UNIT_KEY = "clonalUnit";
    private static final String NEUROTRANSMITTER_KEY = "neurotransmitter";
    private static final String PROPERTY_KEY = "property";
    private static final String SIZE_KEY = "size";
    private static final String ROIS_KEY = "rois";
    private static final String SOMA_KEY = "soma";
    private static final String SYNAPSE_SET_KEY = "synapseSet";

    @Override
    public Neuron deserialize(final JsonElement jsonElement,
                              final Type typeOfT,
                              final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {

        final JsonObject jsonObject = jsonElement.getAsJsonObject();

        if (!jsonObject.has(ID_KEY)) {
            throw new JsonParseException("Neuron must have 'id' property.");
        }

        double d;
        long l;
        Long id;
        try {
            d = jsonObject.get(ID_KEY).getAsDouble();
            l = jsonObject.get(ID_KEY).getAsLong();
        } catch (NumberFormatException nfe) {
            throw new JsonParseException("Neuron ID must be a number.");
        }
        if (d == l) {
            id = l;
        } else {
            throw new JsonParseException("Neuron ID must be integer value.");
        }

        String status = null;
        if (jsonObject.has(STATUS_KEY)) {
            status = jsonObject.get(STATUS_KEY).getAsString();
        }

        String name = null;
        if (jsonObject.has(NAME_KEY)) {
            name = jsonObject.get(NAME_KEY).getAsString();
        }

        String type = null;
        if (jsonObject.has(TYPE_KEY)) {
            type = jsonObject.get(TYPE_KEY).getAsString();
        }

        String instance = null;
        if (jsonObject.has(INSTANCE_KEY)) {
            instance = jsonObject.get(INSTANCE_KEY).getAsString();
        }

        String primaryNeurite = null;
        if (jsonObject.has(PRIMARY_NEURITE_KEY)) {
            primaryNeurite = jsonObject.get(PRIMARY_NEURITE_KEY).getAsString();
        }

        String majorInput = null;
        if (jsonObject.has(MAJOR_INPUT_KEY)) {
            majorInput = jsonObject.get(MAJOR_INPUT_KEY).getAsString();
        }

        String majorOutput = null;
        if (jsonObject.has(MAJOR_OUTPUT_KEY)) {
            majorOutput = jsonObject.get(MAJOR_OUTPUT_KEY).getAsString();
        }

        String clonalUnit = null;
        if (jsonObject.has(CLONAL_UNIT_KEY)) {
            clonalUnit = jsonObject.get(CLONAL_UNIT_KEY).getAsString();
        }

        String neurotransmitter = null;
        if (jsonObject.has(NEUROTRANSMITTER_KEY)) {
            neurotransmitter = jsonObject.get(NEUROTRANSMITTER_KEY).getAsString();
        }

        String property = null;
        if (jsonObject.has(PROPERTY_KEY)) {
            property = jsonObject.get(PROPERTY_KEY).getAsString();
        }

        Long size = null;
        if (jsonObject.has(SIZE_KEY)) {
            size = jsonObject.get(SIZE_KEY).getAsLong();
        }

        final Set<String> rois = SynapseAdapter.parseRoiJsonArray(jsonObject, ROIS_KEY);

        Soma soma = null;
        if (jsonObject.has(SOMA_KEY)) {
            soma = jsonDeserializationContext.deserialize(jsonObject.get(SOMA_KEY), Soma.class);
        }

        final Set<Location> synapseLocationSet = new HashSet<>();
        if (jsonObject.has(SYNAPSE_SET_KEY)) {
            final JsonArray jsonLocationArray = jsonObject.get(SYNAPSE_SET_KEY).getAsJsonArray();
            for (JsonElement element : jsonLocationArray) {
                synapseLocationSet.add(jsonDeserializationContext.deserialize(element, Location.class));
            }
        }

        return new Neuron(id, status, name, type, instance,
                          primaryNeurite, majorInput, majorOutput, clonalUnit, neurotransmitter, property,
                          size, rois, soma, synapseLocationSet);

    }

    @Override
    public JsonElement serialize(final Neuron srcNeuron,
                                 final Type typeOfSrc,
                                 final JsonSerializationContext jsonSerializationContext) {

        final JsonObject jsonObject = new JsonObject();

        jsonObject.addProperty(ID_KEY, srcNeuron.getId());
        jsonObject.addProperty(STATUS_KEY, srcNeuron.getStatus());
        jsonObject.addProperty(NAME_KEY, srcNeuron.getName());
        jsonObject.addProperty(TYPE_KEY, srcNeuron.getType());
        jsonObject.addProperty(INSTANCE_KEY, srcNeuron.getInstance());
        jsonObject.addProperty(PRIMARY_NEURITE_KEY, srcNeuron.getPrimaryNeurite());
        jsonObject.addProperty(MAJOR_INPUT_KEY, srcNeuron.getMajorInput());
        jsonObject.addProperty(MAJOR_OUTPUT_KEY, srcNeuron.getMajorOutput());
        jsonObject.addProperty(CLONAL_UNIT_KEY, srcNeuron.getClonalUnit());
        jsonObject.addProperty(NEUROTRANSMITTER_KEY, srcNeuron.getNeurotransmitter());
        jsonObject.addProperty(PROPERTY_KEY, srcNeuron.getProperty());
        jsonObject.addProperty(SIZE_KEY, srcNeuron.getSize());

        final JsonArray roiArray = new JsonArray();
        for (String roi : srcNeuron.getRois()) {
            roiArray.add(roi);
        }
        jsonObject.add(ROIS_KEY, roiArray);

        jsonObject.add(SOMA_KEY, jsonSerializationContext.serialize(srcNeuron.getSoma()));

        final JsonArray locationArray = new JsonArray();
        for (Location location : srcNeuron.getSynapseLocationSet()) {
            locationArray.add(jsonSerializationContext.serialize(location));
        }
        jsonObject.add(SYNAPSE_SET_KEY, locationArray);

        return jsonObject;

    }

}
