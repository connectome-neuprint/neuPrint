//package org.janelia.flyem.neuprinter.json;
//
//import com.google.gson.*;
//import Synapse;
//
//import java.lang.reflect.Type;
//
//public class SynapseDeserializer implements JsonDeserializer<Synapse> {
//
//    public static final String TYPE_KEY = "Type";
//    public static final String CONFIDENCE_KEY = "Confidence";
//    public static final String LOCATION_KEY = "Location";
//    public static final String CONNECTS_TO_KEY = "ConnectsTo";
//    public static final String CONNECTS_FROM_KEY = "ConnectsFrom";
//    public static final String ROIS_KEY = "rois";
//
//    @Override
//    public Synapse deserialize(final JsonElement jsonElement,
//                               final Type typeOfT,
//                               final JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
//
//        final JsonObject jsonObject = jsonElement.getAsJsonObject();
//
//        final String type = jsonObject.get("type").getAsString();
//        final float confidence = jsonObject.get("confidence").getAsFloat();
//        final JsonArray jsonLocationArray = jsonObject.get("location").getAsJsonArray();
//
//
//
//
//        final Synapse synapse = new Synapse(type,confidence,location,connectsTo,connectsFrom);
//
//        return synapse;
//
//
//    }
//
//
//}
