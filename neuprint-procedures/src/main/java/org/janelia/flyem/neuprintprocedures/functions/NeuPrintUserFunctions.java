package org.janelia.flyem.neuprintprocedures.functions;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.janelia.flyem.neuprinter.model.SynapseCounter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

import java.util.Map;

public class NeuPrintUserFunctions {

    @Context
    public GraphDatabaseService dbService;

    @Context
    public Log log;

    @UserFunction("neuprint.locationAs3dCartPoint")
    @Description("neuprint.locationAs3dCartPoint(x,y,z) : returns a 3D Cartesian org.neo4j.graphdb.spatial.Point type with the provided coordinates. ")
    public Point locationAs3dCartPoint(@Name("x") Double x, @Name("y") Double y, @Name("z") Double z) {
        if (x == null || y == null || z == null) {
            throw new Error("Must provide x, y, and z coordinate.");
        }
        Point point = null;
        try {
            Map<String, Object> pointQueryResult = dbService.execute("RETURN point({ x:" + x + ", y:" + y + ", z:" + z + ", crs:'cartesian-3D'}) AS point").next();
            point = (Point) pointQueryResult.get("point");
        } catch (Exception e) {
            log.error("Error using neuprint.locationAs3dCartPoint(x,y,z): ");
            e.printStackTrace();
        }
        return point;
    }

    @UserFunction("neuprint.roiInfoAsName")
    @Description("neuprint.roiInfoAsName(roiInfo, totalPre, totalPost, threshold) ")
    public String roiInfoAsName(@Name("roiInfo") String roiInfo, @Name("totalPre") Long totalPre, @Name("totalPost") Long totalPost, @Name("threshold") Double threshold) {
        if (roiInfo == null || totalPre == null || totalPost == null || threshold == null) {
            throw new Error("Must provide roiInfo, totalPre, totalPost, and threshold.");
        }

        Gson gson = new Gson();
        Map<String, SynapseCounter> roiInfoMap = gson.fromJson(roiInfo, new TypeToken<Map<String, SynapseCounter>>() {
        }.getType());

        StringBuilder inputs = new StringBuilder();
        StringBuilder outputs = new StringBuilder();
        for (String roi : roiInfoMap.keySet()) {
            if ((roiInfoMap.get(roi).getPre() * 1.0) / totalPre > threshold) {
                outputs.append(roi).append(".");
            }
            if ((roiInfoMap.get(roi).getPost() * 1.0) / totalPost > threshold) {
                inputs.append(roi).append(".");
            }
        }
        if (outputs.length() > 0) {
            outputs.deleteCharAt(outputs.length()-1);
        } else {
            outputs.append("none");
        }
        if (inputs.length() > 0) {
            inputs.deleteCharAt(inputs.length()-1);
        } else {
            inputs.append("none");
        }

        return inputs + "-" + outputs;

    }
}
