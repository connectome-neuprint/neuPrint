package org.janelia.flyem.neuprintprocedures.functions;

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
}
