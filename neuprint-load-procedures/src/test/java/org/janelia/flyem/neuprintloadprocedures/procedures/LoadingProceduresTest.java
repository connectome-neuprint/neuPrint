package org.janelia.flyem.neuprintloadprocedures.procedures;

import apoc.convert.Json;
import apoc.create.Create;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;

public class LoadingProceduresTest {
    @Rule
    public Neo4jRule neo4j = new Neo4jRule()
            .withProcedure(LoadingProcedures.class)
            .withFunction(Json.class)
            .withProcedure(Create.class);

    @Test
    public void shouldAddRoiInfoToConnectionSetAndWeightHPToConnectsTo() {

    }
}
