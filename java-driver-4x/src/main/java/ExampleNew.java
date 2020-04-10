import com.datastax.dse.driver.api.core.graph.*;
import com.datastax.oss.driver.api.core.CqlSession;

import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.net.InetSocketAddress;

public class ExampleNew {

    private static void createSchema(CqlSession session, String graphName){

        String createGraphStmt = String.format("system.graph(\"%s\").ifNotExists().create()", graphName);

        String createContinentVertexLabelStmt = "schema.vertexLabel(\"continent\").ifNotExists().partitionBy(\"name\", String).create()";

        String createCountryVertexLabelStmt = "schema.vertexLabel(\"country\").ifNotExists().partitionBy(\"name\", String).property(\"capital\", String).create()";

        String createHasCountryEdgeLabelStmt = "schema.edgeLabel(\"has_country\").ifNotExists().from(\"continent\").to(\"country\").create()";

        executeIfNotExists(session, createGraphStmt, true);
        executeIfNotExists(session, createContinentVertexLabelStmt, false);
        executeIfNotExists(session, createCountryVertexLabelStmt, false);
        executeIfNotExists(session, createHasCountryEdgeLabelStmt, false);

    }

    private static void executeIfNotExists(CqlSession session, String createStmt, boolean is_system){

        ScriptGraphStatement createScriptStmt = ScriptGraphStatement.newInstance(createStmt).setSystemQuery(is_system);
        session.execute(createScriptStmt);

    }

    private static void addContinentVertex(CqlSession session, GraphTraversalSource g, String continentName){
        GraphTraversal t = g.addV("continent").property("name", continentName);
        GraphResultSet r = session.execute(FluentGraphStatement.newInstance(t));
        System.out.println(r.one());
    }

    private static void addCountryVertex(CqlSession session, GraphTraversalSource g, String countryName, String capitalName){
        GraphTraversal t = g.addV("country").property("name", countryName).property("capital", capitalName);
        session.execute(FluentGraphStatement.newInstance(t));
    }

    private static void addHasCountryEdge(CqlSession session, GraphTraversalSource g, String continentName, String countryName){
        GraphTraversal t = g.addE("has_country").from(g.V().has("continent", "name", continentName)).to(g.V().has("country", "name", countryName));
        session.execute(FluentGraphStatement.newInstance(t));
    }

    public static void main(String[] args){
        String graphName = "world_graph";

        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("Graph")
                .build();

        GraphTraversalSource g = AnonymousTraversalSource.traversal().withRemote(DseGraph.remoteConnectionBuilder(session).build());
        createSchema(session, graphName);

        addContinentVertex(session, g, "Europe");
        addContinentVertex(session, g, "Asia");
        addContinentVertex(session, g, "North America");

        addCountryVertex(session, g, "United Kingdom", "London");
        addCountryVertex(session, g, "Germany", "Berlin");
        addCountryVertex(session, g, "Spain", "Madrid");
        addCountryVertex(session, g, "China", "Beijing");
        addCountryVertex(session, g, "United States", "Washington DC");

        addHasCountryEdge(session, g, "Europe", "United Kingdom");
        addHasCountryEdge(session, g, "Europe", "Germany");
        addHasCountryEdge(session, g, "Europe", "Spain");
        addHasCountryEdge(session, g, "Asia", "China");
        addHasCountryEdge(session, g, "North America", "United States");


        GraphTraversal t = g.V().has("continent", "name", "Europe").outE("has_country").inV();
        GraphResultSet rs = session.execute(FluentGraphStatement.newInstance(t));

        for (GraphNode node : rs) {
            System.out.print(node.asString());
        }

        session.close();
    }
}