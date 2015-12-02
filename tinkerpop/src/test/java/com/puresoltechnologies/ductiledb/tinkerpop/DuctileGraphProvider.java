package com.puresoltechnologies.ductiledb.tinkerpop;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

public class DuctileGraphProvider extends AbstractGraphProvider {

    @Override
    public void clear(Graph arg0, Configuration arg1) throws Exception {
	// TODO Auto-generated method stub

    }

    /**
     * <ol>
     * *
     * <li>{@link Edge}</li>
     * <li>{@link Element}</li>
     * <li>{@link Graph}</li>
     * <li>{@link org.apache.tinkerpop.gremlin.structure.Graph.Variables}</li>
     * <li>{@link Property}</li>
     * <li>{@link Vertex}</li>
     * <li>{@link VertexProperty}</li>
     * </ol>
     * 
     */
    @SuppressWarnings("rawtypes")
    @Override
    public Set<Class> getImplementations() {
	HashSet<Class> implementations = new HashSet<>();
	implementations.add(DuctileEdge.class);
	implementations.add(DuctileElement.class);
	implementations.add(DuctileGraph.class);
	implementations.add(DuctileGraphVariables.class);
	implementations.add(DuctileProperty.class);
	implementations.add(DuctileVertex.class);
	implementations.add(DuctileVertexProperty.class);
	return implementations;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String arg0, Class<?> arg1, String arg2, GraphData arg3) {
	// TODO Auto-generated method stub
	return null;
    }

}
