package com.puresoltechnologies.ductiledb.tinkerpop;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentSuite.class)
@GraphProviderClass(provider = DuctileGraphProvider.class, graph = DuctileGraph.class)
public class DuctileGroovyEnvironmentIT {
}
