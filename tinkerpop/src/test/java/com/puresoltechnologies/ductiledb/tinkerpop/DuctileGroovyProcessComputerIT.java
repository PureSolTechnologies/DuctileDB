package com.puresoltechnologies.ductiledb.tinkerpop;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.GroovyProcessComputerSuite;
import org.junit.runner.RunWith;

@RunWith(GroovyProcessComputerSuite.class)
@GraphProviderClass(provider = DuctileGraphProvider.class, graph = DuctileGraph.class)
public class DuctileGroovyProcessComputerIT {
}
