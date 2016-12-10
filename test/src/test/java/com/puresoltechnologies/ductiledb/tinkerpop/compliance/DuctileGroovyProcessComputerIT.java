package com.puresoltechnologies.ductiledb.tinkerpop.compliance;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.GroovyProcessComputerSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraphProvider;

@Ignore("Not fully implemented, yet.")
@RunWith(GroovyProcessComputerSuite.class)
@GraphProviderClass(provider = DuctileGraphProvider.class, graph = DuctileGraph.class)
public class DuctileGroovyProcessComputerIT {
}
