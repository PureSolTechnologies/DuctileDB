package com.puresoltechnologies.ductiledb.tinkerpop.test;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraphProvider;

@RunWith(StructureTestSuite.class)
@GraphProviderClass(provider = DuctileGraphProvider.class, graph = DuctileGraph.class)
public class DuctileStructureStandardIT {
}
