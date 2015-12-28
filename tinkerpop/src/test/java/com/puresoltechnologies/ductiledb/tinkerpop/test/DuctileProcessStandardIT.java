package com.puresoltechnologies.ductiledb.tinkerpop.test;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.Ignore;
import org.junit.runner.RunWith;

import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraph;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileGraphProvider;

@Ignore
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = DuctileGraphProvider.class, graph = DuctileGraph.class)
public class DuctileProcessStandardIT {
}
