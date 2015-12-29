package com.puresoltechnologies.ductiledb.tinkerpop;

import java.io.IOException;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.puresoltechnologies.ductiledb.api.DuctileDBGraph;
import com.puresoltechnologies.ductiledb.core.DuctileDBGraphFactory;

public class DuctileTinkerpopGraphFactory {

    private static final Logger logger = LoggerFactory.getLogger(DuctileTinkerpopGraphFactory.class);

    public static DuctileGraph createGraph(Configuration hbaseConfiguration) throws IOException {
	BaseConfiguration configuration = new BaseConfiguration();
	DuctileDBGraph graph = DuctileDBGraphFactory.createGraph(configuration);
	return new DuctileGraph(graph, configuration);
    }

}
