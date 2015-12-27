package com.puresoltechnologies.ductiledb.tinkerpop;

import javax.script.ScriptException;

import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor.Builder;

public class DuctileQuery {

    public void query(String script) throws ScriptException {
	Builder executor = GremlinExecutor.build();
	GremlinExecutor gremlinExecutor = executor.create();
	gremlinExecutor.compile(script);
    }

}
