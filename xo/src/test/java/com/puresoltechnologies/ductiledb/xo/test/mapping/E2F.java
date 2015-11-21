package com.puresoltechnologies.ductiledb.xo.test.mapping;

import com.buschmais.xo.api.Query.Result;
import com.buschmais.xo.api.annotation.ResultOf;
import com.buschmais.xo.api.annotation.ResultOf.Parameter;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Incoming;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition.Outgoing;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Gauging;

@EdgeDefinition("E2F")
public interface E2F {

    @Outgoing
    E getE();

    @Incoming
    F getF();

    @ResultOf(query = ByValue.class, usingThisAs = "e2f")
    Result<ByValue> getResultByValueUsingExplicitQuery(@Parameter("value") String value);

    @ResultOf(usingThisAs = "e2f")
    Result<ByValue> getResultByValueUsingReturnType(@Parameter("value") String value);

    @ResultOf(query = ByValue.class, usingThisAs = "e2f")
    ByValue getByValueUsingExplicitQuery(@Parameter("value") String value);

    @ResultOf(usingThisAs = "e2f")
    ByValue getByValueUsingReturnType(@Parameter("value") String value);

    @ResultOf
    ByValueUsingImplicitThis getByValueUsingImplicitThis(@Parameter("value") String value);

    // TODO Cypher query
    @ResultOf
    @Gauging("match ()-[e2f:E2F]->(f:F) where e2f.value={value} return f")
    Result<F> getResultUsingGremlin(@Parameter("value") String value);

    // TODO Cypher query
    @ResultOf
    @Gauging("match ()-[e2f:E2F]->(f:F) where e2f.value={value} return f")
    F getSingleResultUsingGremlin(@Parameter("value") String value);

    void setValue(String value);

    String getValue();

    // TODO Cypher query
    @Gauging("match ()-[e2f:E2F]->(f:F) where e2f={e2f} and e2f.value={value} return f")
    public interface ByValue {
	F getF();
    }

    // TODO Cypher query
    @Gauging("match ()-[e2f:E2F]->(f:F) where e2f={this} ande2f.value={value} return f")
    public interface ByValueUsingImplicitThis {
	F getF();
    }
}
