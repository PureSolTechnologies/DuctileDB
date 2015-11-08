package com.puresoltechnologies.xo.titan.test.mapping;

import java.util.List;

import com.buschmais.xo.api.Query.Result;
import com.buschmais.xo.api.annotation.ResultOf;
import com.buschmais.xo.api.annotation.ResultOf.Parameter;
import com.puresoltechnologies.ductiledb.xo.api.annotation.EdgeDefinition;
import com.puresoltechnologies.ductiledb.xo.api.annotation.Gauging;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.puresoltechnologies.ductiledb.xo.impl.DuctileDBStoreSession;

@VertexDefinition("E")
public interface E {

	@EdgeDefinition("RELATED_TO")
	List<F> getRelatedTo();

	@ResultOf(query = ByValue.class, usingThisAs = "e")
	Result<ByValue> getResultByValueUsingExplicitQuery(
			@Parameter("value") String value);

	@ResultOf(usingThisAs = "e")
	Result<ByValue> getResultByValueUsingReturnType(
			@Parameter("value") String value);

	@ResultOf(query = ByValue.class, usingThisAs = "e")
	ByValue getByValueUsingExplicitQuery(@Parameter("value") String value);

	@ResultOf(usingThisAs = "e")
	ByValue getByValueUsingReturnType(@Parameter("value") String value);

	@ResultOf
	ByValueUsingImplicitThis getByValueUsingImplicitThis(
			@Parameter("value") String value);

	@ResultOf
	@Gauging("_().has('" + DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY
			+ "E').outE.has('label', 'RELATED_TO').inV.has('"
			+ DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY
			+ "F').has('value', {value})")
	Result<F> getResultUsingGremlin(@Parameter("value") String value);

	@ResultOf
	@Gauging("_().has('" + DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY
			+ "E').outE.has('label', 'RELATED_TO').inV.has('"
			+ DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY
			+ "F').has('value', {value})")
	F getSingleResultUsingGremlin(@Parameter("value") String value);

	List<E2F> getE2F();

	// where where e={e}
	@Gauging(value = "_().has('" + DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY
			+ "E').outE.has('label', 'RELATED_TO').inV.has('"
			+ DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY
			+ "F').has('value', {value})", name = "f")
	interface ByValue {
		F getF();
	}

	// where e={this}
	@Gauging(value = "_().has('" + DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY
			+ "E').outE.has('label', 'RELATED_TO').inV.has('"
			+ DuctileDBStoreSession.XO_DISCRIMINATORS_PROPERTY
			+ "F').has('value', {value})", name = "f")
	interface ByValueUsingImplicitThis {
		F getF();
	}
}
