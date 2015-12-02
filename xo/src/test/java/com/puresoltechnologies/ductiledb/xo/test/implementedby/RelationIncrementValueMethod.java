package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import com.buschmais.xo.api.proxy.ProxyMethod;
import com.tinkerpop.blueprints.Edge;

public class RelationIncrementValueMethod implements ProxyMethod<DuctileDBEdge> {

	@Override
	public Object invoke(DuctileDBEdge entity, Object instance, Object[] args) {
		A2B a2b = A2B.class.cast(instance);
		int value = a2b.getValue();
		value++;
		a2b.setValue(value);
		return value;
	}

}
