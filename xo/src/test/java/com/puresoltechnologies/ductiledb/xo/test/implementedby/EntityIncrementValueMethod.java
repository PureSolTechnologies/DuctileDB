package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import com.buschmais.xo.api.proxy.ProxyMethod;
import com.tinkerpop.blueprints.Vertex;

public class EntityIncrementValueMethod implements ProxyMethod<DuctileDBVertex> {

	@Override
	public Object invoke(DuctileDBVertex entity, Object instance, Object[] args) {
		A a = A.class.cast(instance);
		int value = a.getValue();
		value++;
		a.setValue(value);
		return value;
	}

}
