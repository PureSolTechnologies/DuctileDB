package com.puresoltechnologies.xo.titan.test.implementedby;

import com.buschmais.xo.api.proxy.ProxyMethod;
import com.tinkerpop.blueprints.Vertex;

public class EntityIncrementValueMethod implements ProxyMethod<Vertex> {

	@Override
	public Object invoke(Vertex entity, Object instance, Object[] args) {
		A a = A.class.cast(instance);
		int value = a.getValue();
		value++;
		a.setValue(value);
		return value;
	}

}
