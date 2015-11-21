package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import com.buschmais.xo.api.proxy.ProxyMethod;
import com.tinkerpop.blueprints.Vertex;

public class CompareToMethod implements ProxyMethod<Vertex> {

	@Override
	public Object invoke(Vertex node, Object instance, Object[] args) {
		return ((A) instance).getValue() - ((A) args[0]).getValue();
	}

}
