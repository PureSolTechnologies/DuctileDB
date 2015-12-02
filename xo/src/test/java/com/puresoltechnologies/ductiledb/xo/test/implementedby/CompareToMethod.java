package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import com.buschmais.xo.api.proxy.ProxyMethod;
import com.tinkerpop.blueprints.Vertex;

public class CompareToMethod implements ProxyMethod<DuctileDBVertex> {

	@Override
	public Object invoke(DuctileDBVertex node, Object instance, Object[] args) {
		return ((A) instance).getValue() - ((A) args[0]).getValue();
	}

}
