package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import com.buschmais.xo.api.proxy.ProxyMethod;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileEdge;

public class RelationIncrementValueMethod implements ProxyMethod<DuctileEdge> {

    @Override
    public Object invoke(DuctileEdge entity, Object instance, Object[] args) {
	A2B a2b = A2B.class.cast(instance);
	int value = a2b.getValue();
	value++;
	a2b.setValue(value);
	return value;
    }

}
