package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import com.buschmais.xo.api.proxy.ProxyMethod;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;

public class EntityIncrementValueMethod implements ProxyMethod<DuctileVertex> {

    @Override
    public Object invoke(DuctileVertex entity, Object instance, Object[] args) {
	A a = A.class.cast(instance);
	int value = a.getValue();
	value++;
	a.setValue(value);
	return value;
    }

}
