package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import com.buschmais.xo.api.proxy.ProxyMethod;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;

public class CompareToMethod implements ProxyMethod<DuctileVertex> {

    @Override
    public Object invoke(DuctileVertex node, Object instance, Object[] args) {
	return ((A) instance).getValue() - ((A) args[0]).getValue();
    }

}
