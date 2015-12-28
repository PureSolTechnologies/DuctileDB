package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import org.apache.tinkerpop.gremlin.structure.Element;

import com.buschmais.xo.api.proxy.ProxyMethod;

public class SetMethod implements ProxyMethod<Element> {

    @Override
    public Object invoke(Element propertyContainer, Object instance, Object[] args) {
	String value = (String) args[0];
	propertyContainer.property("test", "set_" + value);
	return null;
    }
}
