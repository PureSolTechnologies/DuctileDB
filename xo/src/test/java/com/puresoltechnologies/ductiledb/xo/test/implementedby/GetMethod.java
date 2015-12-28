package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import org.apache.tinkerpop.gremlin.structure.Element;

import com.buschmais.xo.api.proxy.ProxyMethod;

public class GetMethod implements ProxyMethod<Element> {

    @Override
    public Object invoke(Element propertyContainer, Object instance, Object[] args) {
	return propertyContainer.property("test") + "_get";
    }
}
