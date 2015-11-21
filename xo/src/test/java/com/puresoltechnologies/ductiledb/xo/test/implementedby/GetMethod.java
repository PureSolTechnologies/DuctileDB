package com.puresoltechnologies.ductiledb.xo.test.implementedby;

import com.buschmais.xo.api.proxy.ProxyMethod;
import com.tinkerpop.blueprints.Element;

public class GetMethod implements ProxyMethod<Element> {

	@Override
	public Object invoke(Element propertyContainer, Object instance,
			Object[] args) {
		return propertyContainer.getProperty("test") + "_get";
	}
}
