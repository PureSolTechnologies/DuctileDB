package com.puresoltechnologies.xo.titan.test.implementedby;

import com.buschmais.xo.api.proxy.ProxyMethod;
import com.tinkerpop.blueprints.Element;

public class SetMethod implements ProxyMethod<Element> {

	@Override
	public Object invoke(Element propertyContainer, Object instance,
			Object[] args) {
		String value = (String) args[0];
		propertyContainer.setProperty("test", "set_" + value);
		return null;
	}
}
