package com.puresoltechnologies.ductiledb.xo.test.concurrency;

import java.util.concurrent.TimeUnit;

import com.buschmais.xo.api.annotation.ImplementedBy;
import com.buschmais.xo.api.proxy.ProxyMethod;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;
import com.tinkerpop.blueprints.Vertex;

@VertexDefinition
public interface TestEntity {

	@ImplementedBy(IncrementAndGet.class)
	int incrementAndGet();

	public class IncrementAndGet implements ProxyMethod<Vertex> {

		@Override
		public Object invoke(Vertex vertex, Object instance, Object[] args)
				throws Exception {
			Integer value = vertex.getProperty("value");
			if (value == null) {
				value = 0;
			}
			TimeUnit.SECONDS.sleep(5);
			value++;
			vertex.setProperty("value", value);
			return value;
		}
	}

}
