package com.puresoltechnologies.ductiledb.xo.test.concurrency;

import java.util.concurrent.TimeUnit;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.buschmais.xo.api.annotation.ImplementedBy;
import com.buschmais.xo.api.proxy.ProxyMethod;
import com.puresoltechnologies.ductiledb.tinkerpop.DuctileVertex;
import com.puresoltechnologies.ductiledb.xo.api.annotation.VertexDefinition;

@VertexDefinition
public interface TestEntity {

    @ImplementedBy(IncrementAndGet.class)
    int incrementAndGet();

    public class IncrementAndGet implements ProxyMethod<DuctileVertex> {

	@Override
	public Object invoke(DuctileVertex vertex, Object instance, Object[] args) throws Exception {
	    VertexProperty<Object> property = vertex.property("value");
	    int value = 0;
	    if ((property != null) && (property.isPresent())) {
		value = (Integer) property.value();
	    }
	    TimeUnit.SECONDS.sleep(5);
	    value++;
	    vertex.property("value", value);
	    return value;
	}
    }

}
