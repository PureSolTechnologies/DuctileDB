package com.puresoltechnologies.ductiledb.xo.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.buschmais.xo.spi.annotation.EntityDefinition;

/**
 * This annotation marks entities as Titan vertex.
 */
@EntityDefinition
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface VertexDefinition {

    String DEFAULT_VALUE = "";

    /**
     * @return Returns the name of the type as {@link String}.
     */
    String value() default "";

    /**
     * @return The (super) type containing an indexed property ({@link Indexed}
     *         ).
     *         <p>
     *         An index will be created for this label and the indexed property,
     *         too.
     *         </p>
     */
    Class<?> usingIndexedPropertyOf() default Object.class;

}
