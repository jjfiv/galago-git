/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Class annotates iterators that require collection statistics
 *  - current supported statistics can be found in :
 *    AnnotateParameters.java
 *
 * @author sjh
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface RequiredParameters {
    public String[] parameters() default {};
}
