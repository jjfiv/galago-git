/*
 * RequiredStatistics
 * 
 * September 20, 2007
 * 
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval.structured;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 *
 * @author trevor
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface RequiredStatistics {
    public String[] statistics() default {};
}
