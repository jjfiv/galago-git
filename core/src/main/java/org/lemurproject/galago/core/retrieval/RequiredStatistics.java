/*
 * RequiredStatistics
 * 
 * September 20, 2007
 * 
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.retrieval;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * 
 * Class annotates iterators that require collection statistics
 *  - current supported statistics can be found in :
 *  AnnotateCollectionStatistics.java
 *
 * @author trevor
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface RequiredStatistics {
    public String[] statistics() default {};
}
