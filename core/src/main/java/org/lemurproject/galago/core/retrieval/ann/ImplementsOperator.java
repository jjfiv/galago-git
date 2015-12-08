package org.lemurproject.galago.core.retrieval.ann;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * @author jfoley
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface ImplementsOperator {
  String value();
}
