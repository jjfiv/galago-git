/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.tupleflow.execution;

/**
 * This class represents the situation where you are merging multiple
 * pipes. The restriction is that all listed input pipes have the same
 * type and sort order. All input pipes should be named.
 *
 * @author irmarc
 */
public class MultiInputStep extends Step {

  String[] ids;

  public MultiInputStep(String... inputs) {
    super("MultiInput");
    this.ids = inputs;
  }

  public String[] getIds() {
    return ids;
  }
}
