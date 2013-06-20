/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author jfoley
 */
public interface ExtentSource extends CountSource {
  public ExtentArray extents(int id);
}
