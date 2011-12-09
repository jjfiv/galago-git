// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public interface Reducer<T> {
    public ArrayList<T> reduce(List<T> input) throws IOException;
}
