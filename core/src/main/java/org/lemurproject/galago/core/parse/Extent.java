// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

public class Extent {
    public Extent(String text, int begin, int end) {
        this.text = text;
        this.begin = begin;
        this.end = end;
    }
    String text;
    int begin;
    int end;
}
