// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.InputStream;
import org.lemurproject.galago.core.types.DocumentSplit;

class MBTEILocationParser extends MBTEIEntityParser {
    public MBTEILocationParser(DocumentSplit split, InputStream is) {
	super(split, is);
	restrict = "loc";
    }
}