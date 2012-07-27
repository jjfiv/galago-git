// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.InputStream;
import org.lemurproject.galago.core.types.DocumentSplit;

class MBTEIPersonParser extends MBTEIEntityParser {
    public MBTEIPersonParser(DocumentSplit split, InputStream is) {
	super(split, is);
	restrict = "per";
    }
}