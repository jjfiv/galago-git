// BSD License (http://lemurproject.org/galago-license)
package ciir.proteus.galago.parse;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

class MBTEIMiscParser extends MBTEIEntityParser {
    public MBTEIMiscParser(DocumentSplit split, Parameters p) {
	super(split, p);
	restrict = "misc";
    }
}