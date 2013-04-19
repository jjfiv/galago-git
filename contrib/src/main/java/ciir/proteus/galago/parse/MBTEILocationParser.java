// BSD License (http://lemurproject.org/galago-license)
package ciir.proteus.galago.parse;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

public class MBTEILocationParser extends MBTEIEntityParser {

  public MBTEILocationParser(DocumentSplit split, Parameters p) {
    super(split, p);
    restrict = "loc";
  }
}