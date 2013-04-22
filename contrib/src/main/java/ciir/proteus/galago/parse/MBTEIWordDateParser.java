// BSD License (http://lemurproject.org/galago-license)
package ciir.proteus.galago.parse;

import java.util.ArrayList;
import java.util.regex.Pattern;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

public class MBTEIWordDateParser extends MBTEIParserBase {
  Pattern dateTag = Pattern.compile("date");
  Document wholeDocument;

  public MBTEIWordDateParser(DocumentSplit split, Parameters p) {
    super(split, p);
    wholeDocument = new Document();
    wholeDocument.terms = new ArrayList<String>();
    S0();
  }

  @Override
  public void cleanup() {
    parsedDocument = wholeDocument;
  }

  public void S0() {
    addStartElementAction(textTag, "moveToS1");
    addStartElementAction(dateTag, "startDate");
    addEndElementAction(dateTag, "stopDate");
  }

  public void moveToS1(int ignored) {
    clearAllActions();
    addStartElementAction(wordTag, "recordFormAttribute");
  }

  public void recordFormAttribute(int ignored) {
    String formValue = reader.getAttributeValue(null, "form");
    String normalized = normalize(formValue);
    if (normalized != null) {
      wholeDocument.terms.add(normalized);
    }
  }

  public void grabDate(int ignored) {
    String dateString = reader.getText();
    System.err.printf("Found date: '%s'\n", dateString);
    wholeDocument.name = getEarliestDate(dateString);
  }

  public String getEarliestDate(String dateString) {
    // clever is later
    return dateString;
  }

  public void startDate(int ignored) {
    setCharactersAction("grabDate");
  }

  public void stopDate(int ignored) {
    unsetCharactersAction();
  }
}