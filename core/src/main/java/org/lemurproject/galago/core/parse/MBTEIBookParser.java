// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.util.regex.Pattern;
import javax.xml.stream.XMLStreamConstants;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * STATES:
 *
 * S0 = starting state. Reads in header information. Stay in this state until
 * you see the "text" open tag, then head to S1.
 *
 * S1 = "w" tags trigger a read from the "form" attribute, which is the 
 *          outputted string.
 *      "name" opening and closing tags are echoed to the output string.
 *      "text" and "TEI" closing tags are also echoed to the output.
 *
 * S1 is a terminal state.
 *
 */
class MBTEIBookParser extends MBTEIParserBase {

  Pattern matchAll = Pattern.compile("[a-zA-Z0-9-_]+");
  Pattern textTag = Pattern.compile("text");
  Pattern teiTag = Pattern.compile("TEI", Pattern.CASE_INSENSITIVE);
  Pattern wordTag = Pattern.compile("w");
  Pattern nameTag = Pattern.compile("name");
  String header;
  int contentLength;  // use this to count actual terms, not tags.

  public MBTEIBookParser(DocumentSplit split, Parameters p) {
    super(split, p);
    // set up to parse the header       
    S0();
  }

  protected void S0() {
    // Put the more specific matches first
    addStartElementAction(textTag, "moveToS1");

    // Collect everything else
    addStartElementAction(matchAll, "echo");
    addEndElementAction(matchAll, "echo");
    setCharactersAction("echo");
  }

  public void moveToS1(int ignored) {
    header = buffer.toString();
    buffer = new StringBuilder();
    contentLength = 0;
    // Matched on "text" opening, but that should be
    // echoed. Do it manually.
    echo(XMLStreamConstants.START_ELEMENT);

    // Move on to the new rules
    // First remove old matchers
    clearStartElementActions();
    clearEndElementActions();
    unsetCharactersAction();

    // Now set up our normal processing matchers
    addStartElementAction(wordTag, "echoFormAttribute");
    addStartElementAction(nameTag, "echoWithAttributes");
    addEndElementAction(nameTag, "echo");
    addEndElementAction(textTag, "echo");
    addEndElementAction(teiTag, "emitFinalDocument");
  }

  // This needs some more focus. What do we put around punctuation?
  // Right now it simply appends a space after every token. Probably
  // not what we want.
  public void echoFormAttribute(int ignored) {
    String formValue = reader.getAttributeValue(null, "form");
    String scrubbed = scrub(formValue);
    if (scrubbed.length() > 0) {
      buffer.append(scrubbed).append(" ");
      ++contentLength;
    }
  }

  // This should only be called once but there isn't really a way 
  // to handle that gracefully other than removing all the handlers.
  public void emitFinalDocument(int ignored) {
    if (contentLength > 0) {
      // Echo "</tei>" or whatever it is.
      echo(XMLStreamConstants.END_ELEMENT);

      StringBuilder documentContent = new StringBuilder(header);
      documentContent.append(buffer);
      String bookIdentifier = getArchiveIdentifier();
      parsedDocument = new Document(bookIdentifier,
              documentContent.toString());
    }

    // Emit 1 document per file, or none at all.
    clearStartElementActions();
    clearEndElementActions();
    unsetCharactersAction();
    contentLength = 0;
  }
}