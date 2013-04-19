// BSD License (http://lemurproject.org/galago-license)
package ciir.proteus.galago.parse;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Pattern;
import javax.xml.stream.XMLStreamConstants;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.StringPooler;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

// Fundamentally operates differently than the book and page parsers,
// so it is subclassed higher up the hierarchy
//
// Assumptions
// - Names cannot be nested
public class MBTEIEntityParser extends MBTEIParserBase {

  public static final int WINDOW_SIZE = 30;
  Pattern pageBreakTag = Pattern.compile("pb");
  protected StringPooler pooler = new StringPooler();

  public class Context {

    public Context(String name,
            LinkedList<String> previousText) {
      this.name = name;
      tokens = new LinkedList<String>();
      for (String s : previousText) {
        tokens.add(TagTokenizer.processToken(scrub(s)));
      }
      numTrailingWords = WINDOW_SIZE;
    }
    String name;
    String type;
    int startPage;
    int startPos;
    String externalLink;
    List<String> tokens;
    int numTrailingWords;
  }
  // number of words before and after a name tag to associate
  public LinkedList<String> slidingWindow;
  public LinkedList<Context> openContexts;
  Pattern dateTag = Pattern.compile("date");
  String restrict = null;
  int pageNumber = 0;
  int pagePosition = 0;

  public MBTEIEntityParser(DocumentSplit split, Parameters p) {
    super(split, p);
    openContexts = new LinkedList<Context>();
    slidingWindow = new LinkedList<String>();
    S0();
  }

  // Override to check for "finished" documents (contexts)
  // before moving on. This handles a case where two contexts
  // finished at the same time, so after one is emitted, the
  // next one should be emitted immediately after that.
  @Override
  public Document nextDocument() throws IOException {
    if (openContexts.size() > 0) {
      Context leadingContext = openContexts.peek();
      if (leadingContext.numTrailingWords == 0) {
        // Will immediately cause the super call to return,
        // but interferes with the "flow" less.
        buildDocument();
      }
    }
    return super.nextDocument();
  }

  public void S0() {
    addStartElementAction(textTag, "moveToS1");
  }

  public void moveToS1(int ignored) {
    echo(XMLStreamConstants.START_ELEMENT);
    clearStartElementActions();

    addStartElementAction(wordTag, "updateContexts");
    addStartElementAction(nameTag, "openNewContext");
    addEndElementAction(textTag, "removeActionsAndEmit");
    addStartElementAction(pageBreakTag, "nextPage");
  }

  public void nextPage(int ignored) {
    pageNumber = Integer.parseInt(reader.getAttributeValue(null, "n"));
    pagePosition = 0;
  }

  public void removeActionsAndEmit(int event) {
    echo(event);
    clearAllActions();
    buildDocument();
  }

  public void openNewContext(int ignored) {
    String type = reader.getAttributeValue(null, "type").toLowerCase();
    if (restrict != null && !restrict.equals(type)) {
      return; // skip since it's not the restricted type
    }
    String name = filterName(reader.getAttributeValue(null, "name"));
    if (name == null) {
      return;
    }

    Context freshContext = new Context(name, slidingWindow);
    freshContext.type = type;
    freshContext.startPos = pagePosition;
    freshContext.startPage = pageNumber;
    String wikiLink = reader.getAttributeValue(null, "Wiki_Title");
    if (wikiLink == null || wikiLink.equals("NIL") || wikiLink.equals("IGNORE")) {
      freshContext.externalLink = null;
    } else {
      freshContext.externalLink = wikiLink;
    }
    openContexts.addLast(freshContext);
  }

  public void updateContexts(int ignored) {
    String formValue = reader.getAttributeValue(null, "form");
    String scrubbed = TagTokenizer.processToken(scrub(formValue));
    slidingWindow.addLast(scrubbed);
    while (slidingWindow.size() > WINDOW_SIZE) {
      slidingWindow.poll();
    }

    for (Context c : openContexts) {
      c.tokens.add(scrubbed);
      --c.numTrailingWords;
    }

    // Finally check for a finished context
    if (openContexts.size() > 0
            && openContexts.peek().numTrailingWords == 0) {
      buildDocument();
    }
    ++pagePosition;
  }

  public void buildDocument() {
    if (openContexts.size() > 0) {
      Context closingContext = openContexts.poll();
      parsedDocument = new Document();
      parsedDocument.name = closingContext.name;
      parsedDocument.identifier = closingContext.name.hashCode();
      parsedDocument.metadata.put("title", closingContext.name);
      parsedDocument.metadata.put("sourceIdentifier", getArchiveIdentifier());
      if (closingContext.externalLink != null) {
        parsedDocument.metadata.put("externalLink", closingContext.externalLink);
      }
      parsedDocument.metadata.put("startPage", Integer.toString(closingContext.startPage));
      parsedDocument.metadata.put("startPos", Integer.toString(closingContext.startPos));
      parsedDocument.terms = closingContext.tokens;
      pooler.transform(parsedDocument);
    } else {
      parsedDocument = null;
    }
  }

  public void cleanup() {
    if (openContexts.size() > 0) {
      // Restrict out any contexts that don't match.
      if (restrict != null) {
        LinkedList<Context> temp = new LinkedList<Context>();
        for (Context c : openContexts) {
          if (restrict.equals(c.type)) {
            temp.add(c);
          }
        }
        openContexts = temp;
      }

      for (Context c : openContexts) {
        c.numTrailingWords = 0;
      }
      // immediately put a document up
      buildDocument();
    }
  }
}
