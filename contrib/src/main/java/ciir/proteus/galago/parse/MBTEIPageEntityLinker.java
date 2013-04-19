// BSD License (http://lemurproject.org/galago-license)
package ciir.proteus.galago.parse;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.regex.Pattern;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

public class MBTEIPageEntityLinker extends MBTEIParserBase {

  LinkedList<Document> entitiesInScope;
  Document bookLinks;
  Document pageLinks;
  int bookPosition;
  int pagePosition;
  String pageNumber;
  LinkedList<Document> finishedDocumentQueue;
  Pattern pageBreakTag = Pattern.compile("pb");

  public MBTEIPageEntityLinker(DocumentSplit split, Parameters p) {
    super(split, p);
    S0();
    entitiesInScope = new LinkedList<Document>();
    bookLinks = new Document();
    bookLinks.name = "COLLECTION-LINK";
    bookLinks.metadata.put("id", getArchiveIdentifier());
    bookLinks.metadata.put("type", "collection");
    bookLinks.metadata.put("pos", "0");
    bookLinks.tags = new LinkedList<Tag>();
    pagePosition = bookPosition = 0;
    pageLinks = null;
    finishedDocumentQueue = new LinkedList<Document>();
  }

  @Override
  protected Document getParsedDocument() {
    if (finishedDocumentQueue.size() > 0) {
      return finishedDocumentQueue.poll();
    } else {
      return null;
    }
  }

  @Override
  protected boolean documentReady() {
    return finishedDocumentQueue.size() > 0;
  }

  @Override
  public void cleanup() {
    // Close all entities
    while (!entitiesInScope.isEmpty()) {
      emitLinks(entitiesInScope.poll());
    }

    // Last page if needed
    if (pageLinks != null) {
      emitLinks(pageLinks);
    }

    // Finally emit links for the whole book.
    emitLinks(bookLinks);
  }

  public void S0() {
    // Don't need any metadata but need to know when to start
    // normal processing
    addStartElementAction(textTag, "moveToS1");
  }

  public void moveToS1(int ignored) {
    clearStartElementActions();
    addStartElementAction(nameTag, "mapTag");
    addStartElementAction(wordTag, "increment");
    addStartElementAction(pageBreakTag, "emitPageLinks");
  }

  public void mapTag(int ignored) {
    String innerType = reader.getAttributeValue(null, "type");
    String identifier = filterName(reader.getAttributeValue(null, "name"));
    if (identifier == null) {
      return;
    }

    int length = identifier.split(" ").length;
    HashMap<String, String> attributes = new HashMap<String, String>();
    attributes.put("id", identifier);
    attributes.put("type", innerType);
    attributes.put("pos", Integer.toString(pagePosition));
    Tag pageLink = new Tag(String.format("%s-LINK", innerType.toUpperCase()),
            attributes,
            pagePosition,
            pagePosition + length);
    pageLinks.tags.add(pageLink);
    updateInterEntityLinks(identifier, innerType, bookPosition, length);
    Tag bookLink = new Tag(String.format("%s-LINK", innerType.toUpperCase()),
            null,
            bookPosition,
            bookPosition + length);
    // Make a deep copy of the attributes and then replace entries.
    // It's wasteful, but it makes the mapping between documents and tags 
    // and links less painful.
    bookLink.attributes = new HashMap<String, String>();
    bookLink.attributes.putAll(attributes);
    bookLink.attributes.put("pos", Integer.toString(bookPosition));
    bookLinks.tags.add(bookLink);
  }

  public void updateInterEntityLinks(String identifier,
          String type,
          int position,
          int length) {
    // Make it as a Tag
    HashMap<String, String> attributes = new HashMap<String, String>();
    attributes.put("id", identifier);
    attributes.put("type", type);
    attributes.put("pos", Integer.toString(position));
    Tag entityLink = new Tag(String.format("%s-LINK", type.toUpperCase()),
            attributes,
            position,
            position + length);
    for (Document scopedEntity : entitiesInScope) {
      scopedEntity.tags.add(entityLink);
    }

    // Add it into the open scope
    Document entityDocument = new Document();
    entityDocument.name = String.format("%s-LINK", type);
    entityDocument.metadata.put("id", identifier);
    entityDocument.metadata.put("type", type);
    entityDocument.metadata.put("pos", Integer.toString(position));
    entityDocument.tags = new ArrayList<Tag>();
    entitiesInScope.add(entityDocument);
  }

  public void increment(int ignored) {
    ++pagePosition;
    ++bookPosition;
    while (entitiesInScope.size() > 0) {
      Document lead = entitiesInScope.peek();
      int leadPosition = Integer.parseInt(lead.metadata.get("pos"));
      if (bookPosition - leadPosition > MBTEIEntityParser.WINDOW_SIZE) {
        emitLinks(entitiesInScope.poll());
      } else {
        break;
      }
    }

  }

  public void emitPageLinks(int ignored) {
    if (pageLinks != null) {
      emitLinks(pageLinks);
    }
    pageNumber = reader.getAttributeValue(null, "n");
    pagePosition = 0;
    String pageId = String.format("%s_%s",
            getArchiveIdentifier(),
            pageNumber);
    pageLinks = new Document();
    pageLinks.name = "PAGE-LINK";
    pageLinks.metadata.put("id", pageId);
    pageLinks.metadata.put("type", "page");
    pageLinks.metadata.put("pos", "0");
    pageLinks.tags = new LinkedList<Tag>();

    // Add an entry to the bookLinks to map book --> page
    HashMap<String, String> attributes = new HashMap<String, String>();
    attributes.put("id", pageId);
    attributes.put("type", "page");
    attributes.put("pos", Integer.toString(bookPosition));
    Tag pageToBookLink = new Tag("PAGE-LINK",
            attributes,
            bookPosition,
            bookPosition);
    bookLinks.tags.add(pageToBookLink);
  }

  // Although we have links going one way (i.e. page --> person),
  // we need to generate all of the person --> page links.
  protected void emitLinks(Document forwardLinks) {
    if (forwardLinks.tags.size() == 0) {
      return;
    }
    finishedDocumentQueue.add(forwardLinks);
    for (Tag forwardTag : forwardLinks.tags) {
      Document reverseLink = new Document();
      reverseLink.name = forwardTag.name;
      reverseLink.metadata.put("id", forwardTag.attributes.get("id"));
      reverseLink.metadata.put("type", forwardTag.attributes.get("type"));
      reverseLink.metadata.put("pos", forwardTag.attributes.get("pos"));
      reverseLink.tags = new ArrayList<Tag>(1);
      Tag reverseTag = new Tag(forwardLinks.name,
              forwardLinks.metadata,
              0,
              0);
      reverseLink.tags.add(reverseTag);
      finishedDocumentQueue.add(reverseLink);
    }
  }
}
