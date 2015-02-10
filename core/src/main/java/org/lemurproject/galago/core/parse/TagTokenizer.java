// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.parse.tagtok.*;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StringPooler;
import org.lemurproject.galago.core.parse.tagtok.TagTokenizerUtil;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * <p>This class processes document text into tokens that can be indexed.</p>
 * 
 * <p>The text is assumed to contain some HTML/XML tags.  The tokenizer tries
 * to extract as much data as possible from each document, even if it is not
 * well formed (e.g. there are start tags with no ending tags).  The resulting
 * document object contains an array of terms and an array of tags.</p> 
 * 
 * @author trevor
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class TagTokenizer extends Tokenizer {
  public static final Logger log = Logger.getLogger(TagTokenizer.class.getName());
	protected static HashSet<String> ignoredTags = new HashSet<>(Arrays.asList("script", "style"));

  protected String ignoreUntil;
  protected List<Pattern> whitelist;
	protected int maxTokenLength;

  protected String text;
  protected int position;
  protected int lastSplit;
  ArrayList<String> tokens;
  HashMap<String, ArrayList<BeginTag>> openTags;
  ArrayList<ClosedTag> closedTags;
  ArrayList<IntSpan> tokenPositions;
  private boolean tokenizeTagContent = true;

	public TagTokenizer() {
		this(Parameters.create());
	}
	public TagTokenizer(Parameters tokenizerParameters) {
		this(new FakeParameters(tokenizerParameters));
	}
	public TagTokenizer(TupleFlowParameters parameters) {
		super(parameters);
		init(parameters.getJSON());
  }

	private void init(Parameters argp) {
		// Max token length is now customizable.
		maxTokenLength = (int) argp.get("maxTokenLength", 100);

		text = null;
		position = 0;
		lastSplit = -1;

		tokens = new ArrayList<>();
		openTags = new HashMap<>();
		closedTags = new ArrayList<>();
		tokenPositions = new ArrayList<>();
		whitelist = new ArrayList<>();

		// This has to come after we initialize whitelist.
		if (argp.isList("fields") || argp.isString("fields")) {
			for (String value : argp.getAsList("fields", String.class)) {
				assert(whitelist != null);
				addField(value);
			}
		}
	}


	/** Register the fields that should be parsed and collected */
	public void addField(String f) {
    whitelist.add(Pattern.compile(f));
  }

	/**
   * Resets parsing in preparation for the next document.
   */
  public void reset() {
    ignoreUntil = null;
    text = null;
    position = 0;
    lastSplit = -1;

    tokens.clear();
    openTags.clear();
    closedTags.clear();

    if (tokenPositions != null) {
      tokenPositions.clear();
    }
  }

	/** Skip an HTML comment */
  protected void skipComment() {
    if (text.substring(position).startsWith("<!--")) {
      position = text.indexOf("-->", position + 1);

      if (position >= 0) {
        position += 2;
      }
    } else {
      position = text.indexOf(">", position + 1);
    }

    if (position < 0) {
      position = text.length();
    }
  }

  protected void skipProcessingInstruction() {
    position = text.indexOf("?>", position + 1);

    if (position < 0) {
      position = text.length();
    }
  }

  protected void parseEndTag() {
    // 1. read name (skipping the </ part)
    int i;

    for (i = position + 2; i < text.length(); i++) {
      char c = text.charAt(i);
      if (Character.isSpaceChar(c) || c == '>') {
        break;
      }
    }

    String tagName = text.substring(position + 2, i).toLowerCase();

    if (ignoreUntil != null && ignoreUntil.equals(tagName)) {
      ignoreUntil = null;
    }
    if (ignoreUntil == null) {
      closeTag(tagName);        // advance to end '>'
    }
    while (i < text.length() && text.charAt(i) != '>') {
      i++;
    }
    position = i;
  }

  protected void closeTag(final String tagName) {
    if (!openTags.containsKey(tagName)) {
      return;
    }
    ArrayList<BeginTag> tagList = openTags.get(tagName);

    if (tagList.size() > 0) {
      int last = tagList.size() - 1;

      BeginTag openTag = tagList.get(last);
      ClosedTag closedTag = new ClosedTag(openTag, position, tokens.size());
      closedTags.add(closedTag);

      tagList.remove(last);

      // switch out of Do not tokenize mode.
      if (!tokenizeTagContent) {
        tokenizeTagContent = true;
      }
    }

  }

	protected void parseBeginTag() {
    // 1. read the name, skipping the '<'
    int i;

    for (i = position + 1; i < text.length(); i++) {
      char c = text.charAt(i);
      if (Character.isSpaceChar(c) || c == '>') {
        break;
      }
    }

    String tagName = text.substring(position + 1, i).toLowerCase();

    // 2. read attr pairs
    i = TagTokenizerUtil.indexOfNonSpace(text, i);
    int tagEnd = text.indexOf(">", i + 1);
    boolean closeIt = false;

    HashMap<String, String> attributes = new HashMap<>();
    while (i < tagEnd && i >= 0 && tagEnd >= 0) {
      // scan ahead for non space
      int start = TagTokenizerUtil.indexOfNonSpace(text, i);

      if (start > 0) {
        if (text.charAt(start) == '>') {
          i = start;
          break;
        } else if (text.charAt(start) == '/'
                && text.length() > start + 1
                && text.charAt(start + 1) == '>') {
          i = start + 1;
          closeIt = true;
          break;
        }
      }

      int end = TagTokenizerUtil.indexOfEndAttribute(text, start, tagEnd);
      int equals = TagTokenizerUtil.indexOfEquals(text, start, end);

      // try to find an equals sign
      if (equals < 0 || equals == start || end == equals) {
        // if there's no equals, try to move to the next thing
        if (end < 0) {
          i = tagEnd;
          break;
        } else {
          i = end;
          continue;
        }
      }

      // there is an equals, so try to parse the value
      int startKey = start;
      int endKey = equals;

      int startValue = equals + 1;
      int endValue = end;

      if (text.charAt(startValue) == '\"' || text.charAt(startValue) == '\'') {
        startValue++;
      }
      if (startValue >= endValue || startKey >= endKey) {
        i = end;
        continue;
      }

      String key = text.substring(startKey, endKey);
      String value = text.substring(startValue, endValue);

      attributes.put(key.toLowerCase(), value);

      if (end >= text.length()) {
        endParsing();
        break;
      }

      if (text.charAt(end) == '\"' || text.charAt(end) == '\'') {
        end++;
      }

      i = end;
    }

    position = i;

    if (!ignoredTags.contains(tagName)) {
      BeginTag tag = new BeginTag(tagName, attributes, position + 1, tokens.size());

      if (!openTags.containsKey(tagName)) {
        ArrayList<BeginTag> tagList = new ArrayList<>();
        tagList.add(tag);
        openTags.put(tagName, tagList);
      } else {
        openTags.get(tagName).add(tag);
      }

      if (attributes.containsKey("tokenizetagcontent") && !closeIt) {
        String parseAttr = attributes.get("tokenizetagcontent");
        tokenizeTagContent = Boolean.parseBoolean(parseAttr);
      }

      if (closeIt) {
        closeTag(tagName);
      }
    } else if (!closeIt) {
      ignoreUntil = tagName;
    }

  }

  protected void endParsing() {
    position = text.length();
  }

  protected void onSplit() {
    if (position - lastSplit > 1) {
      int start = lastSplit + 1;
      String token = text.substring(start, position);
      StringStatus status = TagTokenizerUtil.checkTokenStatus(token);

      switch (status) {
        case NeedsSimpleFix:
          token = TagTokenizerUtil.normalizeSimple(token);
          break;

        case NeedsComplexFix:
          token = TagTokenizerUtil.normalizeComplex(token);
          break;

        case NeedsAcronymProcessing:
          tokenAcronymProcessing(token, start, position);
          break;

        case Clean:
          // do nothing
          break;
      }

      if (status != StringStatus.NeedsAcronymProcessing) {
        addToken(token, start, position);
      }
    }

    lastSplit = position;
  }

  /**
   * Adds a token to the document object.  This method currently drops tokens
   * longer than 100 bytes long right now.
   *
   * @param token  The token to add.
   * @param start  The starting byte offset of the token in the document text.
   * @param end    The ending byte offset of the token in the document text.
   */
  protected void addToken(final String token, int start, int end) {
    // zero length tokens aren't interesting
    if (token.length() <= 0) {
      return;
    }
    // we want to make sure the token is short enough that someone
    // might actually type it.  UTF-8 can expand one character to 6 bytes.

		// TODO(jfoley) This is a memory "waster" ...
		// we probably don't need this level of accuracy on the heuristic maxTokenLength,
		// but I'm hesitant to change *the* TagTokenizer... also we probably want OR here?
    if (token.length() > maxTokenLength / 6
            && ByteUtil.fromString(token).length >= maxTokenLength) {
      return;
    }
    tokens.add(token);
    tokenPositions.add(new IntSpan(start, end));
  }

	/**
   * This method does three kinds of processing:
   * <ul>
   *  <li>If the token contains periods at the beginning or the end,
   *      they are removed.</li>
   *  <li>If the token contains single letters followed by periods, such
   *      as I.B.M., C.I.A., or U.S.A., the periods are removed.</li>
   *  <li>If, instead, the token contains longer strings of text with
   *      periods in the middle, the token is split into
   *      smaller tokens ("umass.edu" becomes {"umass", "edu"}).  Notice
   *      that this means ("ph.d." becomes {"ph", "d"}).</li>
   * </ul>
   */
  protected void tokenAcronymProcessing(String token, int start, int end) {
    token = TagTokenizerUtil.normalizeComplex(token);

    // remove start and ending periods
    while (token.startsWith(".")) {
      token = token.substring(1);
      start = start + 1;
    }

    while (token.endsWith(".")) {
      token = token.substring(0, token.length() - 1);
      end -= 1;
    }

    // does the token have any periods left?
    if (token.indexOf('.') >= 0) {
      // is this an acronym?  then there will be periods
      // at odd positions:
      boolean isAcronym = token.length() > 0;
      for (int pos = 1; pos < token.length(); pos += 2) {
        if (token.charAt(pos) != '.') {
          isAcronym = false;
        }
      }

      if (isAcronym) {
        token = token.replace(".", "");
        addToken(token, start, end);
      } else {
        int s = 0;
        for (int e = 0; e < token.length(); e++) {
          if (token.charAt(e) == '.') {
            if (e - s > 1) {
              String subtoken = token.substring(s, e);
              addToken(subtoken, start + s, start + e);
            }
            s = e + 1;
          }
        }

        if (token.length() - s > 0) {
          String subtoken = token.substring(s);
          addToken(subtoken, start + s, end);
        }
      }
    } else {
      addToken(token, start, end);
    }
  }

	protected void onStartBracket() {
    if (position + 1 < text.length()) {
      char c = text.charAt(position + 1);

      if (c == '/') {
        parseEndTag();
      } else if (c == '!') {
        skipComment();
      } else if (c == '?') {
        skipProcessingInstruction();
      } else {
        parseBeginTag();
      }
    } else {
      endParsing();
    }

    lastSplit = position;
  }

  /**
   * Translates tags from the internal ClosedTag format to the
   * Tag type. Uses the whitelist in the tokenizer to omit tags
   * that are not matched by any patterns in the whitelist
   */
  protected static ArrayList<Tag> coalesceTags(Map<String, ArrayList<BeginTag>> openTags, Collection<ClosedTag> closedTags, List<Pattern> whitelist) {
    ArrayList<Tag> result = new ArrayList<>();

    // close all open tags
    for (List<BeginTag> tagList : openTags.values()) {
      for (BeginTag tag : tagList) {
        for (Pattern p : whitelist) {
          if (p.matcher(tag.name).matches()) {
            result.add(new Tag(tag.name, tag.attributes, tag.termPosition, tag.termPosition, tag.bytePosition, tag.bytePosition));
            break;
          }
        }
      }
    }

    for (ClosedTag tag : closedTags) {
      for (Pattern p : whitelist) {
        if (p.matcher(tag.name).matches()) {
          result.add(new Tag(tag.name, tag.attributes, tag.termStart, tag.termEnd, tag.byteStart, tag.byteEnd));
          break;
        }
      }
    }

    Collections.sort(result);
    return result;
  }

  protected void onAmpersand() {
    onSplit();

    for (int i = position + 1; i < text.length(); i++) {
      char c = text.charAt(i);

      if (c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '#') {
        continue;
      }
      if (c == ';') {
        position = i;
        lastSplit = i;
        return;
      }

      // not a valid escape sequence
      break;
    }
  }

  /**
   * Parses the text in the document.text attribute and fills in the
   * document.terms and document.tags arrays.
   *
   */
  @Override
  public void tokenize(Document document) {
    reset();
    assert(document != null);
    text = document.text;
    assert(text != null);

    try {
      // this loop is looking for tags, split characters, and XML escapes,
      // which start with ampersands.  All other characters are assumed to
      // be word characters.  The onSplit() method takes care of extracting
      // word text and storing it in the terms array.  The onStartBracket
      // method parses tags.  ignoreUntil is used to ignore comments and
      // script data.
      for (; position >= 0 && position < text.length(); position++) {
        char c = text.charAt(position);

        if (c == '<') {
          if (ignoreUntil == null) {
            onSplit();
          }
          onStartBracket();
        } else if (ignoreUntil != null) {
          continue;
        } else if (c == '&') {
          onAmpersand();
        } else if (c < 256 && TagPunctuation.splits[c] && tokenizeTagContent) {
          onSplit();
        }
      }
    } catch (Exception e) {
      log.log(Level.WARNING, "Parse failure: " + document.name, e);
    }

    if (ignoreUntil == null) {
      onSplit();
    }
    StringPooler.getInstance().transform(this.tokens);
    document.terms = new ArrayList<>(this.tokens);
    for (IntSpan p : this.tokenPositions) {
      document.termCharBegin.add(p.start);
      document.termCharEnd.add(p.end);
    }
    document.tags = coalesceTags(openTags, closedTags, whitelist);
  }

  public ArrayList<IntSpan> getTokenPositions() {
    return this.tokenPositions;
  }
}
