// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.parse.tagtok.*;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StringPooler;

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
	public static HashSet<String> ignoredTags = new HashSet<>(Arrays.asList("script", "style"));

  protected List<Pattern> whitelist;
	TagTokenizerParser state;

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
		state = new TagTokenizerParser(argp);

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
		state.reset();
  }


	/**
   * This method does three kinds of processing:
   * <ul>
   *  <li>If the token contains periods at the beginning or the end,
   *      they are removed.</li>
   *  <li>If the token contains single letters followed by periods, such
   *      as I.B.M., C.I.A., or U.S.A., the periods are removed.</li>
   *  <li>If, instead, the token contains longer strings of state.text with
   *      periods in the middle, the token is split into
   *      smaller tokens ("umass.edu" becomes {"umass", "edu"}).  Notice
   *      that this means ("ph.d." becomes {"ph", "d"}).</li>
   * </ul>
   */
	public void tokenAcronymProcessing(String token, int start, int end) {
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
      // at odd state.positions:
      boolean isAcronym = token.length() > 0;
      for (int pos = 1; pos < token.length(); pos += 2) {
        if (token.charAt(pos) != '.') {
          isAcronym = false;
        }
      }

      if (isAcronym) {
        token = token.replace(".", "");
        state.addToken(token, start, end);
      } else {
        int s = 0;
        for (int e = 0; e < token.length(); e++) {
          if (token.charAt(e) == '.') {
            if (e - s > 1) {
              String subtoken = token.substring(s, e);
              state.addToken(subtoken, start + s, start + e);
            }
            s = e + 1;
          }
        }

        if (token.length() - s > 0) {
          String subtoken = token.substring(s);
          state.addToken(subtoken, start + s, end);
        }
      }
    } else {
      state.addToken(token, start, end);
    }
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

	/**
   * Parses the state.text in the document.state.text attribute and fills in the
   * document.terms and document.tags arrays.
   *
   */
  @Override
  public void tokenize(Document document) {
    reset();
    assert(document != null);
    state.text = document.text;
    assert(state.text != null);

    try {
      // this loop is looking for tags, split characters, and XML escapes,
      // which start with ampersands.  All other characters are assumed to
      // be word characters.  The onSplit() method takes care of extracting
      // word state.text and storing it in the terms array.  The onStartBracket
      // method parses tags.  ignoreUntil is used to ignore comments and
      // script data.
      for (; state.position >= 0 && state.position < state.text.length(); state.position++) {
        char c = state.text.charAt(state.position);

        if (c == '<') {
          if (state.ignoreUntil == null) {
            state.onSplit(this);
          }
          state.onStartBracket();
        } else if (state.ignoreUntil != null) {
          continue;
        } else if (c == '&') {
          state.onAmpersand(this);
        } else if (c < 256 && TagPunctuation.splits[c] && state.tokenizeTagContent) {
          state.onSplit(this);
        }
      }
    } catch (Exception e) {
      log.log(Level.WARNING, "Parse failure: " + document.name, e);
    }

    if (state.ignoreUntil == null) {
      state.onSplit(this);
    }
    StringPooler.getInstance().transform(state.tokens);
    document.terms = new ArrayList<>(state.tokens);
    for (IntSpan p : state.tokenPositions) {
      document.termCharBegin.add(p.start);
      document.termCharEnd.add(p.end);
    }
    document.tags = coalesceTags(state.openTags, state.closedTags, whitelist);
  }

  public ArrayList<IntSpan> getTokenPositions() {
    return state.tokenPositions;
  }
}
