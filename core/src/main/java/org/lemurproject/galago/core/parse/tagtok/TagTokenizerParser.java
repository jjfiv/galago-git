package org.lemurproject.galago.core.parse.tagtok;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This class deals with the pain of parsing HTML/XML/SGML and splitting words into terms. All Normalization of terms is done in a separate class.
 * @author trevor, hacked in two by jfoley.
*/
public final class TagTokenizerParser implements DocumentBuilder {
	private final int maxTokenLength;
	public String ignoreUntil;
	public String text;
	public int position;
	int lastSplit;
	public boolean tokenizeTagContent;
	public ArrayList<String> tokens;
	public HashMap<String, ArrayList<BeginTag>> openTags;
	public ArrayList<ClosedTag> closedTags;
	public ArrayList<IntSpan> tokenPositions;
	TagTokenizerNormalization normalizer;

	public TagTokenizerParser(Parameters argp) {
		// Max token length is now customizable.
		maxTokenLength = (int) argp.get("maxTokenLength", 100);
		tokens = new ArrayList<>();
		openTags = new HashMap<>();
		closedTags = new ArrayList<>();
		tokenPositions = new ArrayList<>();
		normalizer = new TagTokenizerNormalization(this);

		reset();
	}

	public void reset() {
		ignoreUntil = null;
		text = null;
		position = 0;
		lastSplit = -1;
		tokenizeTagContent = true;
		tokens.clear();
		openTags.clear();
		closedTags.clear();
		tokenPositions.clear();
	}

	/**
	 * Adds a token to the document object.  This method currently drops tokens
	 * longer than 100 bytes long right now.
	 *
	 * @param token  The token to add.
	 * @param start  The starting byte offset of the token in the document text.
	 * @param end    The ending byte offset of the token in the document text.
	 */
	@Override
	public void addToken(final String token, int start, int end) {
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

	@Override
	public void finishDocument(Document document, List<Pattern> tagWhitelist) {
		document.terms = new ArrayList<>(tokens);
		for (IntSpan p : tokenPositions) {
			document.termCharBegin.add(p.start);
			document.termCharEnd.add(p.end);
		}
		document.tags = coalesceTags(tagWhitelist);
	}

	/**
	 * Translates tags from the internal ClosedTag format to the
	 * Tag type. Uses the whitelist in the tokenizer to omit tags
	 * that are not matched by any patterns in the whitelist
	 */
	protected ArrayList<Tag> coalesceTags(List<Pattern> whitelist) {
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

		if (!TagTokenizer.ignoredTags.contains(tagName)) {
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

	public void onSplit() {
		if (position - lastSplit > 1) {
			int start = lastSplit + 1;
			String token = text.substring(start, position);
			normalizer.processAndAddToken(token, start, position);
		}

		lastSplit = position;
	}

	public void onStartBracket() {
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

	public void onAmpersand() {
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
	 * This 'main' loop is looking for tags, split characters, and XML escapes, which start with ampersands.  All other characters are assumed to be word characters.  The onSplit() method takes care of extracting word state.text and storing it in the terms array.  The onStartBracket method parses tags. ignoreUntil is used to ignore comments and script data.
	 */
	public void parse() {
		// main parsing loop.
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

		if (ignoreUntil == null) {
			onSplit();
		}
	}

}
