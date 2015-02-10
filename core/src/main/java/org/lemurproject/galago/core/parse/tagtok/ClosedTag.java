package org.lemurproject.galago.core.parse.tagtok;

import java.util.Map;

/**
 * @author trevor.
 * @see org.lemurproject.galago.core.parse.TagTokenizer
 */
public final class ClosedTag {

	public ClosedTag(BeginTag begin, int start, int end) {
		this.name = begin.name;
		this.attributes = begin.attributes;

		this.byteStart = begin.bytePosition;
		this.termStart = begin.termPosition;

		this.byteEnd = start;
		this.termEnd = end;
	}
	public String name;
	public Map<String, String> attributes;
	public int byteStart;
	public int termStart;
	public int byteEnd;
	public int termEnd;
}
