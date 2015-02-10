package org.lemurproject.galago.core.parse.tagtok;

/**
 * @author trevor.
 * @see org.lemurproject.galago.core.parse.TagTokenizer
 */
public final class IntSpan {

	public IntSpan(int start, int end) {
		this.start = start;
		this.end = end;
	}
	public int start;
	public int end;

	@Override
	public String toString() {
		return String.format("%d,%d", start, end);
	}
}
