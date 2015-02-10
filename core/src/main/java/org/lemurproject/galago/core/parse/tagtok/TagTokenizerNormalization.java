package org.lemurproject.galago.core.parse.tagtok;

/**
 * @author trevor, hacked in two by jfoley.
 */
public class TagTokenizerNormalization {

	private final DocumentBuilder documentBuilder;

	public TagTokenizerNormalization(DocumentBuilder parser) {
		this.documentBuilder = parser;
	}

	public void processAndAddToken(String token, int start, int end) {
		StringStatus status = checkTokenStatus(token);

		switch (status) {
			case NeedsSimpleFix:
				token = normalizeSimple(token);
				break;

			case NeedsComplexFix:
				token = normalizeComplex(token);
				break;

			case NeedsAcronymProcessing:
				extractTermsFromAcronym(token, start, end);
				break;

			case Clean:
				// do nothing
				break;
		}

		if (status != StringStatus.NeedsAcronymProcessing) {
			documentBuilder.addToken(token, start, end);
		}
	}

	public static String normalizeComplex(String token) {
		token = normalizeSimple(token);
		token = token.toLowerCase();

		return token;
	}

	/**
	 * This method scans the token, looking for uppercase characters and
	 * special characters.  If the token contains only numbers and lowercase
	 * letters, it needs no further processing, and it returns Clean.
	 * If it also contains uppercase letters or apostrophes, it returns
	 * NeedsSimpleFix.  If it contains special characters (especially Unicode
	 * characters), it returns NeedsComplexFix.  Finally, if any periods are
	 * present, this returns NeedsAcronymProcessing.
	 */
	public static StringStatus checkTokenStatus(final String token) {
		StringStatus status = StringStatus.Clean;
		char[] chars = token.toCharArray();

		for (char c : chars) {
			boolean isAsciiLowercase = (c >= 'a' && c <= 'z');
			boolean isAsciiNumber = (c >= '0' && c <= '9');

			if (isAsciiLowercase || isAsciiNumber) {
				continue;
			}
			boolean isAsciiUppercase = (c >= 'A' && c <= 'Z');
			boolean isPeriod = (c == '.');
			boolean isApostrophe = (c == '\'');

			if ((isAsciiUppercase || isApostrophe) && status == StringStatus.Clean) {
				status = StringStatus.NeedsSimpleFix;
			} else if (!isPeriod) {
				status = StringStatus.NeedsComplexFix;
			} else {
				status = StringStatus.NeedsAcronymProcessing;
				break;
			}
		}

		return status;
	}


	/**
	 * Scans through the token, removing apostrophes and converting
	 * uppercase to lowercase letters.
	 */
	public static String normalizeSimple(String token) {
		char[] chars = token.toCharArray();
		int j = 0;

		for (int i = 0; i < chars.length; i++) {
			char c = chars[i];
			boolean isAsciiUppercase = (c >= 'A' && c <= 'Z');
			boolean isApostrophe = (c == '\'');

			if (isAsciiUppercase) {
				chars[j] = (char) (chars[i] + 'a' - 'A');
			} else if (isApostrophe) {
				// it's an apostrophe, skip it
				j--;
			} else {
				chars[j] = chars[i];
			}

			j++;
		}

		token = new String(chars, 0, j);
		return token;
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
	 * @param token The term containing dots.
	 * @param start Start offset in outer document.
	 * @param end End offset in outer document.
	 */
	public void extractTermsFromAcronym(String token, int start, int end) {
		token = normalizeComplex(token);

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
				documentBuilder.addToken(token, start, end);
			} else {
				int s = 0;
				for (int e = 0; e < token.length(); e++) {
					if (token.charAt(e) == '.') {
						if (e - s > 1) {
							String subtoken = token.substring(s, e);
							documentBuilder.addToken(subtoken, start + s, start + e);
						}
						s = e + 1;
					}
				}

				if (token.length() - s > 0) {
					String subtoken = token.substring(s);
					documentBuilder.addToken(subtoken, start + s, end);
				}
			}
		} else {
			documentBuilder.addToken(token, start, end);
		}
	}
}
