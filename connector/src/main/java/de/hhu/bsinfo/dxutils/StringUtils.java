/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxutils;

import java.io.UnsupportedEncodingException;
import java.text.DecimalFormatSymbols;
import java.util.regex.Pattern;

/**
 * Various string related utility functions.
 *
 * @author Florian Klein, florian.klein@hhu.de, 05.02.2014
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.03.2017
 */
public final class StringUtils {

    private static final DecimalFormatSymbols DECIMAL_SYMBOLS = DecimalFormatSymbols.getInstance();

    public static final char MINUS_SIGN = DECIMAL_SYMBOLS.getMinusSign();

    public static final String DECIMAL_SEPERATOR = Character.toString(DECIMAL_SYMBOLS.getDecimalSeparator());

    /**
     * Utils class
     */
    private StringUtils() {

    }

    /**
     * Checks if a Byte array contains a String (as much precise as possible)
     *
     * @param p_array
     *         the byte array
     * @return whether the byte array contains a String
     * @throws UnsupportedEncodingException
     *         if the encoding is unsupported
     */
    public static boolean looksLikeUTF8(final byte[] p_array) throws UnsupportedEncodingException {
        String phonyString;
        Pattern p;

        p = Pattern.compile(
                "\\A(\n" + "  [\\x09\\x0A\\x0D\\x20-\\x7E]             # ASCII\\n" +
                        "| [\\xC2-\\xDF][\\x80-\\xBF]               # non-overlong 2-byte\n" +
                        "|  \\xE0[\\xA0-\\xBF][\\x80-\\xBF]         # excluding overlongs\n" +
                        "| [\\xE1-\\xEC\\xEE\\xEF][\\x80-\\xBF]{2}  # straight 3-byte\n" +
                        "|  \\xED[\\x80-\\x9F][\\x80-\\xBF]         # excluding surrogates\n" +
                        "|  \\xF0[\\x90-\\xBF][\\x80-\\xBF]{2}      # planes 1-3\n" +
                        "| [\\xF1-\\xF3][\\x80-\\xBF]{3}            # planes 4-15\n" +
                        "|  \\xF4[\\x80-\\x8F][\\x80-\\xBF]{2}      # plane 16\n" + ")*\\z",
                Pattern.COMMENTS);

        phonyString = new String(p_array, "UTF-8");

        return p.matcher(phonyString).matches();
    }

    public static boolean isNumeric(final String p_value) {
        String cleanedInput = p_value.replace(DECIMAL_SEPERATOR, "");

        char firstChar = cleanedInput.charAt(0);
        if (firstChar != MINUS_SIGN && !Character.isDigit(firstChar)) {
            return false;
        }

        char[] characters = cleanedInput.substring(1).toCharArray();
        for (char character : characters) {
            if (!Character.isDigit(character)) {
                return false;
            }
        }

        return true;
    }
}
