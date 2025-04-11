package io.trino.plugin.deltalake.util;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;
import io.trino.spi.TrinoException;

import java.util.Optional;
import java.util.Set;

import static io.trino.plugin.deltalake.DeltaLakeErrorCode.DELTA_LAKE_INVALID_SCHEMA;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.SKIP_STATS_COLUMN_CONFIGURATION_KEY;
import static java.lang.String.format;

public final class SkippingStatsColumnsUtils
{
    private static final String SPECIAL_CHARS = "!@#$%^&*()+-={}|[]:\";'<>,.?/";
    private static final Escaper ESCAPER;

    static {
        Escapers.Builder builder = Escapers.builder();
        for (char c : SPECIAL_CHARS.toCharArray()) {
            builder.addEscape(c, "\\" + c);
        }
        builder.addEscape('\\', "\\\\");
        ESCAPER = builder.build();
    }

    private SkippingStatsColumnsUtils() {}

    public static String toSkippingStatsColumnsString(Iterable<String> skippingStatsColumns)
    {
        ImmutableSet.Builder<String> result = ImmutableSet.builder();
        for (String column : skippingStatsColumns) {
            result.add(toDeltaName(column));
        }
        return Joiner.on(",").join(result.build());
    }

    public static Set<String> getSkippingStatsColumns(Optional<String> skippingStatsColumnsProperty)
    {
        if (skippingStatsColumnsProperty.isEmpty()) {
            return ImmutableSet.of();
        }

        String property = skippingStatsColumnsProperty.get();
        StringBuilder current = new StringBuilder();
        boolean inBackticks = false;

        ImmutableSet.Builder<String> result = ImmutableSet.builder();
        for (char c : property.toCharArray()) {
            if (c == '`') {
                inBackticks = !inBackticks;
            }
            else if (c == ',' && !inBackticks) {
                String token = current.toString().trim();
                if (!token.isEmpty()) {
                    result.add(toTrinoName(token));
                }
                current.setLength(0);
                continue;
            }
            current.append(c);
        }

        if (inBackticks) {
            throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("Invalid value for %s property: %s", SKIP_STATS_COLUMN_CONFIGURATION_KEY, property));
        }

        String lastToken = current.toString().trim();
        if (!lastToken.isEmpty()) {
            result.add(toTrinoName(lastToken));
        }

        return result.build();
    }

    // escape Trino name to Delta name, but without escape ` to ``
    public static String escapeSpecialChars(String columnName)
    {
        return ESCAPER.escape(columnName);
    }

    private static String unescape(String name)
    {
        StringBuilder result = new StringBuilder();
        int length = name.length();

        for (int i = 0; i < length; i++) {
            char c = name.charAt(i);
            if (c != '\\') {
                result.append(c);
                continue;
            }

            if (i + 1 >= length) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                        format("Invalid column in %s property: %s", SKIP_STATS_COLUMN_CONFIGURATION_KEY, name));
            }

            char next = name.charAt(++i);
            if (next != '\\' && !isSpecialChar(next)) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA,
                        format("Invalid column in %s property: %s", SKIP_STATS_COLUMN_CONFIGURATION_KEY, name));
            }

            result.append(next);
        }

        return result.toString();
    }

    private static boolean isValidUnquotedName(String s)
    {
        for (char c : s.toCharArray()) {
            if (!isAllowedInUnquoted(c)) {
                return false;
            }
        }
        return true;
    }

    private static String toDeltaName(String name)
    {
        if (isValidUnquotedName(name)) {
            return name;
        }

        String unescaped = unescape(name);
        String escapedBackticks = unescaped.replace("`", "``");
        return "`" + escapedBackticks + "`";
    }

    /**
     * Parses a single column name according to the rules.
     * Special chars : !@#$%^&*()_+-={}|[]:";'<>,.?/
     * a.b.c -> a.b.c
     * `a.b.c` -> a\\.b\\.c
     * `aa.b.c` -> aa\\.b\\.c
     * `a\.b.c` -> a\\\\\\.b\\.c
     * `abc` -> abc
     * <p>
     * a!b -> ERROR
     * a\.b -> ERROR
     * a@b -> ERROR
     */
    public static String toTrinoName(String name)
    {
        if (name.startsWith("`")) {
            if (!name.endsWith("`")) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("Invalid name in %s property: %s", SKIP_STATS_COLUMN_CONFIGURATION_KEY, name));
            }

            String content = name.substring(1, name.length() - 1);
            content = content.replaceAll("``", "`");

            StringBuilder result = new StringBuilder();
            for (char c : content.toCharArray()) {
                if (c == '\\') {
                    result.append("\\\\");
                }
                else if (isSpecialChar(c)) {
                    // escape special char with \
                    result.append("\\").append(c);
                }
                else {
                    result.append(c);
                }
            }
            return result.toString();
        }

        for (char c : name.toCharArray()) {
            if (!isAllowedInUnquoted(c)) {
                throw new TrinoException(DELTA_LAKE_INVALID_SCHEMA, format("Invalid name in %s property: %s", SKIP_STATS_COLUMN_CONFIGURATION_KEY, name));
            }
        }
        return name;
    }

    private static boolean isSpecialChar(char c)
    {
        return SPECIAL_CHARS.indexOf(c) >= 0 || c == '.';
    }

    private static boolean isAllowedInUnquoted(char c)
    {
        return Character.isLetterOrDigit(c) || c == '_' || c == '.';
    }
}
