package io.trino.plugin.deltalake.util;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

public class TestSkippingStatsColumnsUtils
{
    @Test
    public void testSkippingStatsColumns()
    {
        assertThat(SkippingStatsColumnsUtils.getSkippingStatsColumns(Optional.empty())).isEmpty();

        assertThat(SkippingStatsColumnsUtils.getSkippingStatsColumns(Optional.of("a,b,c"))).isEqualTo(ImmutableSet.of("a", "b", "c"));
        assertThat(SkippingStatsColumnsUtils.toSkippingStatsColumnsString(ImmutableSet.of("a", "b", "c"))).isEqualTo("a,b,c");

        assertThat(SkippingStatsColumnsUtils.getSkippingStatsColumns(Optional.of("aaa,bbb,cc"))).isEqualTo(ImmutableSet.of("aaa", "bbb", "cc"));
        assertThat(SkippingStatsColumnsUtils.toSkippingStatsColumnsString(ImmutableSet.of("aaa", "bbb", "cc"))).isEqualTo("aaa,bbb,cc");

        assertThat(SkippingStatsColumnsUtils.getSkippingStatsColumns(Optional.of("`a!b`, `a#b`, `a$b`, `a-b`"))).isEqualTo(ImmutableSet.of("a\\!b", "a\\#b", "a\\$b", "a\\-b"));
        assertThat(SkippingStatsColumnsUtils.toSkippingStatsColumnsString(ImmutableSet.of("a\\!b", "a\\#b", "a\\$b", "a\\-b"))).isEqualTo("`a!b`,`a#b`,`a$b`,`a-b`");

        assertThat(SkippingStatsColumnsUtils.getSkippingStatsColumns(Optional.of("`a@!#b`, `a[]%.#b`, `a$%^&b`"))).isEqualTo(ImmutableSet.of("a\\@\\!\\#b", "a\\[\\]\\%\\.\\#b", "a\\$\\%\\^\\&b"));
        assertThat(SkippingStatsColumnsUtils.toSkippingStatsColumnsString(ImmutableSet.of("a\\@\\!\\#b", "a\\[\\]\\%\\.\\#b", "a\\$\\%\\^\\&b"))).isEqualTo("`a@!#b`,`a[]%.#b`,`a$%^&b`");

        assertThat(SkippingStatsColumnsUtils.getSkippingStatsColumns(Optional.of("`a.b.c`, `aa.b.c`, `a\\.b.c`, `a,b,c`, `a``b`")))
                .isEqualTo(ImmutableSet.of("a\\.b\\.c", "aa\\.b\\.c", "a\\\\\\.b\\.c", "a\\,b\\,c", "a`b"));
        assertThat(SkippingStatsColumnsUtils.toSkippingStatsColumnsString(ImmutableSet.of("a\\.b\\.c", "aa\\.b\\.c", "a\\\\\\.b\\.c", "a\\,b\\,c", "a`b")))
                .isEqualTo("`a.b.c`,`aa.b.c`,`a\\.b.c`,`a,b,c`,`a``b`");

        assertThat(SkippingStatsColumnsUtils.getSkippingStatsColumns(Optional.of("a.b,`a.b`,`a\\.b.c`,abc,`a,b,c`")))
                .isEqualTo(ImmutableSet.of("a.b", "a\\.b", "a\\\\\\.b\\.c", "abc", "a\\,b\\,c"));
        assertThat(SkippingStatsColumnsUtils.toSkippingStatsColumnsString(ImmutableSet.of("a.b", "a\\.b", "a\\\\\\.b\\.c", "abc", "a\\,b\\,c")))
                .isEqualTo("a.b,`a.b`,`a\\.b.c`,abc,`a,b,c`");

        assertThatCode(() -> SkippingStatsColumnsUtils.getSkippingStatsColumns(Optional.of("abc,a$b")))
                .hasMessage("Invalid name in delta.dataSkippingStatsColumns property: a$b");
        assertThatCode(() -> SkippingStatsColumnsUtils.getSkippingStatsColumns(Optional.of("abc,a\\!#b")))
                .hasMessage("Invalid name in delta.dataSkippingStatsColumns property: a\\!#b");

        assertThatCode(() -> SkippingStatsColumnsUtils.getSkippingStatsColumns(Optional.of("`ab")))
                .hasMessage("Invalid value for delta.dataSkippingStatsColumns property: `ab");
        assertThatCode(() -> SkippingStatsColumnsUtils.getSkippingStatsColumns(Optional.of("ab`")))
                .hasMessage("Invalid value for delta.dataSkippingStatsColumns property: ab`");
    }
}
