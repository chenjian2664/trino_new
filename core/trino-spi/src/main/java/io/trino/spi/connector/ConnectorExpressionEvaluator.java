/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.spi.connector;

import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.NullableValue;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface ConnectorExpressionEvaluator
{
    /**
     * No-op evaluator whose {@link Prepared} always returns {@link Optional#empty()},
     * indicating the expression cannot be evaluated. Connectors should treat this
     * as a conservative pass-through (keep all candidates).
     */
    ConnectorExpressionEvaluator NO_OP = (_, _) -> _ -> Optional.empty();

    /**
     * Prepares to evaluate {@code expression} for a given {@code session}. Any one-time
     * compilation cost (e.g. deserializing an opaque engine predicate) is paid here, once per
     * {@link io.trino.spi.connector.Constraint Constraint}. The returned {@link Prepared} handle
     * is then called once per candidate row or partition.
     */
    Prepared prepare(ConnectorExpression expression, ConnectorSession session);

    interface Prepared
    {
        /**
         * Evaluates the prepared expression against the given variable {@code bindings},
         * where each key is a variable name and each value is the current binding for
         * that variable.
         *
         * @return the result of evaluating the expression, or {@code Optional.empty()} if
         *         the expression cannot be evaluated (caller should retain the candidate).
         */
        Optional<Object> evaluate(Map<String, NullableValue> bindings);

        /**
         * Returns the variable names that must be supplied in the {@code bindings} map for
         * {@link #evaluate} to produce a definite result, or an empty set if no bindings are
         * needed (either because the expression is trivially true, or cannot be evaluated at all).
         * Callers may use this to decide which columns need to be read before calling {@link #evaluate}.
         */
        default Set<String> getArguments()
        {
            return Set.of();
        }
    }
}
