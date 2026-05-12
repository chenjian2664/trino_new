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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.operator.scalar.TryFunction;
import io.trino.spi.connector.ConnectorExpressionEvaluator;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.NullableValue;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Booleans;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class InternalConnectorExpressionEvaluator
        implements ConnectorExpressionEvaluator
{
    public static final FunctionName ENGINE_PREDICATE_FUNCTION_NAME = new FunctionName("$engine_predicate");

    private final IrExpressionOptimizer evaluator;
    private final JsonCodec<Expression> enginePredicateCodec;

    @Inject
    public InternalConnectorExpressionEvaluator(PlannerContext plannerContext, JsonCodec<Expression> enginePredicateCodec)
    {
        this.evaluator = plannerContext.getPartialEvaluator();
        this.enginePredicateCodec = requireNonNull(enginePredicateCodec, "enginePredicateCodec is null");
    }

    @Override
    public Prepared prepare(ConnectorExpression expression, ConnectorSession session)
    {
        return new PreparedExpression(expression, session);
    }

    /**
     * Wraps {@code predicate} as an opaque {@code $engine_predicate} call and ANDs it with
     * {@code connectorExpression}, producing the full expression to place in a {@link io.trino.spi.connector.Constraint}.
     * The connector passes the result back via {@link #prepare} so the engine can evaluate it
     * at partition/split pruning time.
     */
    public static ConnectorExpression buildEnginePredicateExpression(
            ConnectorExpression connectorExpression,
            Expression predicate,
            JsonCodec<Expression> expressionCodec)
    {
        List<Variable> predicateVariables = SymbolsExtractor.extractUnique(predicate).stream()
                .map(symbol -> new Variable(symbol.name(), symbol.type()))
                .collect(toImmutableList());
        ConnectorExpression enginePredicateCall = new Call(
                BOOLEAN,
                ENGINE_PREDICATE_FUNCTION_NAME,
                ImmutableList.<ConnectorExpression>builder()
                        .add(new Constant(Slices.utf8Slice(expressionCodec.toJson(predicate)), VARCHAR))
                        .addAll(predicateVariables)
                        .build());
        return Constant.TRUE.equals(connectorExpression)
                ? enginePredicateCall
                : new Call(BOOLEAN, AND_FUNCTION_NAME, ImmutableList.of(connectorExpression, enginePredicateCall));
    }

    private final class PreparedExpression
            implements Prepared
    {
        private final ConnectorExpression expression;
        private final Session session;
        private final Optional<Expression> enginePredicate;
        private final Set<Symbol> argumentSymbols;

        PreparedExpression(ConnectorExpression expression, ConnectorSession connectorSession)
        {
            this.expression = requireNonNull(expression, "expression is null");
            this.session = ((FullConnectorSession) requireNonNull(connectorSession, "connectorSession is null")).getSession();
            this.enginePredicate = findEnginePredicate(expression, enginePredicateCodec);
            this.argumentSymbols = enginePredicate
                    .map(SymbolsExtractor::extractUnique)
                    .orElse(ImmutableSet.of());
        }

        @Override
        public Set<String> getArguments()
        {
            return argumentSymbols.stream()
                    .map(Symbol::name)
                    .collect(toImmutableSet());
        }

        @Override
        public Optional<Object> evaluate(Map<String, NullableValue> bindings)
        {
            return evaluateExpression(expression, bindings).map(Function.identity());
        }

        private Optional<Boolean> evaluateExpression(ConnectorExpression expression, Map<String, NullableValue> bindings)
        {
            if (expression instanceof Constant constant) {
                return Optional.of(Boolean.TRUE.equals(constant.getValue()));
            }
            if (expression instanceof Call call) {
                if (call.getFunctionName().equals(AND_FUNCTION_NAME)) {
                    return evaluateAnd(call.getArguments(), bindings);
                }
                if (call.getFunctionName().equals(ENGINE_PREDICATE_FUNCTION_NAME)) {
                    checkState(enginePredicate.isPresent(), "engine predicate is absent");
                    return evaluateEnginePredicate(bindings, enginePredicate.get());
                }
            }
            return Optional.empty();
        }

        private Optional<Boolean> evaluateAnd(List<ConnectorExpression> args, Map<String, NullableValue> bindings)
        {
            boolean hasUnknown = false;
            for (ConnectorExpression arg : args) {
                Optional<Boolean> result = evaluateExpression(arg, bindings);
                if (result.isEmpty()) {
                    hasUnknown = true;
                }
                else if (!result.get()) {
                    return Optional.of(false);
                }
            }
            return hasUnknown ? Optional.empty() : Optional.of(true);
        }

        private Optional<Boolean> evaluateEnginePredicate(Map<String, NullableValue> bindings, Expression enginePredicate)
        {
            if (intersection(bindings.keySet(), getArguments()).isEmpty()) {
                return Optional.of(true);
            }

            Map<Symbol, Expression> inputs = argumentSymbols.stream()
                    .filter(symbol -> bindings.containsKey(symbol.name()))
                    .collect(Collectors.toMap(
                            Function.identity(),
                            symbol -> {
                                NullableValue value = bindings.get(symbol.name());
                                return new io.trino.sql.ir.Constant(symbol.type(), value.getValue());
                            }));

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            Expression optimized = TryFunction.evaluate(() -> evaluator.process(enginePredicate, session, inputs).orElse(enginePredicate), Booleans.TRUE);

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            return Optional.of(!(optimized instanceof io.trino.sql.ir.Constant constant) || Boolean.TRUE.equals(constant.value()));
        }

        /**
         * Walks {@code expression} to find the {@code $engine_predicate} call and eagerly
         * deserializes its JSON payload into an IR {@link Expression}.
         * Asserts that at most one {@code $engine_predicate} conjunct is present.
         */
        private static Optional<Expression> findEnginePredicate(ConnectorExpression expression, JsonCodec<Expression> enginePredicateCodec)
        {
            if (expression instanceof Call call) {
                if (call.getFunctionName().equals(ENGINE_PREDICATE_FUNCTION_NAME)) {
                    Slice payload = (Slice) requireNonNull(
                            ((Constant) call.getArguments().getFirst()).getValue(),
                            "engine predicate payload is null");
                    return Optional.of(enginePredicateCodec.fromJson(payload.toStringUtf8()));
                }
                if (call.getFunctionName().equals(AND_FUNCTION_NAME)) {
                    Optional<Expression> result = Optional.empty();
                    for (ConnectorExpression arg : call.getArguments()) {
                        Optional<Expression> found = findEnginePredicate(arg, enginePredicateCodec);
                        if (found.isPresent()) {
                            checkState(result.isEmpty(), "Multiple $engine_predicate conjuncts found in expression");
                            result = found;
                        }
                    }
                    return result;
                }
            }
            return Optional.empty();
        }
    }
}
