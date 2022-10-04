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
package io.trino.plugin.base.expression;

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.ConnectorExpression;

import java.util.Optional;

public interface ConnectorExpressionRule<ExpressionType extends ConnectorExpression, Result>
{
    Pattern<ExpressionType> getPattern();

    Optional<Result> rewrite(ExpressionType expression, Captures captures, RewriteContext<Result> context);

    default boolean appliesTo(Scope scope)
    {
        return true;
    }

    interface RewriteContext<Result>
    {
        ConnectorSession getSession();

        Optional<Result> defaultRewrite(ConnectorExpression expression);

        ConnectorExpressionRewriter.AssignmentResolver getResolver();

        default ColumnHandle getAssignment(String name)
        {
            return getResolver().getAssignment(name);
        }

        default Optional<String> getRelationAlias(String name)
        {
            return getResolver().getRelationAlias(name);
        }

        default Scope getScope()
        {
            return Scope.ANY;
        }
    }

    enum Scope
    {
        FILTER,
        JOIN,
        ANY;
    }
}
