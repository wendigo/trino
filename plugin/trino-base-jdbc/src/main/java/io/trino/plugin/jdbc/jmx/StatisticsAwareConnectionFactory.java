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
package io.trino.plugin.jdbc.jmx;

import io.trino.plugin.base.inject.Decorator;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.ForwardingConnection;
import io.trino.plugin.jdbc.ForwardingConnectionFactory;
import io.trino.spi.connector.ConnectorSession;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.Objects.requireNonNull;

public class StatisticsAwareConnectionFactory
        extends ForwardingConnectionFactory
{
    private final ConnectionFactory delegate;
    private final JdbcApiStats openConnection = new JdbcApiStats();
    private final JdbcApiStats closeConnection = new JdbcApiStats();

    public StatisticsAwareConnectionFactory(ConnectionFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    protected ConnectionFactory delegate()
    {
        return delegate;
    }

    @Override
    public Connection openConnection(ConnectorSession session)
            throws SQLException
    {
        return openConnection.wrap(() -> new CloseTrackingConnection(closeConnection, delegate.openConnection(session)));
    }

    @Managed
    @Nested
    public JdbcApiStats getOpenConnection()
    {
        return openConnection;
    }

    @Managed
    @Nested
    public JdbcApiStats getCloseConnection()
    {
        return closeConnection;
    }

    private static class CloseTrackingConnection
            extends ForwardingConnection
    {
        private final JdbcApiStats closeConnection;
        private final Connection connection;

        private CloseTrackingConnection(JdbcApiStats closeConnection, Connection connection)
        {
            this.closeConnection = requireNonNull(closeConnection, "closeConnection is null");
            this.connection = requireNonNull(connection, "connection is null");
        }

        @Override
        public void close()
                throws SQLException
        {
            closeConnection.wrap(connection::close);
        }

        @Override
        protected Connection delegate()
        {
            return connection;
        }
    }

    public static class FactoryDecorator
            implements Decorator<ConnectionFactory>
    {
        public static final int STATISTICS_PRIORITY = 1;

        @Override
        public int priority()
        {
            // Lowest priority wraps around the raw instance
            return STATISTICS_PRIORITY;
        }

        @Override
        public ConnectionFactory apply(ConnectionFactory delegate)
        {
            return new StatisticsAwareConnectionFactory(delegate);
        }
    }
}
