package lab.nice.nifi.processor.db.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public final class JdbcDummy {
    private JdbcDummy() {
    }

    public static void execute(final Connection connection, final String sql) throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    public static void executeBatch(final Connection connection, final String sql, final List<Map<Integer, Object>> parameterList) throws SQLException {
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
            if (parameterList != null && !parameterList.isEmpty()) {
                for (int i = 0; i < parameterList.size(); i++) {
                    final Map<Integer, Object> parameters = parameterList.get(i);
                    if (parameters != null && !parameters.isEmpty()) {
                        for (Map.Entry<Integer, Object> parameter : parameters.entrySet()) {
                            statement.setObject(parameter.getKey(), parameter.getValue());
                        }
                        statement.addBatch();
                    }
                }
            }
            statement.executeBatch();
        }
    }
}
