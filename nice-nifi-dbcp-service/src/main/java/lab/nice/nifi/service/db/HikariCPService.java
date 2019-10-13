package lab.nice.nifi.service.db;

import lab.nice.nifi.api.db.DatabaseConnectionPoolService;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;

import java.sql.Connection;

/**
 * Implementation of for Database Connection Pooling Service. HikariCP is used for connection pooling functionality.
 */
@Tags({"HikariCP", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service. Connections can be asked from pool and returned after usage.")
@DynamicProperty(name = "JDBC property name", value = "JDBC property value",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Specifies a property name and value to be set on the JDBC connection(s). "
                + "If Expression Language is used, evaluation will be performed upon the controller service being enabled. "
                + "Note that no flow file input (attributes, e.g.) is available for use in Expression Language constructs for these properties.")
public class HikariCPService extends AbstractControllerService implements DatabaseConnectionPoolService {
    @Override
    public Connection getConnection() throws ProcessException {
        return null;
    }
}
