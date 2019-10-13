package lab.nice.nifi.api.db;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.exception.ProcessException;

import java.sql.Connection;

@Tags({"dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service. Connections can be asked from pool and returned after usage.")
public interface DatabaseConnectionPoolService extends ControllerService {
    Connection getConnection() throws ProcessException;
}
