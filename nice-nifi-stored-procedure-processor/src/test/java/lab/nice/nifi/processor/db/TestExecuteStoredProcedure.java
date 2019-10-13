package lab.nice.nifi.processor.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lab.nice.nifi.api.db.DatabaseConnectionPoolService;
import lab.nice.nifi.processor.db.util.JdbcDummy;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestExecuteStoredProcedure {
    private static final Logger LOGGER;

    private static final String TEST_TABLE_CUSTOMERS_CREATION = "CREATE TABLE IF NOT EXISTS customers (id INTEGER GENERATED ALWAYS AS IDENTITY(START WITH 1) PRIMARY KEY, name VARCHAR(255), age INTEGER)";
    private static final String TEST_TABLE_CUSTOMERS_INSERT = "INSERT INTO customers (name, age) VALUES (NULL, NULL)";
    private static final String TEST_TABLE_CUSTOMERS_DROP = "DROP TABLE customers";

    private static final String TEST_TABLE_CITY_CREATION = "CREATE TABLE IF NOT EXISTS address (id INTEGER PRIMARY KEY, city VARCHAR(255))";
    private static final String TEST_TABLE_CITY_DROP = "DROP TABLE address";

    private static final String TEST_TABLE_LOBS_CREATION = "CREATE TABLE IF NOT EXISTS all_lobs (id INTEGER GENERATED ALWAYS AS IDENTITY(START WITH 1) PRIMARY KEY, v_clob CLOB)";
    private static final String TEST_TABLE_LOBS_INSERT = "INSERT INTO all_lobs (v_clob) VALUES ('lab.nice.nifi.invoker.TestExecuteStoredProcedure')";
    private static final String TEST_TABLE_LOBS_DROP = "DROP TABLE all_lobs";

    private static final String TEST_PROCEDURE_CREATION = "CREATE PROCEDURE two_res_new_customer (" +
            "IN i_name VARCHAR(255), " +
            "IN i_age INTEGER, " +
            "IN i_city VARCHAR(255), " +
            "OUT o_id INTEGER ) " +
            "MODIFIES SQL DATA " +
            "DYNAMIC RESULT SETS 3 " +
            "BEGIN ATOMIC " +
            "DECLARE temp_id INTEGER; " +
            "INSERT INTO customers (name, age) VALUES (i_name, i_age); " +
            "SET temp_id = IDENTITY(); " +
            "SET o_id = temp_id; " +
            "INSERT INTO address (id, city) VALUES (temp_id, i_city); " +
            "INSERT INTO all_lobs (v_clob) VALUES ('lab.nice.nifi.invoker.ExecuteStoredProcedure'); "+
            "BEGIN ATOMIC " +
            "DECLARE cus_res CURSOR WITH RETURN FOR SELECT * FROM customers FOR READ ONLY; " +
            "DECLARE address_res CURSOR WITH RETURN FOR SELECT * FROM address FOR READ ONLY; " +
            "DECLARE lobs_res CURSOR WITH RETURN FOR SELECT * FROM all_lobs FOR READ ONLY; " +
            "OPEN cus_res; " +
            "OPEN address_res; " +
            "OPEN lobs_res; " +
            "END; " +
            "END";
    private static final String TEST_PROCEDURE_CALL = "{CALL two_res_new_customer(?,?,?,?)}";
    private static final String TEST_PROCEDURE_DROP = "DROP PROCEDURE two_res_new_customer";

    static {
        LOGGER = LoggerFactory.getLogger(TestExecuteStoredProcedure.class);
    }

    private TestRunner runner;

    @BeforeClass
    public static void setupClass() {
        //System.setProperty("derby.stream.error.file", "target/derby.log");
    }

    @Before
    public void setUp() throws InitializationException, SQLException {
        final DatabaseConnectionPoolService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        // load test data to database
        final Connection connection = dbcp.getConnection();
        JdbcDummy.execute(connection, TEST_TABLE_CITY_CREATION);
        JdbcDummy.execute(connection, TEST_TABLE_CUSTOMERS_CREATION);
        JdbcDummy.execute(connection, TEST_TABLE_CUSTOMERS_INSERT);
        JdbcDummy.execute(connection, TEST_TABLE_LOBS_CREATION);
        JdbcDummy.execute(connection, TEST_TABLE_LOBS_INSERT);
        JdbcDummy.execute(connection, TEST_PROCEDURE_CREATION);

        LOGGER.info("test data loaded");

        runner = TestRunners.newTestRunner(ExecuteStoredProcedure.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(ExecuteStoredProcedure.DBCP_SERVICE, "dbcp");
    }

    @After
    public void tearDown() throws SQLException {
        final Connection connection = ((DatabaseConnectionPoolService) runner.getControllerService("dbcp")).getConnection();
        JdbcDummy.execute(connection, TEST_PROCEDURE_DROP);
        JdbcDummy.execute(connection, TEST_TABLE_CUSTOMERS_DROP);
        JdbcDummy.execute(connection, TEST_TABLE_CITY_DROP);
        JdbcDummy.execute(connection, TEST_TABLE_LOBS_DROP);
    }

    @Test
    public void testNoIncoming() throws IOException {
        runner.setIncomingConnection(false);
        runner.setProperty(ExecuteStoredProcedure.STORED_PROCEDURE_STATEMENT, TEST_PROCEDURE_CALL);
        runner.setProperty("procedure.args.in.1.type", "12");
        runner.setProperty("procedure.args.in.1.value", "Tom");
        runner.setProperty("procedure.args.in.2.type", "4");
        runner.setProperty("procedure.args.in.2.value", "20");
        runner.setProperty("procedure.args.in.3.type", "12");
        runner.setProperty("procedure.args.in.3.value", "Guangzhou");
        runner.setProperty("procedure.args.out.4.type", "4");
        runner.setProperty("procedure.args.out.4.name", "ID");
        invokeOnTrigger(null, null, false, null, false);
    }

    public void invokeOnTrigger(final Integer queryTimeout, final String query, final boolean incomingFlowFile, final Map<String, String> attrs, final boolean setQueryProperty)
            throws IOException {

        if (queryTimeout != null) {
            runner.setProperty(ExecuteStoredProcedure.PROCEDURE_EXECUTION_TIMEOUT, queryTimeout.toString() + " secs");
        }

        if (incomingFlowFile) {
            // incoming FlowFile content is not used, but attributes are used
            final Map<String, String> attributes = (attrs == null) ? new HashMap<>() : attrs;
            if (!setQueryProperty) {
                runner.enqueue(query.getBytes(), attributes);
            } else {
                runner.enqueue("Hello".getBytes(), attributes);
            }
        }

        if (setQueryProperty) {
            runner.setProperty(ExecuteStoredProcedure.STORED_PROCEDURE_STATEMENT, query);
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(ExecuteStoredProcedure.REL_SUCCESS, 1);
        runner.assertAllFlowFilesContainAttribute(ExecuteStoredProcedure.REL_SUCCESS, ExecuteStoredProcedure.PROCEDURE_EXECUTE_DURATION);

        final List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ExecuteStoredProcedure.REL_SUCCESS);

        final ObjectMapper objectMapper = new ObjectMapper();
        for (MockFlowFile flowFile : flowFiles) {
            final JsonNode root = objectMapper.readTree(flowFile.toByteArray());
            LOGGER.info("{}", root);
        }
    }

    /**
     * Simple implementation only for ExecuteSQL processor testing.
     */
    class DBCPServiceSimpleImpl extends AbstractControllerService implements DatabaseConnectionPoolService {

        @Override
        public String getIdentifier() {
            return "dbcp";
        }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.hsqldb.jdbc.JDBCDriver");
                final Connection con = DriverManager.getConnection("jdbc:hsqldb:mem:test", "test", "");
                return con;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
    }

    static class WrapInt {
        private final String name;
        private int value = 0;

        WrapInt(final String name) {
            this.name = name;
        }

        void increase() {
            this.value++;
        }

        String getName() {
            return name;
        }

        int getValue() {
            return value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            WrapInt wrapInt = (WrapInt) o;
            return value == wrapInt.value &&
                    Objects.equals(name, wrapInt.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, value);
        }

        @Override
        public String toString() {
            return "{" +
                    "name='" + name + '\'' +
                    ", value=" + value +
                    '}';
        }
    }
}
