package lab.nice.nifi.service.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lab.nice.nifi.api.db.DatabaseConnectionPoolService;
import lab.nice.nifi.util.PropertyConstant;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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


    public static final PropertyDescriptor DATA_SOURCE_CLASS_NAME = new PropertyDescriptor.Builder()
            .name("dataSourceClassName")
            .displayName("Data Source Class Name")
            .description("This is the name of the DataSource class provided by the JDBC driver. " +
                    "Consult the documentation for your specific JDBC driver to get this class name, " +
                    "or see the table below. Note XA data sources are not supported. XA requires a real " +
                    "transaction manager like bitronix. Note that you do not need this property if you are using " +
                    "jdbcUrl for \"old-school\" DriverManager-based JDBC driver configuration.")
            .required(false)
            .defaultValue(null)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor JDBC_URL = new PropertyDescriptor.Builder()
            .name("jdbcUrl")
            .displayName("jdbcUrl")
            .description("This property directs HikariCP to use \"DriverManager-based\" configuration. " +
                    "When using this property with \"old\" drivers, you may also need to set the driverClassName property, " +
                    "but try it first without. Note that if this property is used, you may still use DataSource properties " +
                    "to configure your driver and is in fact recommended over driver parameters specified in the URL itself.")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("username")
            .displayName("username")
            .description("This property sets the default authentication username used " +
                    "when obtaining Connections from the underlying driver.")
            .defaultValue(null)
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("password")
            .description("This property sets the default authentication password used " +
                    "when obtaining Connections from the underlying driver.")
            .defaultValue(null)
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor AUTO_COMMIT = new PropertyDescriptor.Builder()
            .name("autoCommit")
            .displayName("autoCommit")
            .description("This property controls the default auto-commit behavior of connections " +
                    "returned from the pool. It is a boolean value. ")
            .defaultValue(PropertyConstant.BOOLEAN_YES.getValue())
            .allowableValues(PropertyConstant.BOOLEAN_YES, PropertyConstant.BOOLEAN_NO)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("connectionTimeout")
            .displayName("connectionTimeout")
            .description("This property controls the maximum number of milliseconds that a client " +
                    "will wait for a connection from the pool. If this time is exceeded without " +
                    "a connection becoming available, a SQLException will be thrown. Lowest acceptable " +
                    "connection timeout is 250 ms. Default: 30000 (30 seconds).")
            .defaultValue("30000")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(250L, Long.MAX_VALUE, true))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor IDLE_TIMEOUT = new PropertyDescriptor.Builder()
            .name("idleTimeout")
            .displayName("idleTimeout")
            .description("This property controls the maximum amount of time that a connection is allowed " +
                    "to sit idle in the pool. This setting only applies when minimumIdle is defined to be " +
                    "less than maximumPoolSize. Idle connections will not be retired once the pool reaches " +
                    "minimumIdle connections. Whether a connection is retired as idle or not is subject to " +
                    "a maximum variation of +30 seconds, and average variation of +15 seconds. A connection " +
                    "will never be retired as idle before this timeout. A value of 0 means that idle connections " +
                    "are never removed from the pool. The minimum allowed value is 10000ms (10 seconds). " +
                    "Default: 600000 (10 minutes)")
            .defaultValue("600000")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(10000L, Long.MAX_VALUE, true))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_LIFE_TIME = new PropertyDescriptor.Builder()
            .name("maxLifetime")
            .displayName("maxLifetime")
            .description("This property controls the maximum lifetime of a connection in the pool. " +
                    "An in-use connection will never be retired, only when it is closed will it then be removed. " +
                    "On a connection-by-connection basis, minor negative attenuation is applied to avoid " +
                    "mass-extinction in the pool. We strongly recommend setting this value, and it should be " +
                    "several seconds shorter than any database or infrastructure imposed connection time limit. " +
                    "A value of 0 indicates no maximum lifetime (infinite lifetime), subject of course to the " +
                    "idleTimeout setting. Default: 1800000 (30 minutes)")
            .defaultValue("1800000")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(0, Long.MAX_VALUE, true))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor CONNECTION_TEST_QUERY = new PropertyDescriptor.Builder()
            .name("connectionTestQuery")
            .displayName("connectionTestQuery")
            .description("If your driver supports JDBC4 we strongly recommend not setting this property. " +
                    "This is for \"legacy\" drivers that do not support the JDBC4 Connection.isValid() API. " +
                    "This is the query that will be executed just before a connection is given to you from " +
                    "the pool to validate that the connection to the database is still alive. Again, " +
                    "try running the pool without this property, HikariCP will log an error if your driver " +
                    "is not JDBC4 compliant to let you know. Default: none")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MINIMUM_IDLE = new PropertyDescriptor.Builder()
            .name("minimumIdle")
            .displayName("minimumIdle")
            .description("This property controls the minimum number of idle connections that HikariCP " +
                    "tries to maintain in the pool. If the idle connections dip below this value and " +
                    "total connections in the pool are less than maximumPoolSize, HikariCP will make " +
                    "a best effort to add additional connections quickly and efficiently. However, " +
                    "for maximum performance and responsiveness to spike demands, we recommend not setting " +
                    "this value and instead allowing HikariCP to act as a fixed size connection pool. " +
                    "Default: same as maximumPoolSize")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAXIMUM_POOL_SIZE = new PropertyDescriptor.Builder()
            .name("maximumPoolSize")
            .displayName("maximumPoolSize")
            .description("This property controls the maximum size that the pool is allowed to reach, " +
                    "including both idle and in-use connections. Basically this value will determine " +
                    "the maximum number of actual connections to the database backend. A reasonable " +
                    "value for this is best determined by your execution environment. When the pool " +
                    "reaches this size, and no idle connections are available, calls to getConnection() " +
                    "will block for up to connectionTimeout milliseconds before timing out. " +
                    "Please read about pool sizing. Default: 10")
            .defaultValue("10")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor POOL_NAME = new PropertyDescriptor.Builder()
            .name("poolName")
            .displayName("poolName")
            .description("This property represents a user-defined name for the connection pool and appears " +
                    "mainly in logging and JMX management consoles to identify pools and pool configurations. " +
                    "Default: auto-generated")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor INITIALIZATION_FAIL_TIMEOUT = new PropertyDescriptor.Builder()
            .name("initializationFailTimeout")
            .displayName("initializationFailTimeout")
            .description("This property controls whether the pool will \"fail fast\" if the pool cannot " +
                    "be seeded with an initial connection successfully. Any positive number is taken to be " +
                    "the number of milliseconds to attempt to acquire an initial connection; the application " +
                    "thread will be blocked during this period. If a connection cannot be acquired before " +
                    "this timeout occurs, an exception will be thrown. This timeout is applied after " +
                    "the connectionTimeout period. If the value is zero (0), HikariCP will attempt to obtain " +
                    "and validate a connection. If a connection is obtained, but fails validation, an exception " +
                    "will be thrown and the pool not started. However, if a connection cannot be obtained, " +
                    "the pool will start, but later efforts to obtain a connection may fail. A value less than " +
                    "zero will bypass any initial connection attempt, and the pool will start immediately while " +
                    "trying to obtain connections in the background. Consequently, later efforts to obtain a " +
                    "connection may fail. Default: 1")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();


    public static final PropertyDescriptor DRIVER_CLASS_NAME = new PropertyDescriptor.Builder()
            .name("driverClassName")
            .displayName("driverClassName")
            .description("HikariCP will attempt to resolve a driver through the DriverManager based solely " +
                    "on the jdbcUrl, but for some older drivers the driverClassName must also be specified. " +
                    "Omit this property unless you get an obvious error message indicating that the driver " +
                    "was not found. Default: none")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> supportedProperties;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DATA_SOURCE_CLASS_NAME);
        properties.add(JDBC_URL);
        properties.add(USERNAME);
        properties.add(PASSWORD);
        properties.add(AUTO_COMMIT);
        properties.add(CONNECTION_TIMEOUT);
        properties.add(IDLE_TIMEOUT);
        properties.add(MAX_LIFE_TIME);
        properties.add(CONNECTION_TEST_QUERY);
        properties.add(MINIMUM_IDLE);
        properties.add(MAXIMUM_POOL_SIZE);
        properties.add(POOL_NAME);
        properties.add(INITIALIZATION_FAIL_TIMEOUT);
        properties.add(DRIVER_CLASS_NAME);
        supportedProperties = Collections.unmodifiableList(properties);
    }

    private volatile HikariDataSource hikariDataSource;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return supportedProperties;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
    }

    /**
     * Configures connection pool by creating an instance of the
     * {@link HikariDataSource} based on configuration provided with
     * {@link ConfigurationContext}.
     * <p>
     * This operation makes no guarantees that the actual connection could be
     * made since the underlying system may still go off-line during normal
     * operation of the connection pool.
     *
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        final HikariConfig config = new HikariConfig();
        applyHikariConfig(context, config);
        applyDynamicConfig(context, config);
        hikariDataSource = new HikariDataSource(config);
    }

    private void applyHikariConfig(final ConfigurationContext context, final HikariConfig hikariConfig) throws InitializationException {
        final String dataSourceClass = context.getProperty(DATA_SOURCE_CLASS_NAME).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(dataSourceClass)) {
            hikariConfig.setDataSourceClassName(dataSourceClass);
        }

        final String jdbcUrl = context.getProperty(JDBC_URL).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(jdbcUrl)) {
            hikariConfig.setJdbcUrl(jdbcUrl);
        }

        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(username)) {
            hikariConfig.setUsername(username);
        } else {
            throw new InitializationException("username not set.");
        }

        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(password)) {
            hikariConfig.setPassword(password);
        } else {
            throw new InitializationException("password not set.");
        }

        final Boolean autoCommit = context.getProperty(AUTO_COMMIT).asBoolean();
        if (null != autoCommit) {
            hikariConfig.setAutoCommit(autoCommit);
        }

        final Long connectionTimeout = context.getProperty(CONNECTION_TIMEOUT).asLong();
        if (null != connectionTimeout) {
            hikariConfig.setConnectionTimeout(connectionTimeout);
        }

        final Long idleTimeout = context.getProperty(IDLE_TIMEOUT).asLong();
        if (null != idleTimeout) {
            hikariConfig.setIdleTimeout(idleTimeout);
        }

        final Long maxLifetime = context.getProperty(MAX_LIFE_TIME).asLong();
        if (null != maxLifetime) {
            hikariConfig.setMaxLifetime(maxLifetime);
        }

        final String connectionTestQuery = context.getProperty(CONNECTION_TEST_QUERY).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(connectionTestQuery)) {
            hikariConfig.setConnectionTestQuery(connectionTestQuery);
        }

        final Integer minIdle = context.getProperty(MINIMUM_IDLE).evaluateAttributeExpressions().asInteger();
        if (null != minIdle) {
            hikariConfig.setMinimumIdle(minIdle);
        }

        final Integer maxPoolSize = context.getProperty(MAXIMUM_POOL_SIZE).asInteger();
        if (null != maxPoolSize) {
            hikariConfig.setMaximumPoolSize(maxPoolSize);
        }

        final String poolName = context.getProperty(POOL_NAME).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(poolName)) {
            hikariConfig.setPoolName(poolName);
        }

        final Long initializationFailTimeout = context.getProperty(INITIALIZATION_FAIL_TIMEOUT).evaluateAttributeExpressions().asLong();
        if (null != initializationFailTimeout) {
            hikariConfig.setInitializationFailTimeout(initializationFailTimeout);
        }

        final String driverClass = context.getProperty(DRIVER_CLASS_NAME).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(driverClass)) {
            hikariConfig.setDriverClassName(driverClass);
        }
        try{
            hikariConfig.validate();
        }catch (Exception e){
            throw new InitializationException(e);
        }
    }

    private void applyDynamicConfig(final ConfigurationContext context, final HikariConfig hikariConfig) throws InitializationException {
        for (Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor propertyDescriptor = entry.getKey();
            if (propertyDescriptor.isDynamic()) {
                if (propertyDescriptor.isExpressionLanguageSupported()) {
                    hikariConfig.addDataSourceProperty(propertyDescriptor.getName(),
                            context.getProperty(propertyDescriptor).evaluateAttributeExpressions().getValue());
                } else {
                    hikariConfig.addDataSourceProperty(propertyDescriptor.getName(), entry.getValue());
                }
            }
        }
        try{
            hikariConfig.validate();
        }catch (Exception e){
            throw new InitializationException(e);
        }
    }

    /**
     * Shutdown pool, close all open connections.
     */
    @OnDisabled
    public void shutdown() {
        hikariDataSource.close();
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            return hikariDataSource.getConnection();
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public String toString() {
        return "HikariCP[id=" + getIdentifier() + "]";
    }
}
