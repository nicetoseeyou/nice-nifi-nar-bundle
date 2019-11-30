### Description
Provides Database Connection Pooling Service. Connections can be asked from pool and returned after usage.

### Tags
HikariCP, dbcp, jdbc, database, connection, pooling

### Properties
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values, whether a property supports the [NiFi Expression Language](http://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html), and whether a property is considered "sensitive", meaning that its value will be encrypted. Before entering a value in a sensitive property, ensure that the _**nifi.properties**_ file has an entry for the property _**nifi.sensitive.props.key**_.

| Name | Default Value | Allowable Values | Description |
|:---|:---|:---|:---|
| Data Source Class Name | | | This is the name of the DataSource class provided by the JDBC driver. Consult the documentation for your specific JDBC driver to get this class name, or see the table below. Note XA data sources are not supported. XA requires a real transaction manager like bitronix. Note that you do not need this property if you are using jdbcUrl for "old-school" DriverManager-based JDBC driver configuration. |
| jdbcUrl | | | This property directs HikariCP to use "DriverManager-based" configuration. When using this property with "old" drivers, you may also need to set the driverClassName property, but try it first without. Note that if this property is used, you may still use DataSource properties to configure your driver and is in fact recommended over driver parameters specified in the URL itself. |
| **username** | | | This property sets the default authentication username used when obtaining Connections from the underlying driver |
| **password** | | | This property sets the default authentication password used when obtaining Connections from the underlying driver |
| **autoCommit** | true | true/false | This property controls the default auto-commit behavior of connections returned from the pool. It is a boolean value. |
| connectionTimeout | 30000 | | This property controls the maximum number of milliseconds that a client will wait for a connection from the pool. If this time is exceeded without a connection becoming available, a SQLException will be thrown. Lowest acceptable connection timeout is 250 ms. Default: 30000 (30 seconds). |
| idleTimeout | 600000 | | This property controls the maximum amount of time that a connection is allowed to sit idle in the pool. This setting only applies when minimumIdle is defined to be less than maximumPoolSize. Idle connections will not be retired once the pool reaches minimumIdle connections. Whether a connection is retired as idle or not is subject to a maximum variation of +30 seconds, and average variation of +15 seconds. A connection will never be retired as idle before this timeout. A value of 0 means that idle connections are never removed from the pool. The minimum allowed value is 10000ms (10 seconds). Default: 600000 (10 minutes) |
| maxLifetime | 1800000 | | This property controls the maximum lifetime of a connection in the pool. An in-use connection will never be retired, only when it is closed will it then be removed. On a connection-by-connection basis, minor negative attenuation is applied to avoid mass-extinction in the pool. We strongly recommend setting this value, and it should be several seconds shorter than any database or infrastructure imposed connection time limit. A value of 0 indicates no maximum lifetime (infinite lifetime), subject of course to the idleTimeout setting. Default: 1800000 (30 minutes) |
| connectionTestQuery | | | If your driver supports JDBC4 we strongly recommend not setting this property. This is for "legacy" drivers that do not support the JDBC4 Connection.isValid() API. This is the query that will be executed just before a connection is given to you from the pool to validate that the connection to the database is still alive. Again, try running the pool without this property, HikariCP will log an error if your driver is not JDBC4 compliant to let you know. Default: none |
| minimumIdle | | | This property controls the minimum number of idle connections that HikariCP tries to maintain in the pool. If the idle connections dip below this value and total connections in the pool are less than maximumPoolSize, HikariCP will make a best effort to add additional connections quickly and efficiently. However, for maximum performance and responsiveness to spike demands, we recommend not setting this value and instead allowing HikariCP to act as a fixed size connection pool. Default: same as maximumPoolSize |
| **maximumPoolSize** | 10 | | This property controls the maximum size that the pool is allowed to reach, including both idle and in-use connections. Basically this value will determine the maximum number of actual connections to the database backend. A reasonable value for this is best determined by your execution environment. When the pool reaches this size, and no idle connections are available, calls to getConnection() will block for up to connectionTimeout milliseconds before timing out. Please read about pool sizing. Default: 10 |
| poolName | | | This property represents a user-defined name for the connection pool and appears mainly in logging and JMX management consoles to identify pools and pool configurations. Default: auto-generated |
| initializationFailTimeout | | | This property controls whether the pool will "fail fast" if the pool cannot be seeded with an initial connection successfully. Any positive number is taken to be the number of milliseconds to attempt to acquire an initial connection; the application thread will be blocked during this period. If a connection cannot be acquired before this timeout occurs, an exception will be thrown. This timeout is applied after the connectionTimeout period. If the value is zero (0), HikariCP will attempt to obtain and validate a connection. If a connection is obtained, but fails validation, an exception will be thrown and the pool not started. However, if a connection cannot be obtained, the pool will start, but later efforts to obtain a connection may fail. A value less than zero will bypass any initial connection attempt, and the pool will start immediately while trying to obtain connections in the background. Consequently, later efforts to obtain a connection may fail. Default: 1|
| driverClassName | | | HikariCP will attempt to resolve a driver through the DriverManager based solely on the jdbcUrl, but for some older drivers the driverClassName must also be specified. Omit this property unless you get an obvious error message indicating that the driver was not found. Default: none|

### Dynamic Properties
| Name | Value | Description |
| :--- | :--- | :--- |
| JDBC property name | JDBC property value | Specifies a property name and value to be set on the JDBC connection(s). If Expression Language is used, evaluation will be performed upon the controller service being enabled. Note that no flow file input (attributes, e.g.) is available for use in Expression Language constructs for these properties. |

### State management
This component does not store state.
    
### Restricted
This component is not restricted.

### System Resource Considerations
None specified.