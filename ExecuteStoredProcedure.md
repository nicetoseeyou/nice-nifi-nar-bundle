### Description
Executes provided stored procedure. 
Stored procedure output (if any) will be returned and converted to JSON format. 
Streaming is used so arbitrarily large result sets are supported. 
This processor can be scheduled to run on a timer, or cron expression, using the standard scheduling methods, 
or it can be triggered by an incoming FlowFile. 
If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating 
the stored procedure call statement, and the call statement may use ? to to escape parameters. 
In this case, the parameters to use must exist as FlowFile attributes with the naming convention 
procedure.args.(in|out|inout).N.(type|value|name|format), where N is a positive integer. 
The procedure.args.N.type is expected to be a number indicating the JDBC Type. 
The content of the FlowFile is expected to be in UTF-8 format. 
FlowFile attribute 'procedure.execute.duration' indicates how long the execution is took. 

### Tags
procedure, execute, rdbms, database

### Properties
In the list below, the names of required properties appear in **bold**. Any other properties (not in bold) are considered optional. The table also indicates any default values, and whether a property supports the [NiFi Expression Language](http://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html).      

| Name | Default Value | Allowable Values | Description |
| :--- | :--- | :--- | :--- |
| **Database Connection Pooling Service** | | **Controller Service API:** <br> DatabaseConnectionPoolService <br> **Implementations:** <br> HikariCPService | The Controller Service that is used to obtain connection to database |
| Stored Procedure Statement | | | The stored procedure statement to execute. The statement can be empty, a constant value, or built from attributes using Expression Language. If this property is specified, it will be used regardless of the content of incoming FlowFile. If this property is empty, the attributes of the incoming FlowFile is expected to contain an attribute 'stored.procedure.statement' with a valid stored procedure statement. <br> **Supports Expression Language: true (will be evaluated using flow file attributes and variable registry)** |
| Max Wait Time in Seconds | 0 seconds | | The maximum amount of time allowed for a running stored procedure statement, zero means there is no limit. Max time less than 1 second will be equal to zero. |

### Relationships
| Name | Description |
| :--- | :---|
| success | Successfully created FlowFile from stored procedure execution return result. |
| failure | Stored procedure execution failed. Incoming FlowFile will be penalized and routed to this relationship |

### Reads Attributes
| Name | Description |
| :--- | :--- |
| procedure.args.in.N.type | IN argument type for parametrized stored procedure statement. The type of each Parameter is specified as an integer that represents the JDBC Type of the parameter. |
| procedure.args.in.N.value | IN argument value for parametrized stored procedure statement. The value of the Parameters are specified as _procedure.args.in.1.value_, _procedure.args.in.2.value_ and so on. The type of the _procedure.args.in.1.value_ Parameter is specified by the _procedure.args.in.1.type_ attribute. |
| procedure.args.in.N.format | This attribute is always optional, but default options may not always work for your data. Incoming FlowFiles are expected to be parametrized SQL statements. In some cases a format option needs to be specified, currently this is only applicable for binary data types, dates, times and timestamps. Binary Data Types (defaults to 'ascii') - ascii: each string character in your attribute value represents a single byte. This is the format provided by Avro Processors. base64: the string is a Base64 encoded string that can be decoded to bytes. hex: the string is hex encoded with all letters in upper case and no '0x' at the beginning. Dates/Times/Timestamps - Date, Time and Timestamp formats all support both custom formats or named format ('yyyy-MM-dd','ISO_OFFSET_DATE_TIME') as specified according to java.time.format.DateTimeFormatter. If not specified, a long value input is expected to be an unix epoch (milli seconds from 1970/1/1), or a string value in 'yyyy-MM-dd' format for Date, 'HH:mm:ss.SSS' for Time (some database engines e.g. Derby or MySQL do not support milliseconds and will truncate milliseconds), 'yyyy-MM-dd HH:mm:ss.SSS' for Timestamp is used. |
| procedure.args.out.N.type | OUT argument type for parametrized stored procedure statement. The type of each Parameter is specified as an integer that represents the JDBC Type of the parameter. |
| procedure.args.out.N.name | OUT argument name for parametrized stored procedure statement. The value of the Parameters are specified as procedure.args.out.1.name, procedure.args.out.2.name and so on. The return value of the procedure.args.out.1.type Parameter is named by the procedure.args.out.1.name attribute. This attribute would be used to named the stored procedure output value. |
| procedure.args.inout.N.type | INOUT argument type for parametrized stored procedure statement. The type of each Parameter is specified as an integer that represents the JDBC Type of the parameter. |
| procedure.args.inout.N.value | INOUT argument value for parametrized stored procedure statement. The value of the Parameters are specified as procedure.args.inout.1.value, procedure.args.inout.2.value and so on. The type of the procedure.args.inout.1.value Parameter is specified by the procedure.args.inout.1.type attribute. |
| procedure.args.inout.N.name | INOUT argument name for parametrized stored procedure statement. The value of the Parameters are specified as procedure.args.inout.1.name, procedure.args.inout.2.name and so on. The return value of the procedure.args.inout.1.type Parameter is named by the procedure.args.inout.1.name attribute. This attribute would be used to named the stored procedure output value. |
| procedure.args.inout.N.format | This attribute is always optional, but default options may not always work for your data. Incoming FlowFiles are expected to be parametrized SQL statements. In some cases a format option needs to be specified, currently this is only applicable for binary data types, dates, times and timestamps. Binary Data Types (defaults to 'ascii') - ascii: each string character in your attribute value represents a single byte. This is the format provided by Avro Processors. base64: the string is a Base64 encoded string that can be decoded to bytes. hex: the string is hex encoded with all letters in upper case and no '0x' at the beginning. Dates/Times/Timestamps - Date, Time and Timestamp formats all support both custom formats or named format ('yyyy-MM-dd','ISO_OFFSET_DATE_TIME') as specified according to java.time.format.DateTimeFormatter. If not specified, a long value input is expected to be an unix epoch (milli seconds from 1970/1/1), or a string value in 'yyyy-MM-dd' format for Date, 'HH:mm:ss.SSS' for Time (some database engines e.g. Derby or MySQL do not support milliseconds and will truncate milliseconds), 'yyyy-MM-dd HH:mm:ss.SSS' for Timestamp is used. |

### Writes Attributes
| Name | Description |
| :--- | :--- |
| procedure.execute.duration | Duration of the stored procedure execution in milliseconds |

### State management
This component does not store state.

### Restricted
This component is not restricted.

### Input requirement
This component allows an incoming relationship.

### System Resource Considerations
None specified.