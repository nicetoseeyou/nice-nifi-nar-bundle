# nice-nifi-nar-bundle
NiFi nar bundle
### Available Processor
1. ExecuteStoredProcedure
    - Maven dependency (not deployed)  
    ```
   <denpendency>
       <groupId>lab.nice</groupId>
       <artifactId>nice-nifi-stored-procedure-processor-nar</artifactId>
       <version>0.0.1-SNAPSHOT</version>
       <type>nar</type>
   </denpendency>
    ```
    - Description  
    Executes provided stored procedure.
2. ConsumerKafkaInJson
    - Maven dependency (not deployed)  
    ``` 
   <denpendency>
       <groupId>lab.nice</groupId>
       <artifactId>nice-nifi-kafka-nar</artifactId>
       <version>0.0.1-SNAPSHOT</version>
       <type>nar</type>
   </denpendency>
    ```
    - Description  
    Consume Kafka topic with JSON messages. The default Kafka deserializer is _org.apache.kafka.connect.json.JsonSerializer_ . Please do configure key/value deserializer via dynamic property with key '_key.serializer_' and '_value.serializer_'.
### Available Service Controller
1. HikariCPService
    - Maven dependency (not deployed)  
    ``` 
   <dependency>
       <groupId>lab.nice</groupId>
       <artifactId>nice-nifi-dbcp-service-nar</artifactId>
       <version>0.0.1-SNAPSHOT</version>
       <type>nar</type>
   </dependency>
    ```
    - Description  
    DBCP service with HikariCP implementation.