# nice-nifi-nar-bundle
NiFi nar bundle
### Available Processor
1. [ExecuteStoredProcedure](ExecuteStoredProcedure.md)  
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
2. [ConsumerKafkaInJson](ConsumerKafkaInJson.md)
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
    Consume Kafka topic with JSON messages. The default Kafka deserializer is _org.apache.kafka.connect.json.JsonSerializer_ .
### Available Service Controller
1. [HikariCPService](HikariCPService.md)
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