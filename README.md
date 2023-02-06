**Case Study:** 
Apache Camel File Operations using Rabbit MQ

**Introduction**
This project is a case study developed using Apache Camel and RabbitMQ. The project aims to demonstrate the capabilities of Apache Camel in integrating with RabbitMQ to achieve various messaging patterns. 

**Technical Stack**
Apache Camel(Java DSL)
RabbitMQ
Spring Boot
Java 11


**Problem Statement:**

_Create a rest API using Apache camel that accepts a file as an input and does the following: 
POST /file?name=currency.csv to create a file.

Based on the type of file they must be routed to XML or CSV or JSON in Rabbit MQ 
appropriate queues:
File format for 
CSV: 
USD, INR 
1, 81 

JSON: 
{ USD: 1, INR: 81 } 

XML: 
<CONVERSION> <USD>1</USD> <INR>81</INR> </CONVERSION>

Convert them to Java bean and handle any errors in conversion using try catch and finally 
blocks and the output to be written in YAML format to a folder named “outputs” without as 
filename-{timestamp}.yaml and if the files is not processable use a Dead letter queue to handle the posted content as “filename-{timestamp}-error.txt"._

**Features**

a) Apache Camel Integration with ESB Rabbit MQ

b) Fallbacks to deadletter exchanges and queue.

c) File Handling IO Operations with the use of exchanges

d) Integration of Rest API with Apache Camel

**Running the Project**
a) Setup Rest client with below API config. Can use POSTMAN, Curl to hit the rest API:
API: http://localhost:9290/file/currency.json
Headers: Content-Type:application/json
Body:
{
    "usd": "1",
    "inr": "81"
}

b) Make sure Apache Camel is configured to consume API from the same port. Here :9290 for example.

c) Create Router for your Rest API which will direct the content of the file based on header value "content type".

d) Process the file and tranform based on content type i.e. CSV-> JAVA Object, XML-> JAVA bject, JSON-> JAVA Object. 

d) Send the content of the file including header and body to rabbitmq exchange and queue.

e) Please check POM.xml for framework versions.


**Conclusion**
The conclusion of this problem statement is that a REST API has been developed using Apache Camel that can accept a file as input, route it to appropriate queues in RabbitMQ based on its format (CSV, JSON, or XML), convert it to a Java bean, handle errors in conversion using try-catch-finally blocks, and write the output in YAML format to a folder named "outputs." In case the file is not processable, a Dead Letter Queue is used to handle the posted content, which is saved as "filename-{timestamp}-error.txt." This solution demonstrates the capabilities of Apache Camel in integrating with RabbitMQ for messaging and error handling.