package com.codeud.springcamelintegration.routes;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.dataformat.YAMLDataFormat;
import org.apache.camel.model.rest.RestBindingMode;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Value;

@Component
public class FileProcessorRouterBuilder extends RouteBuilder {
    YAMLDataFormat yaml = new YAMLDataFormat();

    @Value("${rabbitmq.server}")
    private String rabbitServerAddress;
    @Value("${rest.webserver}")
    private String restWebServerAddress;
    @Value("${rest.serverhost}")
    private String serverHost;
    @Value("${rest.serverport}")
    private String serverPort;

    @Override
    public void configure() {

        //rest service configuration
        restConfiguration()
                .host(serverHost)
                .port(serverPort)
                .component(restWebServerAddress)
                .bindingMode(RestBindingMode.auto);

        //Invoking rest API with route id as rest-router
        rest("/file/{fileName}")
                .post()
                .routeId("rest-router")
                .to("direct:content");

        //Control flow based on content type from Rest API between XML, JSON and CSV type
        from("direct:content")
                .log("content-route")
                .routeId("content-route")
                .choice()
                    .when(header("Content-Type").isEqualTo("text/csv"))
                        .log("block routing to direct:csv")
                        .to("direct:csv")
                    .when(header("Content-Type").isEqualTo("application/xml"))
                        .log("block routing to direct:xml")
                        .to("direct:xml")
                    .when(header("Content-Type").isEqualTo("application/json"))
                        .log("block block routing to direct:json")
                        .to("direct:json")
                    .otherwise()
                        .log("block routing to direct: content-error")
                        .log("block routing to direct:error")
                        .to("direct:error");

        //CSV Content
        from("direct:csv").routeId("csv-route")
                .doTry()
                    .log("csv-route")
                    .unmarshal().csv()
                    .bean("FileTransformProcessor", "csvJavaTransformation")
                    .marshal(yaml)
                    .to("rabbitmq://" + rabbitServerAddress + "/jsonExchange?queue=jsonQueue&routingKey=jsonRoutingKey&autoDelete=false")
                .doCatch(Exception.class)
                    .log("csv-route-error")
                    .process(exchange -> {
                        String fileName = (String) exchange.getIn().getHeader("fileName");
                        String errorFileName = fileName.substring(0, fileName.lastIndexOf(".")) + "-" + System.currentTimeMillis() + "-error.txt";
                        exchange.getIn().setHeader("errFileName", errorFileName);
                    })
                    .to("rabbitmq://" + rabbitServerAddress + "/deadLetterExchange?queue=deadLetterQueue&routingKey=deadLetterRoutingKey")
                .doFinally()
                    .log("csv-route-finally")
                    .to("file:outputs?fileName=${header.fileName}-${header.currentTimeStamp}.yaml")
                .end();

        //XML Content
        from("direct:xml").routeId("xml-route")
                .doTry()
                    .log("xml-route")
                    .unmarshal().jacksonXml()
                    .bean("FileTransformProcessor", "xmlJavaTransformation")
                    .marshal(yaml)
                    .to("rabbitmq://"+ rabbitServerAddress + "/jsonExchange?queue=jsonQueue&routingKey=jsonRoutingKey&autoDelete=false")
                .doCatch(Exception.class)
                    .log("xml-route-error")
                    .process(exchange -> {
                        String fileName = (String) exchange.getIn().getHeader("fileName");
                        String errorFileName = fileName.substring(0, fileName.lastIndexOf(".")) + "-" + System.currentTimeMillis() + "-error.txt";
                        exchange.getIn().setHeader("errFileName", errorFileName);
                    })
                    .to("rabbitmq://"+ rabbitServerAddress +"/deadLetterExchange?queue=deadLetterQueue&routingKey=deadLetterRoutingKey")
                .doFinally()
                    .log("xml-route-finally")
                    .to("file:outputs?fileName=${header.fileName}-${header.currentTimeStamp}.yaml")
                .end();

        //JSON Content
        from("direct:json").routeId("json-route")
                .doTry()
                    .log("json-route")
                    .bean("FileTransformProcessor", "jsonJavaTransformation")
                    .marshal(yaml)
                    .to("rabbitmq://"+ rabbitServerAddress +"/jsonExchange?queue=jsonQueue&routingKey=jsonRoutingKey&autoDelete=false")
                .doCatch(Exception.class)
                    .log("json-route-error")
                     .process(exchange -> {
                         String fileName = (String) exchange.getIn().getHeader("fileName");
                         String errorFileName = fileName.substring(0, fileName.lastIndexOf(".")) + "-" + System.currentTimeMillis() + "-error.txt";
                         exchange.getIn().setHeader("errFileName", errorFileName);
                     })
                    .to("rabbitmq://"+ rabbitServerAddress +"/deadLetterExchange?queue=deadLetterQueue&routingKey=deadLetterRoutingKey")
                .doFinally()
                         .log("json-route-finally")
                         .to("file:outputs?fileName=${header.fileName}-${header.currentTimeStamp}.yaml")
                .end();
    }
}
