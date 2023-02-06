package com.codeud.springcamelintegration.util;

import org.apache.camel.Exchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;



@Component
public class FileExchangeProcessorUtil {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Below method process the object and exchange object from file type
     * @param fileProcessor
     * @param exchange
     */
    public void processFileExchange(Object fileProcessor, Exchange exchange) {
        try {
            if(null != fileProcessor) {
                String fileName =  exchange.getIn().getHeader("fileName").toString();
                if(null != fileName && !fileName.isBlank()) {
                    exchange.getOut().setHeader("fileName", fileName.substring(0, fileName.indexOf(".")));
                }
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");
                exchange.getOut().setHeader("currentTimeStamp", formatter.format(new Date()));
                exchange.getOut().setBody(fileProcessor);
                logger.info("File with name '{}' processed successfully", fileName);
            }
        } catch(Exception ex) {
            String fileName =  exchange.getIn().getHeader("fileName").toString();
            if(null != fileName && !fileName.isBlank()) {
                exchange.getOut().setHeader("fileName", fileName.substring(0, fileName.indexOf(".")));
            }
            ex.printStackTrace();
            exchange.getOut().setBody(ex.getMessage());
            logger.error("Error processing file with name '{}': {}", fileName, ex.getMessage(), ex);
            throw ex;
        }
    }
}
