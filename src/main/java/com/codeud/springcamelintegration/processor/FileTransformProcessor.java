package com.codeud.springcamelintegration.processor;

import com.codeud.springcamelintegration.util.FileExchangeProcessorUtil;
import org.apache.camel.Exchange;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public class FileTransformProcessor {
    @Autowired
    FileExchangeProcessorUtil fileExchangeProcessorUtil;

    public FileTransformProcessor(FileExchangeProcessorUtil fileExchangeProcessorUtil) {
        this.fileExchangeProcessorUtil = fileExchangeProcessorUtil;
    }

    public void csvJavaTransformation(Exchange exchange) {
        Object body = exchange.getIn().getBody();
        if (body instanceof List) {
            List csvRecordList = (List) body;
            if (csvRecordList.isEmpty() || !(csvRecordList.get(0) instanceof List)) {
                return;
            }

            List<List<String>> castedCsvRecordList = (List<List<String>>) (List) csvRecordList;
            fileExchangeProcessorUtil.processFileExchange(castedCsvRecordList, exchange);
        }
    }

    public void jsonJavaTransformation(Exchange exchange) {
        Object body = exchange.getIn().getBody();
        if (body instanceof Map) {
            Map<String, Object> jsonRecordMap = (Map<String, Object>) body;
            fileExchangeProcessorUtil.processFileExchange(jsonRecordMap, exchange);
        }
    }

    public void xmlJavaTransformation(Exchange exchange) {
        Object body = exchange.getIn().getBody();
        if (body instanceof Map) {
            Map<String, Object> xmlRecordMap = (Map<String, Object>) body;
            fileExchangeProcessorUtil.processFileExchange(xmlRecordMap, exchange);
        }
    }
}