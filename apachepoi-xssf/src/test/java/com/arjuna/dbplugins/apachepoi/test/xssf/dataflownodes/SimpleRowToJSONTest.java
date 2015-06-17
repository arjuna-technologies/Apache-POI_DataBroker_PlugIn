/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.apachepoi.test.xssf.dataflownodes;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;
import static org.junit.Assert.*;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.core.DataFlowNodeLifeCycleControl;
import com.arjuna.dbplugins.apachepoi.xssf.XSSFRowToJSONDataProcessor;
import com.arjuna.dbutils.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLifeCycleControl;

public class SimpleRowToJSONTest
{
    @Test
    public void simpleInvocation()
    {
        DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

        String                     name                       = "XSSF Row To JSON Data Processor";
        Map<String, String>        properties                 = Collections.emptyMap();
        XSSFRowToJSONDataProcessor xssfRowToJSONDataProcessor = new XSSFRowToJSONDataProcessor(name, properties);

        dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), xssfRowToJSONDataProcessor, null);

        File file = new File("Test01.xlsx");

        ((ObserverDataConsumer<File>) xssfRowToJSONDataProcessor.getDataConsumer(File.class)).consume(null, file);

        dataFlowNodeLifeCycleControl.removeDataFlowNode(xssfRowToJSONDataProcessor);
    }
}
