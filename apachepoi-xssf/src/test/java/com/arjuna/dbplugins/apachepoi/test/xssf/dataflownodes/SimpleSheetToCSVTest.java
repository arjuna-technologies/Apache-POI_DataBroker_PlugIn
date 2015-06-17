/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.apachepoi.test.xssf.dataflownodes;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import org.junit.Test;
import static org.junit.Assert.*;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.core.DataFlowNodeLifeCycleControl;
import com.arjuna.dbplugins.apachepoi.xssf.XSSFSheetToCSVDataProcessor;
import com.arjuna.dbutils.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLifeCycleControl;

public class SimpleSheetToCSVTest
{
    @Test
    public void simpleInvocation()
    {
        DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

        String                      name                        = "XSSF Sheet To CSV Data Processor";
        Map<String, String>         properties                  = Collections.emptyMap();
        XSSFSheetToCSVDataProcessor xssfSheetToCSVDataProcessor = new XSSFSheetToCSVDataProcessor(name, properties);

        dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), xssfSheetToCSVDataProcessor, null);

        Map map = new HashMap();

        ((ObserverDataConsumer<Map>) xssfSheetToCSVDataProcessor.getDataConsumer(Map.class)).consume(null, map);

        dataFlowNodeLifeCycleControl.removeDataFlowNode(xssfSheetToCSVDataProcessor);
    }
}
