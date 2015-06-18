/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.apachepoi.test.xssf.dataflownodes;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.IOException;
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
        try
        {
            File         spreadsheetFile        = new File("Test01.xlsx");
            byte[]       spreadsheetData        = new byte[(int) spreadsheetFile.length()];
            InputStream  spreadsheetInputStream = new FileInputStream(spreadsheetFile);
            int          character              = spreadsheetInputStream.read();
            int          index                  = 0;
            while (character != -1)
            {
                spreadsheetData[index] = (byte) character;
                character = spreadsheetInputStream.read();
                index++;
            }
            spreadsheetInputStream.close();

            DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

            String                      name                        = "XSSF Sheet To CSV Data Processor";
            Map<String, String>         properties                  = Collections.emptyMap();
            XSSFSheetToCSVDataProcessor xssfSheetToCSVDataProcessor = new XSSFSheetToCSVDataProcessor(name, properties);

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), xssfSheetToCSVDataProcessor, null);

            Map map = new HashMap();
            map.put("filename", "Test.xslx");
            map.put("data", spreadsheetData);

            ((ObserverDataConsumer<Map>) xssfSheetToCSVDataProcessor.getDataConsumer(Map.class)).consume(null, map);

            dataFlowNodeLifeCycleControl.removeDataFlowNode(xssfSheetToCSVDataProcessor);
        }
        catch (IOException ioException)
        {
            fail("IO Exception");
        }
    }
}
