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
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import static org.junit.Assert.*;
import com.arjuna.databroker.data.connector.ObservableDataProvider;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.core.DataFlowNodeLifeCycleControl;
import com.arjuna.databroker.data.core.DataFlowNodeLinkLifeCycleControl;
import com.arjuna.dbplugins.apachepoi.xssf.XSSFStreamSheetToCSVDataProcessor;
import com.arjuna.dbutils.testsupport.dataflownodes.dummy.DummyDataSink;
import com.arjuna.dbutils.testsupport.dataflownodes.dummy.DummyDataSource;
import com.arjuna.dbutils.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLifeCycleControl;
import com.arjuna.dbutils.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLinkLifeCycleControl;

public class SimpleStreamSheetToCSVTest
{
    @SuppressWarnings("rawtypes")
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

            DataFlowNodeLifeCycleControl     dataFlowNodeLifeCycleControl     = new TestJEEDataFlowNodeLifeCycleControl();
            DataFlowNodeLinkLifeCycleControl dataFlowNodeLinkLifeCycleControl = new TestJEEDataFlowNodeLinkLifeCycleControl();

            String                            name                              = "XSSF Stream Sheet To CSV Data Processor";
            Map<String, String>               properties                        = Collections.emptyMap();

            DummyDataSource                   dummyDataSource                   = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
            XSSFStreamSheetToCSVDataProcessor xssfStreamSheetToCSVDataProcessor = new XSSFStreamSheetToCSVDataProcessor(name, properties);
            DummyDataSink                     dummyDataSink                     = new DummyDataSink("Dummy Data Sink", Collections.<String, String>emptyMap());

            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), xssfStreamSheetToCSVDataProcessor, null);
            dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSink, null);

            ((ObservableDataProvider<Map>) dummyDataSource.getDataProvider(Map.class)).addDataConsumer((ObserverDataConsumer<Map>) xssfStreamSheetToCSVDataProcessor.getDataConsumer(Map.class));
            ((ObservableDataProvider<Map>) xssfStreamSheetToCSVDataProcessor.getDataProvider(Map.class)).addDataConsumer((ObserverDataConsumer<Map>) dummyDataSink.getDataConsumer(Map.class));

            Map<String, Object> map = new HashMap<String, Object>();
            map.put("filename", "Test.xslx");
            map.put("data", spreadsheetData);

            dummyDataSource.sendData(map);

            List<Object> receivedData = dummyDataSink.receivedData();
            assertEquals("Unexpected received data", 1, receivedData.size());

            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(xssfStreamSheetToCSVDataProcessor);
            dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSink);
        }
        catch (IOException ioException)
        {
            fail("IO Exception");
        }
    }
}
