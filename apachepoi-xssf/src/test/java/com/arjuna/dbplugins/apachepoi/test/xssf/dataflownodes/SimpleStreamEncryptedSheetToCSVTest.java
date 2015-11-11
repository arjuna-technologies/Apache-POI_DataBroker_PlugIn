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
import com.arjuna.dbplugins.apachepoi.xssf.XSSFStreamSheetToCSVDataProcessor;
import com.arjuna.dbutils.testsupport.dataflownodes.dummy.DummyDataSink;
import com.arjuna.dbutils.testsupport.dataflownodes.dummy.DummyDataSource;
import com.arjuna.dbutils.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLifeCycleControl;

public class SimpleStreamEncryptedSheetToCSVTest
{
    @SuppressWarnings("rawtypes")
    @Test
    public void simpleDecryptInvocation()
    {
        try
        {
            int character = 0;
            int index     = 0;

            File         spreadsheetFile        = new File("Test02.xlsx");
            byte[]       spreadsheetData        = new byte[(int) spreadsheetFile.length()];
            InputStream  spreadsheetInputStream = new FileInputStream(spreadsheetFile);
            index     = 0;
            character = spreadsheetInputStream.read();
            while (character != -1)
            {
                spreadsheetData[index] = (byte) character;
                character = spreadsheetInputStream.read();
                index++;
            }
            spreadsheetInputStream.close();

            File         csvFile        = new File("Test01_s.csv");
            byte[]       csvData        = new byte[(int) csvFile.length()];
            InputStream  csvInputStream = new FileInputStream(csvFile);
            index     = 0;
            character = csvInputStream.read();
            while (character != -1)
            {
                csvData[index] = (byte) character;
                character = csvInputStream.read();
                index++;
            }
            csvInputStream.close();

            DataFlowNodeLifeCycleControl     dataFlowNodeLifeCycleControl     = new TestJEEDataFlowNodeLifeCycleControl();
//            DataFlowNodeLinkLifeCycleControl dataFlowNodeLinkLifeCycleControl = new TestJEEDataFlowNodeLinkLifeCycleControl();

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

            Map<String, Object> inputMap = new HashMap<String, Object>();
            inputMap.put("filename", "Test.xslx");
            inputMap.put("data", spreadsheetData);
            inputMap.put("password", "test");

            dummyDataSource.sendData(inputMap);

            List<Object> receivedData = dummyDataSink.receivedData();
            assertEquals("Unexpected received data", 1, receivedData.size());

            Map<String, Object> outputMap = (Map<String, Object>) receivedData.get(0);
            assertNotNull("Unexpected null received data", outputMap);
            assertEquals("Unexpected received map size", 3, outputMap.size());
            assertNotNull("Unexpected null 'filename' entry", outputMap.get("filename"));
            assertNotNull("Unexpected null 'data' entry", outputMap.get("data"));
            assertNotNull("Unexpected null 'resourceformat' entry", outputMap.get("resourceformat"));
            assertEquals("Unexpected value of 'filename' entry", "Test_1.csv", outputMap.get("filename"));
            assertArrayEquals("Unexpected value of 'data' entry", csvData, (byte[]) outputMap.get("data"));
            assertEquals("Unexpected value of 'resourceformat' entry", "csv", outputMap.get("resourceformat"));

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
