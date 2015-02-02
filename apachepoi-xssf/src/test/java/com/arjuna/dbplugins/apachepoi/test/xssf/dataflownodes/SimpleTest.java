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
import com.arjuna.dbplugins.apachepoi.xssf.XSSFDataProcessor;
import com.arjuna.dbutilities.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLifeCycleControl;

public class SimpleTest
{
    @Test
    public void simpleInvocation()
    {
    	DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();
    	
    	String              name              = "XSSF Data Processor";
    	Map<String, String> properties        = Collections.emptyMap();
        XSSFDataProcessor   xssfDataProcessor = new XSSFDataProcessor(name, properties);

        dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), xssfDataProcessor, null);
        
        File file = new File("Test01.xlsx");

        ((ObserverDataConsumer<File>) xssfDataProcessor.getDataConsumer(File.class)).consume(null, file);
        
        dataFlowNodeLifeCycleControl.removeDataFlowNode(xssfDataProcessor);
    }
}
