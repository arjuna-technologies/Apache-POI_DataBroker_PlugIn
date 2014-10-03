/*
 * Copyright (c) 2013-2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.apachepoi.test.xssf.dataflownodes;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import org.junit.Test;
import static org.junit.Assert.*;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.jee.DataFlowNodeLifeCycleControl;
import com.arjuna.dbplugins.apachepoi.xssf.XSSFDataProcessor;

public class SimpleTest
{
    @Test
    public void simpleInvocation()
    {
    	String              name              = "XSSF Data Processor";
    	Map<String, String> properties        = Collections.emptyMap();
        XSSFDataProcessor   xssfDataProcessor = new XSSFDataProcessor(name, properties);

        DataFlowNodeLifeCycleControl.processCreatedDataFlowNode(xssfDataProcessor, null);
        
        File file = new File("Test01.xlsx");

        ((ObserverDataConsumer<File>) xssfDataProcessor.getDataConsumer(File.class)).consume(null, file);
    }
}
