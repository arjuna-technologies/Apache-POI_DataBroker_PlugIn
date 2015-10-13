/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.apachepoi;

import java.util.Collections;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.EJB;
import javax.ejb.Singleton;
import javax.ejb.Startup;
import com.arjuna.databroker.data.DataFlowNodeFactory;
import com.arjuna.databroker.data.DataFlowNodeFactoryInventory;
import com.arjuna.dbplugins.apachepoi.xssf.XSSFRowToJSONDataFlowNodeFactory;
import com.arjuna.dbplugins.apachepoi.xssf.XSSFSheetToCSVDataFlowNodeFactory;
import com.arjuna.dbplugins.apachepoi.xssf.XSSFStreamSheetToCSVDataFlowNodeFactory;

@Startup
@Singleton
public class ApachePOIDataFlowNodeFactoriesSetup
{
    @PostConstruct
    public void setup()
    {
        DataFlowNodeFactory xssfRowToJSONDataFlowNodeFactory        = new XSSFRowToJSONDataFlowNodeFactory("Apache POI XSSF Row To JSON Data Flow Node Factories", Collections.<String, String>emptyMap());
        DataFlowNodeFactory xssfSheetToCSVDataFlowNodeFactory       = new XSSFSheetToCSVDataFlowNodeFactory("Apache POI XSSF Sheet To CSV Data Flow Node Factories", Collections.<String, String>emptyMap());
        DataFlowNodeFactory xssfStreamSheetToCSVDataFlowNodeFactory = new XSSFStreamSheetToCSVDataFlowNodeFactory("Apache POI XSSF Stream Sheet To CSV Data Flow Node Factories", Collections.<String, String>emptyMap());

        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(xssfRowToJSONDataFlowNodeFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(xssfSheetToCSVDataFlowNodeFactory);
        _dataFlowNodeFactoryInventory.addDataFlowNodeFactory(xssfStreamSheetToCSVDataFlowNodeFactory);
    }

    @PreDestroy
    public void cleanup()
    {
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Apache POI XSSF Row To JSON Data Flow Node Factories");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Apache POI XSSF Sheet To CSV Data Flow Node Factories");
        _dataFlowNodeFactoryInventory.removeDataFlowNodeFactory("Apache POI XSSF Stream Sheet To CSV Data Flow Node Factories");
    }

    @EJB(lookup="java:global/databroker/data-core-jee/DataFlowNodeFactoryInventory")
    private DataFlowNodeFactoryInventory _dataFlowNodeFactoryInventory;
}
