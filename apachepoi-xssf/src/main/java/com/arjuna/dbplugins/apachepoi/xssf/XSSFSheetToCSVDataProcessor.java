/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.apachepoi.xssf;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProcessor;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;

public class XSSFSheetToCSVDataProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(XSSFRowToJSONDataProcessor.class.getName());

    public XSSFSheetToCSVDataProcessor()
    {
        logger.log(Level.FINE, "XSSFRowToJSONDataProcessor");
    }

    public XSSFSheetToCSVDataProcessor(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "XSSFRowToJSONDataProcessor: " + name + ", " + properties);

        _name       = name;
        _properties = properties;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }
    
    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    public void consume(Map data)
    {
        try
        {
        	Object filename = data.get("filename");
            if ((filename != null) && (filename instanceof String))
                data.put("resourcename", filename);
            data.put("resourceformat", "xlsx");
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem processing XSSF", throwable);
        }
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(Map.class);

        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (Map.class.isAssignableFrom(dataClass))
            return (DataConsumer<T>) _dataConsumer;
        else
            return null;
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(Map.class);

        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (Map.class.isAssignableFrom(dataClass))
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    private String              _name;
    private Map<String, String> _properties;
    private DataFlow            _dataFlow;
    @DataConsumerInjection(methodName="consume")
    private DataConsumer<Map>   _dataConsumer;
    @DataProviderInjection
    private DataProvider<Map>   _dataProvider;
}