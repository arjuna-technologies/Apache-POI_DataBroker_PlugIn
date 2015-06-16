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

public class XSSFRowToJSONDataProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(XSSFRowToJSONDataProcessor.class.getName());

    public XSSFRowToJSONDataProcessor()
    {
        logger.log(Level.FINE, "XSSFRowToJSONDataProcessor");
    }

    public XSSFRowToJSONDataProcessor(String name, Map<String, String> properties)
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

    private class WorkbookHandler extends DefaultHandler
    {
        private static final String SPREADSHEETML_NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
        private static final String RELATIONSHIPS_NAMESPACE = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
        private static final String NONE_NAMESPACE          = "";
        private static final String SHEET_TAGNAME           = "sheet";
        private static final String NAME_ATTRNAME           = "name";
        private static final String ID_ATTRNAME             = "id";

        public WorkbookHandler(Map<String, String> refIdMap)
        {
            _refIdMap = refIdMap;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException
        {
            if ((localName != null) && localName.equals(SHEET_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
            {
                String name = attributes.getValue(NONE_NAMESPACE, NAME_ATTRNAME);
                String id   = attributes.getValue(RELATIONSHIPS_NAMESPACE, ID_ATTRNAME);

                if (name != null)
                    _refIdMap.put(name, id);
            }
        }

        private Map<String, String> _refIdMap;
    }

    private class SheetHandler extends DefaultHandler
    {
        private static final String SPREADSHEETML_NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
        private static final String NONE_NAMESPACE          = "";
        private static final String ROW_TAGNAME             = "row";
        private static final String CELL_TAGNAME            = "c";
        private static final String VALUE_TAGNAME           = "v";
        private static final String REF_ATTRNAME            = "r";

        public SheetHandler(SharedStringsTable sharedStringsTable)
        {
            _sharedStringsTable = sharedStringsTable;
            _cellName           = null;
            _value              = new StringBuffer();
            _rowMap             = new LinkedHashMap<String, String>();
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException
        {
            if ((localName != null) && localName.equals(CELL_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
                _cellName = attributes.getValue(NONE_NAMESPACE, REF_ATTRNAME);
            else if ((localName != null) && localName.equals(VALUE_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
                _value.setLength(0);
        }

        @Override
        public void endElement(String uri, String localName, String qName)
            throws SAXException
        {
            if ((localName != null) && localName.equals(VALUE_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
            {
                _rowMap.put(_cellName, _value.toString());
                _value.setLength(0);
            }
            else if ((localName != null) && localName.equals(ROW_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
            {
                String rowJSON = rowMap2JSON(_rowMap);
                if (logger.isLoggable(Level.FINER))
                    logger.log(Level.FINER, "Row: [" + rowJSON + "]");
                _dataProvider.produce(rowJSON);
                _rowMap.clear();
            }
        }

        @Override
        public void characters(char[] characters, int start, int length)
            throws SAXException
        {
            _value.append(characters, start, length);
        }

        private String rowMap2JSON(Map<String, String> rowMap)
        {
            StringBuffer json = new StringBuffer();

            json.append("{");
            boolean first = true;
            for (Entry<String, String> row: rowMap.entrySet())
            {
                if (! first)
                    json.append(",");
                else
                    first = false;

                json.append("\"");
                json.append(string2JSON(removeRowNumber(row.getKey())));
                json.append("\"");
                json.append(":");
                json.append("\"");
                json.append(string2JSON(row.getValue()));
                json.append("\"");
            }
            json.append("}");

            return json.toString();
        }

        private String removeRowNumber(String cellName)
        {
            int index = 0;
            while ((index < cellName.length()) && Character.isAlphabetic(cellName.charAt(index)))
                index++;

            return cellName.substring(0, index);
        }

        private String string2JSON(String value)
        {
            return value.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"");
        }

        private SharedStringsTable  _sharedStringsTable;
        private String              _cellName;
        private StringBuffer        _value;
        private Map<String, String> _rowMap;
    }

    public void consume(File data)
    {
        try
        {
            Map<String, String> refIdMap = new HashMap<String, String>();

            OPCPackage         opcPackage         = OPCPackage.open(data);
            XSSFReader         xssfReader         = new XSSFReader(opcPackage);
            SharedStringsTable sharedStringsTable = xssfReader.getSharedStringsTable();

            XMLReader      workbookParser  = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            ContentHandler workbookHandler = new WorkbookHandler(refIdMap);
            workbookParser.setContentHandler(workbookHandler);

            InputStream workbookInputStream = xssfReader.getWorkbookData();
            InputSource workbookSource      = new InputSource(workbookInputStream);
            workbookParser.parse(workbookSource);
            workbookInputStream.close();

            XMLReader      sheetParser  = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            ContentHandler sheetHandler = new SheetHandler(sharedStringsTable);
            sheetParser.setContentHandler(sheetHandler);

            Iterator<InputStream> sheetInputStreamIterator = xssfReader.getSheetsData();
            while (sheetInputStreamIterator.hasNext())
            {
                InputStream sheetInputStream = sheetInputStreamIterator.next();
                InputSource sheetSource = new InputSource(sheetInputStream);
                sheetParser.parse(sheetSource);
                sheetInputStream.close();
            }
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem processing XSSF file \"" + data.getAbsolutePath() + "\"", throwable);
        }
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(File.class);

        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (dataClass == File.class)
            return (DataConsumer<T>) _dataConsumer;
        else
            return null;
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataProviderDataClasses = new HashSet<Class<?>>();

        dataProviderDataClasses.add(String.class);

        return dataProviderDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        if (dataClass == String.class)
            return (DataProvider<T>) _dataProvider;
        else
            return null;
    }

    private String               _name;
    private Map<String, String>  _properties;
    private DataFlow             _dataFlow;
    @DataConsumerInjection(methodName="consume")
    private DataConsumer<File>   _dataConsumer;
    @DataProviderInjection
    private DataProvider<String> _dataProvider;
}