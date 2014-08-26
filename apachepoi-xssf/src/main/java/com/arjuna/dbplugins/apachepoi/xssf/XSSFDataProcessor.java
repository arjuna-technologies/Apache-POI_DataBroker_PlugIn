/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.apachepoi.xssf;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.risbic.intraconnect.basic.BasicDataConsumer;
import org.risbic.intraconnect.basic.BasicDataProvider;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataProcessor;
import com.arjuna.databroker.data.DataProvider;

public class XSSFDataProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(XSSFDataProcessor.class.getName());

    public XSSFDataProcessor(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "XSSFDataProcessor: " + name + ", " + properties);

        _name       = name;
        _properties = properties;

        _dataConsumer = new BasicDataConsumer<File>(this, "consume", File.class);
        _dataProvider = new BasicDataProvider<String>(this);
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    private static class WorkbookHandler extends DefaultHandler
    {
    	private static final String SHEET_NAME               = "sheet";
    	private static final String SPREADSHEETML_NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";

    	private WorkbookHandler(SharedStringsTable sharedStringsTable)
        {
            _sharedStringsTable = sharedStringsTable;
            _content            = new StringBuffer();
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException
        {
            if ((localName != null) && localName.equals(SHEET_NAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
            {
                for (int index = 0; index < attributes.getLength(); index++)
                    System.out.println("<LocalName=" + attributes.getLocalName(index) + ", URL=" + attributes.getURI(index)  + ", QName=" + attributes.getQName(index)+ ", Value=" + attributes.getValue(index) + ">");
                System.out.println();
            }

            _content = new StringBuffer();
        }

        @Override
        public void endElement(String uri, String localName, String qName)
            throws SAXException
        {
            System.out.println("end :" + qName);
        }

        @Override
        public void characters(char[] characters, int start, int length)
            throws SAXException
        {
            _content.append(characters, start, length);
        }

        private SharedStringsTable _sharedStringsTable;
        private StringBuffer       _content;
    }

    private static class SheetHandler extends DefaultHandler
    {
        private SheetHandler(SharedStringsTable sharedStringsTable)
        {
            _sharedStringsTable = sharedStringsTable;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException
        {
            System.out.print("start :" + qName + "[");
            for (int index = 0; index < attributes.getLength(); index++)
                System.out.print("<qname=" + attributes.getQName(index) + ",type=" + attributes.getType(index) + ",value=" + attributes.getValue(index) + ">");
            System.out.println("]");

            if (qName.equals("c"))
            {
//                 System.out.print(attributes.getValue("r") + " - ");
                 String cellType = attributes.getValue("t");
                 if (cellType != null && cellType.equals("s"))
                     _nextIsString = true;
                 else
                     _nextIsString = false;
            }
            _lastContents = "";
        }
        
        @Override
        public void endElement(String uri, String localName, String qName)
            throws SAXException
        {
            System.out.println("end :" + qName);
            if (_nextIsString)
            {
                int index = Integer.parseInt(_lastContents);
                _lastContents = new XSSFRichTextString(_sharedStringsTable.getEntryAt(index)).toString();
                _nextIsString = false;
            }

 //           if (name.equals("v"))
 //               System.out.println(_lastContents);
        }

        @Override
        public void characters(char[] characters, int start, int length)
            throws SAXException
        {
            _lastContents += new String(characters, start, length);
        }

        private SharedStringsTable _sharedStringsTable;
        private String             _lastContents;
        private boolean            _nextIsString;
    }

    public void consume(File data)
    {
        try
        {
            OPCPackage         opcPackage         = OPCPackage.open(data);
            XSSFReader         xssfReader         = new XSSFReader(opcPackage);
            SharedStringsTable sharedStringsTable = xssfReader.getSharedStringsTable();

            XMLReader      workbookParser      = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            ContentHandler workbookHandler     = new WorkbookHandler(sharedStringsTable);
            workbookParser.setContentHandler(workbookHandler);

            InputStream    workbookInputStream = xssfReader.getWorkbookData();
            // InputSource    workbookSource      = new InputSource(workbookInputStream);
            // workbookParser.parse(workbookSource);
            int wbch = workbookInputStream.read();
            while (wbch != -1)
            {
                System.out.print((char) wbch);
                wbch = workbookInputStream.read();
            }
            workbookInputStream.close();

            XMLReader      sheetParser  = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            ContentHandler sheetHandler = new SheetHandler(sharedStringsTable);
            sheetParser.setContentHandler(sheetHandler);

            Iterator<InputStream> sheetInputStreamIterator = xssfReader.getSheetsData();
            while (sheetInputStreamIterator.hasNext())
            {
                InputStream sheetInputStream = sheetInputStreamIterator.next();
                // InputSource sheetSource = new InputSource(sheetInputStream);
                // parser.parse(sheetSource);
                int sch = sheetInputStream.read();
                while (sch != -1)
                {
                    System.out.print((char) sch);
                    sch = sheetInputStream.read();
                }
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
    private DataConsumer<File>   _dataConsumer;
    private DataProvider<String> _dataProvider;
}