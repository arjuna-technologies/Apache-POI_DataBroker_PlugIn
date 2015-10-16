/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.apachepoi.xssf;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.SharedStringsTable;
import org.openxmlformats.schemas.spreadsheetml.x2006.main.CTRst;
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
import static org.apache.commons.lang3.StringEscapeUtils.escapeCsv;

public class XSSFStreamSheetToCSVDataProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(XSSFStreamSheetToCSVDataProcessor.class.getName());

    public XSSFStreamSheetToCSVDataProcessor()
    {
        logger.log(Level.FINE, "XSSFStreamSheetToCSVDataProcessor");
    }

    public XSSFStreamSheetToCSVDataProcessor(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "XSSFStreamSheetToCSVDataProcessor: " + name + ", " + properties);

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

    @SuppressWarnings("rawtypes")
    public void consume(Map data)
    {
        try
        {
            logger.log(Level.FINE, "Generate CSV from XSSF (streamed)");

            try
            {
                String filename     = (String) data.get("filename");
                String baseFilename = null;
                if (filename != null)
                    baseFilename = filename.substring(0, filename.lastIndexOf('.'));
                else
                    baseFilename = "file";

                InputStream        xssfWorkbookInputStream = new ByteArrayInputStream((byte[]) data.get("data"));
                OPCPackage         opcPackage              = OPCPackage.open(xssfWorkbookInputStream);
                XSSFReader         xssfReader              = new XSSFReader(opcPackage);
                SharedStringsTable sharedStringsTable      = xssfReader.getSharedStringsTable();

                int sheetNumber = 0;
                Iterator<InputStream> sheetsData = xssfReader.getSheetsData();
                while (sheetsData.hasNext())
                {
                    InputStream sheetData = sheetsData.next();

                    Map<Integer, Map<Integer, Object>> sheet = parseCSVFromSheet(sheetData, sharedStringsTable);

                    String csv = generateCSVFromSheet(sheet);
                    if (csv != null)
                    {
                        Map<String, Object> csvData = new HashMap<String, Object>();
                        csvData.put("filename", baseFilename + "_" + (sheetNumber + 1) + ".csv");
                        csvData.put("resourceformat", "csv");
                        csvData.put("data", csv.getBytes());

                        _dataProvider.produce(csvData);
                    }

                    sheetNumber++;
                }
                xssfWorkbookInputStream.close();
            }
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem Generating CSV from XSSF (Map)", throwable);
            }
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem Generating CSV from XSSF (Map)", throwable);
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

    private Map<Integer, Map<Integer, Object>> parseCSVFromSheet(InputStream sheetInputStream, SharedStringsTable sharedStringsTable)
    {
        try
        {
            Map<Integer, Map<Integer, Object>> sheet = new HashMap<Integer, Map<Integer, Object>>();

            XMLReader      sheetParser  = XMLReaderFactory.createXMLReader("org.apache.xerces.parsers.SAXParser");
            ContentHandler sheetHandler = new SheetHandler(sheet, sharedStringsTable);
            sheetParser.setContentHandler(sheetHandler);
            InputSource sheetSource = new InputSource(sheetInputStream);
            sheetParser.parse(sheetSource);
            sheetInputStream.close();

            return sheet;
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Sheet conversion to CSV", throwable);

            return null;
        }
    }

    private class SheetHandler extends DefaultHandler
    {
        private static final String SPREADSHEETML_NAMESPACE = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
        private static final String NONE_NAMESPACE          = "";
        private static final String CELL_TAGNAME            = "c";
        private static final String VALUE_TAGNAME           = "v";
        private static final String REF_ATTRNAME            = "r";
        private static final String TYPE_ATTRNAME           = "t";

        public SheetHandler(Map<Integer, Map<Integer, Object>> sheet, SharedStringsTable sharedStringsTable)
        {
            _sheet              = sheet;
            _sharedStringsTable = sharedStringsTable;

            _cellName  = null;
            _cellType  = null;
            _value     = new StringBuffer();
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes)
            throws SAXException
        {
            try
            {
                if ((localName != null) && localName.equals(CELL_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
                {
                    _cellName  = attributes.getValue(NONE_NAMESPACE, REF_ATTRNAME);
                    _cellType  = attributes.getValue(NONE_NAMESPACE, TYPE_ATTRNAME);
                }
                else if ((localName != null) && localName.equals(VALUE_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
                    _value.setLength(0);
            }
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem processing start tag", throwable);
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName)
            throws SAXException
        {
            try
            {
                if ((localName != null) && localName.equals(VALUE_TAGNAME) && (uri != null) && uri.equals(SPREADSHEETML_NAMESPACE))
                {
                    Integer rowNumber    = getRowNumber(_cellName);
                    Integer columnNumber = getColumnNumber(_cellName);

                    Map<Integer, Object> row = _sheet.get(rowNumber);
                    if (row == null)
                    {
                        row = new HashMap<Integer, Object>();
                        _sheet.put(rowNumber, row);
                    }

                    if ("str".equals(_cellType))
                        row.put(columnNumber, _value.toString());
                    else if ("n".equals(_cellType))
                        row.put(columnNumber, _value.toString());
                    else if ("s".equals(_cellType))
                    {
                        int index = Integer.parseInt(_value.toString());
                        CTRst stringRef = _sharedStringsTable.getEntryAt(index);
                        row.put(columnNumber, stringRef.getT());
                    }

                    _value.setLength(0);
                }
            }
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem processing end tag", throwable);
            }
        }

        @Override
        public void characters(char[] characters, int start, int length)
            throws SAXException
        {
            try
            {
                _value.append(characters, start, length);
            }
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem processing characters", throwable);
            }
        }

        private Map<Integer, Map<Integer, Object>> _sheet;

        private SharedStringsTable _sharedStringsTable;

        private String       _cellName;
        private String       _cellType;
        private StringBuffer _value;
    }

    private Integer getColumnNumber(String cellName)
    {
        int columnNumber = 0;

        int index = 0;
        while ((index < cellName.length()) && Character.isAlphabetic(cellName.charAt(index)))
        {
            columnNumber = (26 * columnNumber) + (cellName.charAt(index) - 'A' + 1);
            index++;
        }

        return columnNumber;
    }

    private Integer getRowNumber(String cellName)
    {
        int index = 0;
        while ((index < cellName.length()) && Character.isAlphabetic(cellName.charAt(index)))
            index++;

        return Integer.parseInt(cellName.substring(index, cellName.length()));
    }

    private String generateCSVFromSheet(Map<Integer, Map<Integer, Object>> sheetData)
    {
        StringBuffer csv = new StringBuffer();

        int rowMin    = Integer.MAX_VALUE;
        int rowMax    = Integer.MIN_VALUE;
        int columnMin = Integer.MAX_VALUE;
        int columnMax = Integer.MIN_VALUE;
        for (Integer rowNumber: sheetData.keySet())
        {
            rowMin = Math.min(rowMin, rowNumber);
            rowMax = Math.max(rowMax, rowNumber);

            Map<Integer, Object> rowData = sheetData.get(rowNumber);
            for (Integer columnNumber: rowData.keySet())
            {
                columnMin = Math.min(columnMin, columnNumber);
                columnMax = Math.max(columnMax, columnNumber);
            }
        }

        if ((rowMin <= rowMax) && (columnMin <= columnMax))
            for (int rowIndex = rowMin; rowIndex <= rowMax; rowIndex++)
                generateCSVFromRow(csv, sheetData.get(rowIndex), columnMin, columnMax);

        return csv.toString();
    }

    private void generateCSVFromRow(StringBuffer csv, Map<Integer, Object> rowData, int columnMin, int columnMax)
    {
        for (int columnIndex = columnMin; columnIndex <= columnMax; columnIndex++)
        {
            if (columnIndex != columnMin)
                csv.append(',');

            if (rowData != null)
                generateCSVFromCell(csv, rowData.get(columnIndex));
        }
        csv.append('\n');
    }

    private void generateCSVFromCell(StringBuffer csv, Object cellData)
    {
        if (cellData != null)
        {
            if (cellData instanceof String)
                csv.append(escapeCsv((String) cellData));
            else
                csv.append(escapeCsv((String) cellData.toString()));
        }
    }

    private String              _name;
    private Map<String, String> _properties;
    private DataFlow            _dataFlow;
    @SuppressWarnings("rawtypes")
    @DataConsumerInjection(methodName="consume")
    private DataConsumer<Map>   _dataConsumer;
    @SuppressWarnings("rawtypes")
    @DataProviderInjection
    private DataProvider<Map>   _dataProvider;
}