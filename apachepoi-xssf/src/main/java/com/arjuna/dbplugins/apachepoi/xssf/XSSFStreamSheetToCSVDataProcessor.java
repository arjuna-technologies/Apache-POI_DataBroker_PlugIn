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
import org.apache.poi.xssf.model.StylesTable;
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
                    filename.substring(0, filename.lastIndexOf('.'));
                else
                    baseFilename = "file";

                InputStream xssfWorkbookInputStream = new ByteArrayInputStream((byte[]) data.get("data"));
                OPCPackage  opcPackage              = OPCPackage.open(xssfWorkbookInputStream);
                XSSFReader  xssfReader              = new XSSFReader(opcPackage);
                StylesTable stylesTable             = xssfReader.getStylesTable();

                int sheetNumber = 0;
                Iterator<InputStream> sheetsData = xssfReader.getSheetsData();
                while (sheetsData.hasNext())
                {
                    InputStream sheetData = sheetsData.next();

                    Map<Integer, Map<Integer, Object>> sheet = parseCSVFromSheet(sheetData, opcPackage, stylesTable);

                    String csv = generateCSVFromSheet(sheet);
                    if (csv != null)
                    {
                        Map<String, String> csvData = new HashMap<String, String>();
                        csvData.put("filename", baseFilename + "_" + sheetNumber + ".csv");
                        csvData.put("resourceformat", "csv");
                        csvData.put("data", csv);

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

    private Map<Integer, Map<Integer, Object>> parseCSVFromSheet(InputStream sheetData, OPCPackage opcPackage, StylesTable stylesTable)
    {
        Map<Integer, Map<Integer, Object>> sheet = new HashMap<Integer, Map<Integer, Object>>();



        return sheet;
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
            {
                csv.append('"');
                csv.append(escapeCsv((String) cellData));
                csv.append('"');
            }
            else
                csv.append(cellData);
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