/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.apachepoi.xssf;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.FormulaEvaluator;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProcessor;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.DataProviderInjection;

public class XSSFSheetToCSVDataProcessor implements DataProcessor
{
    private static final Logger logger = Logger.getLogger(XSSFSheetToCSVDataProcessor.class.getName());

    public XSSFSheetToCSVDataProcessor()
    {
        logger.log(Level.FINE, "XSSFSheetToCSVDataProcessor");
    }

    public XSSFSheetToCSVDataProcessor(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "XSSFSheetToCSVDataProcessor: " + name + ", " + properties);

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
            logger.log(Level.FINE, "Generate CSV for XSSF");

            try
            {
                String      filename                = (String) data.get("filename");
                String      baseFilename            = filename.substring(0, filename.lastIndexOf('.'));
                InputStream xssfWorkbookInputStream = new ByteArrayInputStream((byte[]) data.get("data"));

                XSSFWorkbook xssfWorkbook = new XSSFWorkbook(xssfWorkbookInputStream);

                for (int sheetIndex = 0; sheetIndex < xssfWorkbook.getNumberOfSheets(); sheetIndex++)
                {
                    String csv = generateCSVFromSheet(xssfWorkbook.getSheetAt(sheetIndex), xssfWorkbook.getCreationHelper().createFormulaEvaluator());

                    Map<String, String> csvData = new HashMap<String, String>();
                    if (xssfWorkbook.getNumberOfSheets() == 0)
                        csvData.put("filename", baseFilename + ".csv");
                    else
                        csvData.put("filename", baseFilename + "_" + xssfWorkbook.getSheetName(sheetIndex) + ".csv");
                    csvData.put("resourceformat", "csv");
                    csvData.put("data", csv);

                    _dataProvider.produce(csvData);
                }
                xssfWorkbookInputStream.close();

                xssfWorkbook.close();
            }
            catch (Throwable throwable)
            {
                logger.log(Level.WARNING, "Problem Generating during XSSF Speadsheet Metadata Scan (File)", throwable);
            }
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

    private String generateCSVFromSheet(XSSFSheet xssfSheet, FormulaEvaluator evaluator)
    {
        StringBuffer csvText = new StringBuffer();

        for (int rowIndex = xssfSheet.getFirstRowNum(); rowIndex < xssfSheet.getLastRowNum(); rowIndex++)
        {
            XSSFRow row = xssfSheet.getRow(rowIndex);

            boolean firstCell = true;
            for (int columnIndex = 0; columnIndex < row.getPhysicalNumberOfCells() + row.getFirstCellNum(); columnIndex++)
            {
                if (firstCell)
                    firstCell = false;
                else
                    csvText.append(',');

                csvText.append(generateCSVFromCell(row.getCell(columnIndex), evaluator));
            }
        }

        return csvText.toString();
    }

    private String generateCSVFromCell(Cell cell, FormulaEvaluator evaluator)
    {
        CellValue cellValue = evaluator.evaluate(cell);

        if (cellValue.getCellType() == Cell.CELL_TYPE_STRING)
            return cellValue.getStringValue();
        else if (cellValue.getCellType() == Cell.CELL_TYPE_NUMERIC)
            return Double.toString(cellValue.getNumberValue());
        else if (cellValue.getCellType() == Cell.CELL_TYPE_BOOLEAN)
            return Boolean.toString(cellValue.getBooleanValue());
        else if (cellValue.getCellType() == Cell.CELL_TYPE_BLANK)
            return "";
        else
        {
            logger.log(Level.WARNING, "Problem process cell: Unknown Cell Type = " + cell.getCellType());
            return "";
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