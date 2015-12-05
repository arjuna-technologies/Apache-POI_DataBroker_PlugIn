/*
 * Copyright (c) 2014-2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.apachepoi.xssf;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellValue;
import org.apache.poi.ss.usermodel.DateUtil;
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
import static org.apache.commons.lang3.StringEscapeUtils.escapeCsv;

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
            logger.log(Level.FINE, "Generate CSV from XSSF");

            try
            {
                String filename     = (String) data.get("filename");
                String baseFilename = null;
                if (filename != null)
                    baseFilename = filename.substring(0, filename.lastIndexOf('.'));
                else
                    baseFilename = "file";

                InputStream  xssfWorkbookInputStream = new ByteArrayInputStream((byte[]) data.get("data"));
                XSSFWorkbook xssfWorkbook            = new XSSFWorkbook(xssfWorkbookInputStream);

                for (int sheetIndex = 0; sheetIndex < xssfWorkbook.getNumberOfSheets(); sheetIndex++)
                {
                    String csv = generateCSVFromSheet(xssfWorkbook.getSheetAt(sheetIndex), xssfWorkbook.getCreationHelper().createFormulaEvaluator());

                    Map<String, Object> csvData = new HashMap<String, Object>();
                    if (xssfWorkbook.getNumberOfSheets() == 0)
                        csvData.put("filename", baseFilename + ".csv");
                    else
                        csvData.put("filename", baseFilename + "_" + xssfWorkbook.getSheetName(sheetIndex) + ".csv");
                    csvData.put("resourceformat", "csv");
                    csvData.put("data", csv.getBytes());

                    _dataProvider.produce(csvData);
                }
                xssfWorkbookInputStream.close();

//                xssfWorkbook.close(); // Add to newer version of API
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

    private String generateCSVFromSheet(XSSFSheet xssfSheet, FormulaEvaluator evaluator)
    {
        StringBuffer csvText = new StringBuffer();

        int firstCellNumber = Integer.MAX_VALUE;
        int lastCellNumber  = Integer.MIN_VALUE;
        for (int rowIndex = xssfSheet.getFirstRowNum(); rowIndex <= xssfSheet.getLastRowNum(); rowIndex++)
        {
            XSSFRow row = xssfSheet.getRow(rowIndex);

            if ((row.getFirstCellNum() >= 0) && (row.getFirstCellNum() < firstCellNumber))
                firstCellNumber = row.getFirstCellNum();

            if ((row.getLastCellNum() >= 0) && (row.getLastCellNum() > lastCellNumber))
                lastCellNumber = row.getLastCellNum() - 1; // "Gets the index of the last cell contained in this row plus one"!
        }

        if ((firstCellNumber != Integer.MAX_VALUE) && (lastCellNumber != Integer.MIN_VALUE))
            for (int rowIndex = xssfSheet.getFirstRowNum(); rowIndex <= xssfSheet.getLastRowNum(); rowIndex++)
            {
                XSSFRow row = xssfSheet.getRow(rowIndex);

                boolean firstCell = true;
                for (int columnIndex = firstCellNumber; columnIndex <= lastCellNumber; columnIndex++)
                {
                    if (firstCell)
                        firstCell = false;
                    else
                        csvText.append(',');

                    csvText.append(escapeCsv(generateCSVFromCell(row.getCell(columnIndex), evaluator)));
                }

                csvText.append('\n');
            }

        return csvText.toString();
    }

    private String generateCSVFromCell(Cell cell, FormulaEvaluator evaluator)
    {
        try
        {
            if ((cell != null) && (cell.getCellType() != Cell.CELL_TYPE_BLANK))
            {
                CellValue cellValue = evaluator.evaluate(cell);

                if (cellValue.getCellType() == Cell.CELL_TYPE_STRING)
                    return cellValue.getStringValue();
                else if (cellValue.getCellType() == Cell.CELL_TYPE_NUMERIC)
                {
                    if (DateUtil.isCellDateFormatted(cell))
                    {
                        CellStyle cellStyle = cell.getCellStyle();

                        String     excelDateFormat = cellStyle.getDataFormatString();
                        String     javaDateFormat  = excelToJavaDataFormat(excelDateFormat);
                        DateFormat dateFormat      = new SimpleDateFormat(javaDateFormat);
                        return dateFormat.format(cell.getDateCellValue());
                    }
                    else
                        return Double.toString(cellValue.getNumberValue());
                }
                else if (cellValue.getCellType() == Cell.CELL_TYPE_BOOLEAN)
                    return Boolean.toString(cellValue.getBooleanValue());
                else if (cellValue.getCellType() == Cell.CELL_TYPE_BLANK)
                    return "";
                else
                {
                    logger.log(Level.WARNING, "Problem process cell: Unknown CellValue Type = " + cellValue.getCellType());
                    return "";
                }
            }
            else
                return "";
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problem process cell: Unknown Cell Type", throwable);
            return "";
        }
    }

    private String excelToJavaDataFormat(String excelDateFormat)
    {
        if ("DD/MM/YY".equals(excelDateFormat))
            return "dd/MM/yy";
        else if ("DD/MM/YYYY".equals(excelDateFormat))
            return "dd/MM/yyyy";
        else if ("HH:MM".equals(excelDateFormat))
            return "HH:mm";
        else
            return excelDateFormat;
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