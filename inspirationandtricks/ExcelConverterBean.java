package org.ki.meb.geneconnector;

import java.io.InputStream;
import java.util.Iterator;
import java.util.GregorianCalendar;
import java.math.BigDecimal;
import javax.xml.datatype.DatatypeFactory;
import org.apache.camel.Body;
import org.apache.activemq.camel.tutorial.partners.invoice.Invoice;
import org.apache.activemq.camel.tutorial.partners.invoice.ObjectFactory;
import org.apache.activemq.camel.tutorial.partners.invoice.LineItemType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFCell;

/**
 * Uses POI to convert an Excel spreadsheet to the desired JAXB XML format.
 */
public class ExcelConverterBean {
    private final static Log log = LogFactory.getLog(ExcelConverterBean.class);

    public Invoice processExcelInvoice(@Body InputStream body) {
        ObjectFactory factory = new ObjectFactory();
        Invoice invoice = factory.createInvoice();
        try {
            HSSFWorkbook workbook = new HSSFWorkbook(body);
            HSSFSheet sheet = workbook.getSheetAt(0);
            DatatypeFactory dateFactory = DatatypeFactory.newInstance();
            boolean headersFound = false;
            int colNum;
            for(Iterator rit = sheet.rowIterator(); rit.hasNext();) {
                HSSFRow row = (HSSFRow) rit.next();
                if(!headersFound) {  // Skip the first row with column headers
                    headersFound = true;
                    continue;
                }
                colNum = 0;
                LineItemType item = factory.createLineItemType();
                for(Iterator cit = row.cellIterator(); cit.hasNext(); ++colNum) {
                    HSSFCell cell = (HSSFCell) cit.next();
                    if(headersFound)
                    switch(colNum) {
                        case 0: // Date
                            GregorianCalendar calendar = new GregorianCalendar();
                            calendar.setTime(cell.getDateCellValue());
                            item.setOrderDate(dateFactory.newXMLGregorianCalendar(calendar));
                            break;
                        case 1: // Price
                            item.setItemPrice(new BigDecimal(cell.getNumericCellValue()).setScale(2, BigDecimal.ROUND_HALF_UP));
                            break;
                        case 2: // Quantity
                            item.setQuantity((int)cell.getNumericCellValue());
                            break;
                        case 3: // Total
                            BigDecimal total = new BigDecimal(cell.getNumericCellValue()).setScale(2, BigDecimal.ROUND_HALF_UP);
                            BigDecimal computed = item.getItemPrice().multiply(new BigDecimal(item.getQuantity()));
                            if(!total.equals(computed)) {
                                log.warn("COMPUTED TOTAL INVALID ("+item.getItemPrice()+"x"+item.getQuantity()+" <> "+total);
                            }
                            break;
                        case 4: // Name
                            item.setDescription(cell.getRichStringCellValue().getString());
                            break;
                        case 5: // ID
                            item.setProductId((long)cell.getNumericCellValue());
                            break;
                    }
                }
                invoice.getLineItem().add(item);
            }
        } catch (Exception e) {
            log.error("Unable to import Excel invoice", e);
            throw new RuntimeException("Unable to import Excel invoice", e);
        }
        return invoice;
    }
}
