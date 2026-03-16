package ru.x5.model;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlElement;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;

import java.time.LocalDate;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@XmlAccessorType(XmlAccessType.FIELD)
public class LineItemTax extends BaseTransactionKey {
    @XmlElement(name = "RETAILSEQUENCENUMBER")
    private String retailSequenceNumber;
    @XmlElement(name = "TAXSEQUENCENUMBER")
    private String taxSequenceNumber;
    @XmlElement(name = "TAXTYPECODE")
    private String taxTypeCode;
    @XmlElement(name = "TAXAMOUNT")
    private String taxAmount;

    @Override
    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        RowTablePart basePart = super.toRowTablePart();
        return RowTablePart.fromBase(basePart).segment_name(getSegmentName()).retailsequencenumber(retailSequenceNumber)
                .taxsequencenumber(taxSequenceNumber).taxtypecode(taxTypeCode).taxamount(taxAmount).build()
                .toRowData(icebergSchema, timestampDataXml, dateXml);
    }
    @Override
    public String getSegmentName(){return "E1BPLINEITEMTAX";}
}
