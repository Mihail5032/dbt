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
public class LineItemVoid extends BaseTransactionKey {
    @XmlElement(name = "RETAILSEQUENCENUMBER")
    private String retailSequenceNumber;
    @XmlElement(name = "VOIDEDLINE")
    private String voidedLine;
    @XmlElement(name = "VOIDFLAG")
    private String voidFlag;

    @Override
    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        RowTablePart basePart = super.toRowTablePart();
        return RowTablePart.fromBase(basePart).segment_name(getSegmentName()).retailsequencenumber(retailSequenceNumber)
                .voidedline(voidedLine).voidflag(voidFlag).build()
                .toRowData(icebergSchema, timestampDataXml, dateXml);
    }
    @Override
    public String getSegmentName(){return "E1BPLINEITEMVOID";}
}
