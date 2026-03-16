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
public class PostVoidDetails extends BaseTransactionKey {
    @XmlElement(name = "VOIDEDRETAILSTOREID")
    private String voidedRetailStoreId;
    @XmlElement(name = "VOIDEDBUSINESSDAYDATE")
    private String voidedBusinessDayDate;
    @XmlElement(name = "VOIDEDWORKSTATIONID")
    private String voidedWorkstationId;
    @XmlElement(name = "VOIDEDTRANSACTIONSEQUENCENUMBE")
    private String voidedTransactionSequenceNumbe;
    @XmlElement(name = "VOIDFLAG")
    private String voidFlag;

    @Override
    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        RowTablePart basePart = super.toRowTablePart();
        return RowTablePart.fromBase(basePart).segment_name(getSegmentName()).voidedretailstoreid(voidedRetailStoreId)
                .voidedbusinessdaydate(voidedBusinessDayDate).voidedworkstationid(voidedWorkstationId)
                .voidedtransactionsequencenumbe(voidedTransactionSequenceNumbe)
                .voidflag(voidFlag).build()
                .toRowData(icebergSchema, timestampDataXml, dateXml);
    }
    @Override
    public String getSegmentName(){return "E1BPPOSTVOIDDETAILS";}
}
