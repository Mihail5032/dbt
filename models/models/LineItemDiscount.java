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
public class LineItemDiscount extends BaseTransactionKey {
    @XmlElement(name = "RETAILSEQUENCENUMBER")
    private String retailSequenceNumber;
    @XmlElement(name = "DISCOUNTSEQUENCENUMBER")
    private String discountSequenceNumber;
    @XmlElement(name = "DISCOUNTTYPECODE")
    private String discountTypeCode;
    @XmlElement(name = "DISCOUNTREASONCODE")
    private String discountReasonCode;
    @XmlElement(name = "REDUCTIONAMOUNT")
    private String reductionAmount;
    @XmlElement(name = "STOREFINANCIALLEDGERACCOUNTID")
    private String storeFinancialLedgerAccountId;
    @XmlElement(name = "DISCOUNTID")
    private String discountId;
    @XmlElement(name = "DISCOUNTIDQUALIFIER")
    private String discountIdQualifier;
    @XmlElement(name = "BONUSBUYID")
    private String bonusBuyId;
    @XmlElement(name = "OFFERID")
    private String offerId;

    @Override
    public RowData toRowData(Schema icebergSchema, TimestampData timestampDataXml, LocalDate dateXml) {
        RowTablePart basePart = super.toRowTablePart();
        return RowTablePart.fromBase(basePart).segment_name(getSegmentName()).retailsequencenumber(retailSequenceNumber)
                .discountsequencenumber(discountSequenceNumber).discounttypecode(discountTypeCode)
                .discountreasoncode(discountReasonCode).reductionamount(reductionAmount)
                .storefinancialledgeraccountid(storeFinancialLedgerAccountId)
                .discountid(discountId).discountidqualifier(discountIdQualifier)
                .bonusbuyid(bonusBuyId).offerid(offerId).build().toRowData(icebergSchema, timestampDataXml, dateXml);
    }
    @Override
    public String getSegmentName(){return "E1BPLINEITEMDISCOUNT";}

    public String createKeyLineItem(){
        return retailStoreId+businessDayDate+transactionTypeCode+workstationId
                +transactionSequenceNumber+retailSequenceNumber;
    }

    public RowData toRowDataPst(Schema pstSchema, TimestampData timestampDataXml, LocalDate dateXml){
        return toRowData(pstSchema, timestampDataXml, dateXml); //доп. преобразований нет
    }
}