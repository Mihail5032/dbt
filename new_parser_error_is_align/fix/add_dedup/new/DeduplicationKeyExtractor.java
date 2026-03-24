package ru.x5.process;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.Unmarshaller;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.log4j.Logger;
import ru.x5.decoder.CustomDecoder;
import ru.x5.model.IDocWrapper;
import ru.x5.model.Transaction;

import java.io.InputStream;
import java.util.List;

/**
 * Извлекает ключ дедупликации (retailStoreId|businessDayDate|workstationId|transactionSequenceNumber)
 * из сырого Kafka-сообщения.
 *
 * Используется для keyBy() перед DeduplicationFilter, чтобы гарантировать
 * что одинаковые транзакции попадают на один и тот же инстанс.
 */
public class DeduplicationKeyExtractor implements KeySelector<String, String> {

    private static final Logger log = Logger.getLogger(DeduplicationKeyExtractor.class);

    private static final JAXBContext contextJAXB;

    static {
        try {
            contextJAXB = JAXBContext.newInstance(IDocWrapper.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public String getKey(String kafkaValue) throws Exception {
        try (InputStream inputStream = CustomDecoder.decodeAndDecompress(kafkaValue)) {
            Unmarshaller unmarshaller = contextJAXB.createUnmarshaller();
            unmarshaller.setEventHandler(event -> true); // игнорируем ошибки валидации
            IDocWrapper doc = (IDocWrapper) unmarshaller.unmarshal(inputStream);

            // Берём первый Transaction из IDoc — в нём 4 ключевых поля
            List<Transaction> transactions = doc.getIdoc()
                    .getPostrCreateMultip()
                    .getTransactions();

            if (transactions != null && !transactions.isEmpty()) {
                Transaction txn = transactions.get(0);
                return buildDeduplicationKey(
                        txn.getRetailStoreId(),
                        txn.getBusinessDayDate(),
                        txn.getWorkstationId(),
                        txn.getTransactionSequenceNumber()
                );
            }

            // Fallback: если Transaction нет — берём из Tender (тоже наследник BaseTransactionKey)
            List<? extends ru.x5.model.BaseTransactionKey> tenders = doc.getIdoc()
                    .getPostrCreateMultip()
                    .getTenders();

            if (tenders != null && !tenders.isEmpty()) {
                ru.x5.model.BaseTransactionKey first = tenders.get(0);
                return buildDeduplicationKey(
                        first.getRetailStoreId(),
                        first.getBusinessDayDate(),
                        first.getWorkstationId(),
                        first.getTransactionSequenceNumber()
                );
            }

        } catch (Exception e) {
            log.error("Failed to extract deduplication key, using raw hashCode as fallback", e);
        }

        // Fallback: если не смогли распарсить — используем хеш сообщения
        // Такие сообщения не дедуплицируются, но и не теряются
        return "UNKNOWN_" + kafkaValue.hashCode();
    }

    /**
     * Строит ключ дедупликации из 4 полей — те же поля, что и в rtl_txn_rk.
     * Формат: "retailStoreId|businessDayDate|workstationId|transactionSequenceNumber"
     */
    private String buildDeduplicationKey(String retailStoreId, String businessDayDate,
                                          String workstationId, String transactionSequenceNumber) {
        return String.join("|",
                retailStoreId != null ? retailStoreId : "",
                businessDayDate != null ? businessDayDate : "",
                workstationId != null ? workstationId : "",
                transactionSequenceNumber != null ? transactionSequenceNumber : ""
        );
    }
}
