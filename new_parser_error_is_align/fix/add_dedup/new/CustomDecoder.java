package ru.x5.decoder;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.Serializable;

public class CustomDecoder implements Serializable {

    public static InputStream decodeAndDecompress(String base64GzipData) throws Exception {
        // Декодирование Base64
        if (base64GzipData == null) {
            throw new FileNotFoundException("Cannot extract data from kafka");
        }
        byte[] decodedBytes = Base64.decodeBase64(base64GzipData);

        // Распаковка GZIP
        try (InputStream gzipStream = new GzipCompressorInputStream(new ByteArrayInputStream(decodedBytes));) {
            return new ByteArrayInputStream(gzipStream.readAllBytes());
        }
    }
}
