package com.swt.batchwriters.listener;

import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.item.file.FlatFileParseException;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class ProductSkipListener {
    private String errFileName = "error/read_skipped";

    @OnSkipInRead
    public void onSkipRead(Throwable t){
        if( t instanceof FlatFileParseException){
            FlatFileParseException ffpe = (FlatFileParseException) t;
            onSkip(ffpe.getInput(), errFileName);
        }
    }

    public void onSkip(Object o, String fileName) {
        FileOutputStream fos = null;

        try{
            fos = new FileOutputStream(fileName, true);
            fos.write(o.toString().getBytes());
            fos.write("\r\n".getBytes());
            fos.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
