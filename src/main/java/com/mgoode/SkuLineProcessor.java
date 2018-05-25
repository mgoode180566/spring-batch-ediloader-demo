package com.mgoode;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

/**
 * Created by michaelgoode on 23/04/2018.
 */
@Component
public class SkuLineProcessor implements ItemProcessor<SkuLine, SkuLine> {

    @Override
    public SkuLine process(SkuLine skuLine) throws Exception {
        skuLine.setIMPORTTIME("NOW...");
        return skuLine;
    }


}