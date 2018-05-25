package com.mgoode;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

/**
 * Created by michaelgoode on 27/04/2018.
 */
public class MapForceDelimitedLineTokenizer extends DelimitedLineTokenizer {

    @Override
    protected boolean isQuoteCharacter(char c) {
        return false;
    }
}
