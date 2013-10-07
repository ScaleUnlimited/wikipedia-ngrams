package com.amazon.aws.training.emr.wikipedia;

public enum NgramCounters {

    PAGES_PARSED,               // Pages parsed
    PAGES_SKIPPED,              // Pages skipped due to percentage filtering
    PAGES_FAILED,               // Pages that couldn't be parsed
    
    NGRAMS_CREATED,             // Total ngrams (not unique)
    NGRAMS_UNIQUE_SAVED,        // Unique ngrams written out
    NGRAMS_UNIQUE_SKIPPED       // Unique ngrams skipped due to low count
}
