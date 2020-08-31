package com.amazon.opendistroforelasticsearch.ad.model;

public enum ValidationFailures {
    MISSING("missing"),
    OTHERS("others"),
    FIELD_TYPE("field_type"),
    FORMAT("format"),
    DUPLICATES("duplicates");

    private String name;

    /**
     * Get stat name
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    ValidationFailures(String name) {
        this.name = name;
    }

}
