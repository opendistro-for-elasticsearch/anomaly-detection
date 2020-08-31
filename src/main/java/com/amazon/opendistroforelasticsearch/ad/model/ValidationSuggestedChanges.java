package com.amazon.opendistroforelasticsearch.ad.model;

public enum ValidationSuggestedChanges {
    OTHERS("others"),
    FILTER_QUERY("filter_query"),
    FEATURE_ATTRIBUTES("feature_attributes"),
    DETECTION_INTERVAL("detection_interval"),
    WINDOW_DELAY("window_delay");

    private String name;

    /**
     * Get stat name
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    ValidationSuggestedChanges(String name) {
        this.name = name;
    }

}
