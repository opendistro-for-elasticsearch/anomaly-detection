package com.amazon.opendistroforelasticsearch.ad.model;

import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;

public class ValidateResponse implements ToXContentObject {
        private List<String> failures;
        private List<String> suggestedChanges;

        public XContentBuilder toXContent(XContentBuilder builder) throws IOException {
            return toXContent(builder, ToXContent.EMPTY_PARAMS);
        }

        public ValidateResponse() {
            failures = null;
            suggestedChanges = null;
        }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        XContentBuilder xContentBuilder = builder.startObject();

        System.out.println("inside x content");
        if (failures != null && failures.size() > 0) {
            xContentBuilder.array("failures", failures);

        }
        if (suggestedChanges != null && suggestedChanges.size() > 0) {
            xContentBuilder.array("suggestedChanges", suggestedChanges);

        }
        return xContentBuilder.endObject();
    }

    public List<String> getFailures() { return failures; }

    public List<String> getSuggestedChanges() { return suggestedChanges; }

    public void setFailures(List<String> failures) { this.failures = failures; }

    public void setSuggestedChanges(List<String> suggestedChanges) { this.suggestedChanges = suggestedChanges; }




}
