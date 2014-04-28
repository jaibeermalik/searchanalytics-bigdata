package org.jai.flume.sinks.elasticsearch.serializer;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.elasticsearch.ContentBuilderUtil;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

public class ElasticSearchJsonBodyEventSerializer implements ElasticSearchEventSerializer
{
    @Override
    public void configure(final Context arg0)
    {
        // NO-OP...
    }

    @Override
    public void configure(final ComponentConfiguration arg0)
    {
        // NO-OP...
    }

    @Override
    public BytesStream getContentBuilder(final Event event) throws IOException
    {
        final XContentBuilder builder = jsonBuilder().startObject();
        appendBody(builder, event);
        appendHeaders(builder, event);
        return builder;
    }

    private void appendBody(final XContentBuilder builder, final Event event) throws IOException, UnsupportedEncodingException
    {
        addComplexField(builder, "body", XContentType.JSON, event.getBody());
    }

    private void appendHeaders(final XContentBuilder builder, final Event event) throws IOException
    {
        final Map<String, String> headers = event.getHeaders();
        for (final String key : headers.keySet())
        {
            ContentBuilderUtil.appendField(builder, key, headers.get(key).getBytes(charset));
        }
    }

    public void addComplexField(final XContentBuilder builder, final String fieldName, final XContentType contentType, final byte[] data)
            throws IOException
    {
        builder.field(fieldName, JsonXContent.jsonXContent.createParser(data).mapAndClose());
    }
}
