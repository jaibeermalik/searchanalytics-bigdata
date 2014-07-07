package org.jai.flume.sinks.hbase.serializer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.jai.search.analytics.SearchQueryInstruction;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.PropertyNamingStrategy.PropertyNamingStrategyBase;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedField;
import com.fasterxml.jackson.databind.introspect.AnnotatedMethod;
import com.google.common.collect.Lists;

public class HbaseJsonEventSerializer implements HbaseEventSerializer {

	public static final byte[] COLUMFAMILY_CLIENT_BYTES = "client".getBytes();
	public static final byte[] COLUMFAMILY_CUSTOMER_BYTES = "customer"
			.getBytes();
	public static final byte[] COLUMFAMILY_SEARCH_BYTES = "search".getBytes();
	public static final byte[] COLUMFAMILY_FILTERS_BYTES = "filters".getBytes();
	private byte[] payload;
	private SearchQueryInstruction searchQueryInstruction;

	private Map<String, String> headers;
	private boolean depositHeaders;
	public static final String CHARSET_DEFAULT = "UTF-8";

	@Override
	public void configure(Context arg0) {
		// configure local properties here.
	}

	@Override
	public void configure(ComponentConfiguration arg0) {
		// can leave blank.
	}

	@Override
	public void initialize(Event event, byte[] columnFamily) {
		this.headers = event.getHeaders();
		this.payload = event.getBody();
		this.depositHeaders = false;
		setSearchQueryInstruction();
		// hard coding column family impl here for now.
	}

	private void setSearchQueryInstruction() {
		try {
			ObjectMapper mapper = getObjectMapper();
			searchQueryInstruction = mapper.readValue(payload,
					SearchQueryInstruction.class);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(
					"Error generating bean from event body!", e);
		}
	}

	@Override
	public List<Row> getActions() {
		List<Row> actions = Lists.newArrayList();
		try {
			Put put = getRow();
			actions.add(put);
		} catch (Exception e) {
			throw new FlumeException("Could not get row key!", e);
		}
		return actions;
	}

	private Put getRow() throws UnsupportedEncodingException {
		byte[] rowKey = searchQueryInstruction.getEventId().getBytes(
				CHARSET_DEFAULT);
		Put put = new Put(rowKey);

		// Client Infor
		put.add(COLUMFAMILY_CLIENT_BYTES, "eventid".getBytes(),
				searchQueryInstruction.getEventId().getBytes());
		put.add(COLUMFAMILY_CLIENT_BYTES,
				"createdtimestampinmillis".getBytes(),
				String.valueOf(
						searchQueryInstruction.getCreatedTimeStampInMillis())
						.getBytes());
		put.add(COLUMFAMILY_CLIENT_BYTES, "hostedmachinename".getBytes(),
				String.valueOf(searchQueryInstruction.getHostedMachineName())
						.getBytes());
		put.add(COLUMFAMILY_CLIENT_BYTES, "pageurl".getBytes(),
				String.valueOf(searchQueryInstruction.getPageUrl()).getBytes());

		// Customer Info
		put.add(COLUMFAMILY_CUSTOMER_BYTES, "customerid".getBytes(), String
				.valueOf(searchQueryInstruction.getCustomerId()).getBytes());
		put.add(COLUMFAMILY_CUSTOMER_BYTES, "sessionid".getBytes(), String
				.valueOf(searchQueryInstruction.getSessionId()).getBytes());

		// Search Column Family Info
		if (searchQueryInstruction.getQueryString() != null) {
			put.add(COLUMFAMILY_SEARCH_BYTES,
					"querystring".getBytes(),
					searchQueryInstruction.getQueryString().getBytes(
							CHARSET_DEFAULT));

		}
		if (searchQueryInstruction.getClickedDocId() != null) {
			put.add(COLUMFAMILY_SEARCH_BYTES, "clickeddocid".getBytes(),
					searchQueryInstruction.getClickedDocId().getBytes());
		}
		put.add(COLUMFAMILY_SEARCH_BYTES, "sortorder".getBytes(),
				searchQueryInstruction.getSortOrder().getBytes());
		put.add(COLUMFAMILY_SEARCH_BYTES, "pagenumber".getBytes(), String
				.valueOf(searchQueryInstruction.getPageNumber()).getBytes());
		put.add(COLUMFAMILY_SEARCH_BYTES, "totalhits".getBytes(), String
				.valueOf(searchQueryInstruction.getTotalHits()).getBytes());
		put.add(COLUMFAMILY_SEARCH_BYTES, "hitsshown".getBytes(), String
				.valueOf(searchQueryInstruction.getTotalHits()).getBytes());
		if (searchQueryInstruction.getFavourite() != null) {
			put.add(COLUMFAMILY_SEARCH_BYTES, "favourite".getBytes(), String
					.valueOf(searchQueryInstruction.getFavourite()).getBytes());
		}

		// Filters Column Family Info
		if (searchQueryInstruction.getFacetFilters() != null) {
			for (SearchQueryInstruction.FacetFilter filter : searchQueryInstruction
					.getFacetFilters()) {
				put.add(COLUMFAMILY_FILTERS_BYTES, filter.getCode().getBytes(),
						filter.getValue().getBytes());
			}
		}

		// Headers Column Family Info
		if (depositHeaders) {
			for (Map.Entry<String, String> entry : headers.entrySet()) {
				put.add("headers".getBytes(),
						entry.getKey().getBytes(CHARSET_DEFAULT), entry
								.getValue().getBytes(CHARSET_DEFAULT));
			}
		}
		return put;
	}

	@Override
	public List<Increment> getIncrements() {
		return Lists.newArrayList();
	}

	@Override
	public void close() {
		// Nothing to do.
	}

	public SearchQueryInstruction getSearchQueryInstruction() {
		return searchQueryInstruction;
	}

	public Map<String, String> getHeaders() {
		return headers;
	}

	private ObjectMapper getObjectMapper() throws JsonProcessingException {
		final ObjectMapper mapper = new ObjectMapper();
		// try without pretty print..all data in single line
		return mapper
				.setPropertyNamingStrategy(
						new SearchFieldsLowerCaseNameStrategy())
				.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
				// .setPropertyNamingStrategy(
				// new SearchPropertyNameCaseInsensitiveNameStrategy())
				.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
	}

	@SuppressWarnings("serial")
	private static class SearchFieldsLowerCaseNameStrategy extends
			PropertyNamingStrategyBase {
		@Override
		public String translate(final String input) {
			if (input == null) {
				return input;
			}
			final int length = input.length();
			final StringBuilder result = new StringBuilder(length * 2);
			int resultLength = 0;
			for (int i = 0; i < length; i++) {
				char c = input.charAt(i);
				if (i > 0 || c != '_') // skip first starting underscore
				{
					if (Character.isUpperCase(c)) {
						c = Character.toLowerCase(c);
					}
					result.append(c);
					resultLength++;
				}
			}
			return resultLength > 0 ? result.toString() : input;
		}
	}

	@SuppressWarnings("serial")
	public class SearchPropertyNameCaseInsensitiveNameStrategy extends
			PropertyNamingStrategy {
		@SuppressWarnings("rawtypes")
		@Override
		public String nameForField(MapperConfig config, AnnotatedField field,
				String defaultName) {
			return convert(defaultName);

		}

		@SuppressWarnings("rawtypes")
		@Override
		public String nameForGetterMethod(MapperConfig config,
				AnnotatedMethod method, String defaultName) {
			return convert(defaultName);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public String nameForSetterMethod(MapperConfig config,
				AnnotatedMethod method, String defaultName) {
			String a = convert(defaultName);
			return a;
		}

		public String convert(String defaultName) {
			char[] arr = defaultName.toCharArray();
			if (arr.length != 0) {
				if (Character.isLowerCase(arr[0])) {
					char upper = Character.toUpperCase(arr[0]);
					arr[0] = upper;
				}
			}
			return new StringBuilder().append(arr).toString();
		}

	}

}
