package org.jai.search.analytics;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.apache.hive.com.esotericsoftware.minlog.Log;
import org.elasticsearch.search.sort.SortOrder;
import org.jai.flume.agent.FlumeAgentService;
import org.jai.search.config.ElasticSearchIndexConfig;
import org.jai.search.model.FacetResult;
import org.jai.search.model.FacetResultEntry;
import org.jai.search.model.ProductSearchResult;
import org.jai.search.model.SearchCriteria;
import org.jai.search.model.SearchFacetName;
import org.jai.search.query.ProductQueryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy.PropertyNamingStrategyBase;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;

@Service
public class GenerateSearchAnalyticsDataImpl implements
		GenerateSearchAnalyticsDataService {
	// rolling file appender is used for events, so only log info events.
	private static final Logger searchEventsLogger = LoggerFactory
			.getLogger(GenerateSearchAnalyticsDataImpl.class);

	@Autowired
	private ProductQueryService productQueryService;

	@Autowired
	private FlumeAgentService flumeAgentService;

	@Override
	public void generateAndPushSearchEvents(final int numberOfEvents) {
		Assert.isTrue(numberOfEvents > 0,
				"Number of events should be greater than zero!");
		searchEventsLogger.debug("Starting generating data!");
		try {
			final SearchCriteria searchCriteria = getSearchCriteria();
			final ProductSearchResult searchProducts = productQueryService
					.searchProducts(searchCriteria);
			for (int i = 1, j = 1; i <= numberOfEvents; i++) {
				// sleep 30 secs every 1k requests...the further channel may not
				// be able to process that fast.
				if (i == 1000 * j) {
					System.out
							.println("Sleeping for 1 sec, a batch of 1000 records processing! current count: "
									+ i);
					Thread.sleep(1000);
					j++;
				}
				final SearchQueryInstruction searchQueryInstruction = getRandomSearchQueryInstruction(
						i, searchProducts);
				final Event event = getJsonEvent(searchQueryInstruction);
				flumeAgentService.getFlumeAgent().put(event);
			}

			// wait for 5 sec before all events are submitted...in test case
			// agent
			// is destroyed.
			System.out
					.println("Sleeping for 5 sec to wait for search events to be processed!");
			Thread.sleep(5000);
			flumeAgentService.processAllEvents();
		} catch (Exception e) {
			String errMsg = "Error occured while generating search events data!";
			Log.error(errMsg, e);
			throw new RuntimeException(errMsg, e);
		}
	}

	@Override
	public List<Event> getSearchEvents(int numberOfEvents) {
		try {
			List<Event> events = new ArrayList<>();
			final SearchCriteria searchCriteria = getSearchCriteria();
			final ProductSearchResult searchProducts = productQueryService
					.searchProducts(searchCriteria);

			for (int i = 1; i <= numberOfEvents; i++) {
				final SearchQueryInstruction searchQueryInstruction = getRandomSearchQueryInstruction(
						i, searchProducts);
				final Event event = getJsonEvent(searchQueryInstruction);
				events.add(event);
			}
			return events;
		} catch (UnknownHostException | JsonProcessingException ex) {
			throw new RuntimeException(
					"Error occured while generating search events!", ex);
		}
	}

	public Event getJsonEvent(
			final SearchQueryInstruction searchQueryInstruction)
			throws JsonProcessingException {
		final String searchQueryInstructionAsString = getObjectMapper()
				.writeValueAsString(searchQueryInstruction);
		// String writeValueAsString =
		// mapper.writerWithDefaultPrettyPrinter().writeValueAsString(searchQueryInstruction);
		searchEventsLogger.info(searchQueryInstructionAsString);
		final Event event = new JSONEvent();
		event.setBody(searchQueryInstructionAsString.getBytes());
		final Map<String, String> headers = new HashMap<String, String>();
		headers.put("eventId", searchQueryInstruction.getEventIdSuffix());
		headers.put("timestamp", searchQueryInstruction
				.getCreatedTimeStampInMillis().toString());
		if (searchQueryInstruction.getClickedDocId() != null) {
			if (searchQueryInstruction.getFavourite() != null
					&& searchQueryInstruction.getFavourite()) {
				headers.put("State", "FAVOURITE");
			} else {
				headers.put("State", "VIEWED");
			}
		}
		event.setHeaders(headers);
		return event;
	}

	public ObjectMapper getObjectMapper() throws JsonProcessingException {
		final ObjectMapper mapper = new ObjectMapper();
		// try without pretty print..all data in single line
		return mapper
				.setPropertyNamingStrategy(
						new SearchFieldsLowerCaseNameStrategy())
				.setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
				.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
	}

	private SearchCriteria getSearchCriteria() {
		final ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
		final SearchCriteria searchCriteria = new SearchCriteria()
				.indices(config.getIndexAliasName());
		searchCriteria.documentTypes(config.getDocumentType());
		searchCriteria.size(50);
		for (final SearchFacetName facet : SearchFacetName.categoryFacets) {
			searchCriteria.facets(facet.getFacetFieldNameAtLevel(2));
		}
		searchCriteria.facets(SearchFacetName.PRODUCT_PRICE_RANGE.getCode());
		searchCriteria.facets(SearchFacetName.PRODUCT_PROPERTY_SIZE.getCode());
		searchCriteria.facets(SearchFacetName.PRODUCT_PROPERTY_COLOR.getCode());
		searchCriteria.facets(SearchFacetName.SPECIFICATION_RESOLUTION
				.getCode());
		searchCriteria.facets(SearchFacetName.SPECIFICATION_MEMORY.getCode());
		return searchCriteria;
	}

	public SearchQueryInstruction getRandomSearchQueryInstruction(
			final int recordNumber, final ProductSearchResult searchProducts)
			throws UnknownHostException {
		final SearchQueryInstruction searchQueryInstruction = new SearchQueryInstruction();
		// 5 machines
		searchQueryInstruction.setHostedMachineName(InetAddress.getLocalHost()
				.getHostAddress() + new Random().nextInt(5));
		// same customer id repeated twice.
		final Long customerId = Long.valueOf(new Random().nextInt(500));
		searchQueryInstruction.setCustomerId(customerId);
		// Event id combination of uniqueuuid + timestamp + customerid
		// Changing event id to customerid + timestamp + unique id (to test search by customer id in hbase row key)
		final String eventId = customerId + "_" +  searchQueryInstruction.getCreatedTimeStampInMillis()
				 + "-" + searchQueryInstruction.getEventIdSuffix();
		searchQueryInstruction.setEventId(eventId);
		// random url
		searchQueryInstruction.setPageUrl("http://blahblah:/"
				+ new Random().nextInt(recordNumber));
		//
		if (new Random().nextBoolean()) {
			searchQueryInstruction.setQueryString("queryString"
					+ new Random().nextInt(20));
		}
		// random product id for odd
		final String clickedDocId = recordNumber % 2 == 0 ? null : String
				.valueOf(new Random().nextInt(50));
		searchQueryInstruction.setClickedDocId(clickedDocId);
		// set favourite
		if (searchQueryInstruction.getCustomerId() != null
				&& searchQueryInstruction.getClickedDocId() != null
				&& new Random().nextBoolean()) {
			searchQueryInstruction.setFavourite(true);
		}
		//
		searchQueryInstruction.setHitsShown(Long.valueOf(new Random()
				.nextInt(50)));
		//
		searchQueryInstruction.setTotalHits(Long.valueOf(new Random()
				.nextInt(50)));
		// lets say 10 per page, 5 pages etc.
		searchQueryInstruction.setPageNumber(Long.valueOf(new Random()
				.nextInt(5)));
		//
		searchQueryInstruction.setSessionId(UUID.randomUUID().toString());
		//
		final String sortOrder = recordNumber % 2 == 0 ? SortOrder.ASC
				.toString() : SortOrder.DESC.toString();
		searchQueryInstruction.setSortOrder(sortOrder);
		//
		final Map<String, Set<String>> filters = getRandomFilters(searchProducts);
		searchQueryInstruction.setFilters(filters);
		return searchQueryInstruction;
	}

	private Map<String, Set<String>> getRandomFilters(
			final ProductSearchResult searchProducts) {
		final Map<String, Set<String>> filters = new LinkedHashMap<String, Set<String>>();
		// Add once
		final FacetResult facetResult = searchProducts.getFacets().get(
				new Random().nextInt(6));
		FacetResultEntry facetResultEntry = null;
		if (facetResult.getFacetResultEntries().size() > 1) {
			facetResultEntry = facetResult.getFacetResultEntries().get(
					new Random().nextInt(facetResult.getFacetResultEntries()
							.size() - 1));
		} else {
			facetResultEntry = facetResult.getFacetResultEntries().get(0);
		}
		final Set<String> list = new HashSet<String>();
		list.add(facetResultEntry.getTerm());
		filters.put(facetResult.getCode(), list);
		// try randomly again
		secondfilter: if (new Random().nextBoolean()) {
			final FacetResult facetResult2 = searchProducts.getFacets().get(
					new Random().nextInt(6));
			FacetResultEntry facetResultEntry2 = null;
			// if no entries, break the label.
			if (facetResult2.getFacetResultEntries().size() == 0) {
				break secondfilter;
			}
			if (facetResult2.getFacetResultEntries().size() > 1) {
				facetResultEntry2 = facetResult2.getFacetResultEntries().get(
						new Random().nextInt(facetResult2
								.getFacetResultEntries().size() - 1));
			} else {
				facetResultEntry2 = facetResult2.getFacetResultEntries().get(0);
			}
			final Set<String> list2 = new HashSet<String>();
			list2.add(facetResultEntry2.getTerm());
			final String key2 = facetResult2.getCode();
			if (filters.containsKey(key2)) {
				filters.get(key2).add(facetResultEntry2.getTerm());
			} else {
				filters.put(key2, list2);
			}
		}
		return filters;
	}

	@Override
	public String generateSearchQueryInstructionJsonSchema() {
		String jsonSchemaAsString = null;
		try {
			ObjectMapper mapper = getObjectMapper();
			SchemaFactoryWrapper visitor = new SchemaFactoryWrapper();
			mapper.acceptJsonFormatVisitor(
					mapper.constructType(SearchQueryInstruction.class), visitor);
			JsonSchema schema = visitor.finalSchema();
			jsonSchemaAsString = mapper.writeValueAsString(schema);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Error occured generating json schema!",
					e);
		}
		return jsonSchemaAsString;
	}

	@Override
	public String generateSearchQueryInstructionPIGJsonSchema() {
		// If default JsonLoader is used, sequencing of schema fields is
		// important.
		// Same order as the data is written in event.
		return "eventid:chararray, hostedmachinename:chararray, pageurl:chararray,"
				+ "customerid:long, sessionid:chararray, querystring:chararray, sortorder:chararray,"
				+ "pagenumber:int, totalhits:int, hitsshown:int,"
				+ "createdtimestampinmillis:long, clickeddocid:chararray, favourite:boolean,"
				+ "eventidsuffix:chararray, filters:{(value:chararray, code:chararray)}";
	}

	@SuppressWarnings("serial")
	public static class SearchFieldsLowerCaseNameStrategy extends
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
}
