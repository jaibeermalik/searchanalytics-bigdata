package org.jai.search.actors;

import org.jai.search.config.ElasticSearchIndexConfig;
import org.jai.search.config.IndexDocumentType;
import org.jai.search.data.SampleDataGeneratorService;
import org.jai.search.exception.DocumentTypeIndexingException;
import org.jai.search.exception.IndexingException;
import org.jai.search.index.IndexProductDataService;
import org.jai.search.setup.SetupIndexService;

import org.apache.commons.lang.StringUtils;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.FromConfig;

public class SetupIndexWorkerActor extends UntypedActor
{
    final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    private final SetupIndexService setupIndexService;

    private ElasticSearchIndexConfig config;

    private String newIndexName;

    private final Map<IndexDocumentType, Boolean> documentTypesDone = new HashMap<IndexDocumentType, Boolean>();

    private final ActorRef workerRouter;

    public SetupIndexWorkerActor(final SetupIndexService setupIndexService, final SampleDataGeneratorService sampleDataGeneratorService,
            final IndexProductDataService indexProductDataService)
    {
        this.setupIndexService = setupIndexService;
        workerRouter = getContext().actorOf(
                Props.create(SetupDocumentTypeWorkerActor.class, sampleDataGeneratorService, indexProductDataService)
                        .withDispatcher("setupDocumentTypeWorkerActorDispatcher").withRouter(new FromConfig()),
                "setupDocumentTypeWorkerActor");
    }

    @Override
    public void onReceive(final Object message)
    {
        LOG.debug("Worker Actor message for SetupIndexWorkerActor is:" + message);
        try
        {
            // message from master actor
            if (message instanceof ElasticSearchIndexConfig)
            {
                handleEachIndex(message);
            }
            else if (message instanceof IndexDocumentType)
            {
                handleIndexDocumentType(message);
            }
            else if (message instanceof Exception)
            {
                handleException(message);
            }
            else
            {
                unhandled(message);
            }
        }
        catch (final Exception ex)
        {
            // TODO: check if it needs to be handled different, not sure how much indexing for index or types is already done.
            final String errorMessage = "Error occurred while indexing: " + message;
            LOG.error(ex, errorMessage);
            final IndexingException indexingException = new IndexingException(config, errorMessage, ex);
            sendMessageToParent(indexingException);
        }
    }

    private void handleIndexDocumentType(final Object message)
    {
        final IndexDocumentType indexDocumentType = (IndexDocumentType) message;
        documentTypesDone.put(indexDocumentType, true);
        updateStateAndNotifyParentIfAllDone();
    }

    private void handleException(final Object message)
    {
        final Exception ex = (Exception) message;
        if (ex instanceof DocumentTypeIndexingException)
        {
            documentTypesDone.put(((DocumentTypeIndexingException) ex).getIndexDocumentType(), true);
            updateStateAndNotifyParentIfAllDone();
        }
        else
        {
            unhandled(message);
        }
    }

    // private void handleIndexingStatusCheckForEachIndex(final Object message)
    // {
    // final IndexingMessage indexingMessage = (IndexingMessage) message;
    // if (IndexingMessage.DOCUMENTTYPE_DONE.equals(indexingMessage))
    // {
    // totalDocumentTypesToIndexDone++;
    // updateStateAndNotifyParentIfAllDone();
    // }
    // else
    // {
    // unhandled(message);
    // }
    // }
    private void handleEachIndex(final Object message)
    {
        LOG.debug("Worker Actor message for initial config is  received");
        // Each worker is supposed to handle one index.
        config = (ElasticSearchIndexConfig) message;
        newIndexName = setupIndexService.createNewIndex(config);
        Assert.isTrue(StringUtils.isNotBlank(newIndexName));
        // Loop through all document types and index relevant products for those.
        // eg. you want to index products/product group/specifications separately.
        // This only works if no parent child stuff other order indexing accordingly.
        // Index products
        indexDocumentType(config, IndexDocumentType.PRODUCT);
        // Index product property
        indexDocumentType(config, IndexDocumentType.PRODUCT_PROPERTY);
        // Index product group
        indexDocumentType(config, IndexDocumentType.PRODUCT_GROUP);
    }

    private void indexDocumentType(final ElasticSearchIndexConfig config, final IndexDocumentType indexDocumentType)
    {
        workerRouter.tell(new IndexDocumentTypeMessageVO().config(config).documentType(indexDocumentType).newIndexName(newIndexName),
                getSelf());
        documentTypesDone.put(indexDocumentType, false);
    }

    private void updateStateAndNotifyParentIfAllDone()
    {
        boolean isAlldocumentTypeDone = true;
        for (final Entry<IndexDocumentType, Boolean> entry : documentTypesDone.entrySet())
        {
            LOG.debug("Total indexing stats are: documentType: {}, DocumentTypesStatus: {}",
                    new Object[] { entry.getKey(), entry.getValue() });
            if (!entry.getValue())
            {
                isAlldocumentTypeDone = false;
            }
        }
        if (isAlldocumentTypeDone)
        {
            // TODO: index type should have been done. shift alising for newly created indices now.
            setupIndexService.replaceAlias(newIndexName, config.getIndexAliasName());
            // sendMessageToParent(config);
            sendMessageToParent(config);
            LOG.debug("All indexing done for the index: {} {}", new Object[] { newIndexName, config });
            documentTypesDone.clear();
            stopTheActor();
        }
    }

    private void sendMessageToParent(final Object message)
    {
        // Find parent actor in the hierarchy.
        // akka://SearchIndexingSystem/user/setupIndexMasterActor/setupIndexWorkerActor/$a
        getContext().actorSelection("../../").tell(message, null);
    }

    private void stopTheActor()
    {
        // Stop the actors
        // TODO: stopping this won't restart automatically, check it.
        // getContext().stop(getSelf());
    }
}
