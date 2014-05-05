package org.jai.search.actors;

import org.jai.search.exception.IndexDataException;
import org.jai.search.index.IndexProductDataService;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class IndexProductDataWorkerActor extends UntypedActor
{
    final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    private final IndexProductDataService indexProductDataService;

    public IndexProductDataWorkerActor(final IndexProductDataService indexProductDataService)
    {
        this.indexProductDataService = indexProductDataService;
    }

    @Override
    public void onReceive(final Object message)
    {
        // LOG.debug("Worker Actor message for IndexProductDataWorkerActor is:" + message);
        if (message instanceof IndexDocumentVO)
        {
            try
            {
                final IndexDocumentVO indexDocumentVO = (IndexDocumentVO) message;
                switch (indexDocumentVO.getDocumentType())
                {
                    case PRODUCT:
                        indexProductDataService.indexProduct(indexDocumentVO.getConfig(), indexDocumentVO.getNewIndexName(),
                                indexDocumentVO.getProduct());
                        indexDocumentVO.indexDone(true);
                        getSender().tell(indexDocumentVO, getSelf());
                        break;
                    case PRODUCT_PROPERTY:
                        indexProductDataService.indexProductPropterty(indexDocumentVO.getConfig(), indexDocumentVO.getNewIndexName(),
                                indexDocumentVO.getProductProperty());
                        indexDocumentVO.indexDone(true);
                        getSender().tell(indexDocumentVO, getSelf());
                        break;
                    case PRODUCT_GROUP:
                        indexProductDataService.indexProductGroup(indexDocumentVO.getConfig(), indexDocumentVO.getNewIndexName(),
                                indexDocumentVO.getProductGroup());
                        indexDocumentVO.indexDone(true);
                        getSender().tell(indexDocumentVO, getSelf());
                        break;
                    default:
                        handleUnhandledMessage(message);
                }
            }
            catch (final Exception e)
            {
                LOG.error(e, "Error occured while indexing document data for message: {}", message);
                final IndexDataException indexDataException = new IndexDataException(e);
                getSender().tell(indexDataException, getSelf());
            }
        }
        else
        {
            handleUnhandledMessage(message);
        }
    }

    private void handleUnhandledMessage(final Object message)
    {
        // No local state the Actor, so can be restarted etc. no issues.
        LOG.error("Unhandled message encountered in DataGeneratorWorkerActor: {}", message);
        unhandled(message);
    }
}
