package org.jai.search.actors;

import org.jai.search.data.SampleDataGeneratorService;
import org.jai.search.exception.DocumentTypeDataGenerationException;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import scala.Option;

public class DataGeneratorWorkerActor extends UntypedActor
{
    final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    private final SampleDataGeneratorService sampleDataGeneratorService;

    public DataGeneratorWorkerActor(final SampleDataGeneratorService sampleDataGeneratorService)
    {
        this.sampleDataGeneratorService = sampleDataGeneratorService;
    }

    @Override
    public void onReceive(final Object message)
    {
        // LOG.debug("Worker Actor message for DataGeneratorWorkerActor is:" + message);
        // message from master actor
        if (message instanceof IndexDocumentTypeMessageVO)
        {
            final IndexDocumentTypeMessageVO indexDocumentTypeMessageVO = (IndexDocumentTypeMessageVO) message;
            try
            {
                switch (indexDocumentTypeMessageVO.getIndexDocumentType())
                {
                    case PRODUCT:
                        generateData(indexDocumentTypeMessageVO, sampleDataGeneratorService.generateProductsSampleData().size());
                        break;
                    case PRODUCT_PROPERTY:
                        generateData(indexDocumentTypeMessageVO, sampleDataGeneratorService.generateProductPropertySampleData().size());
                        break;
                    case PRODUCT_GROUP:
                        generateData(indexDocumentTypeMessageVO, sampleDataGeneratorService.generateProductGroupSampleData().size());
                        break;
                    default:
                        handleUnhandledMessage(message);
                }
            }
            catch (final Exception ex)
            {
                final String errorMessage = "Error occurred while generating data for message" + message;
                LOG.error(ex, errorMessage);
                final DocumentTypeDataGenerationException dataGenerationException = new DocumentTypeDataGenerationException(
                        indexDocumentTypeMessageVO.getIndexDocumentType(), errorMessage, ex);
                getSender().tell(dataGenerationException, getSelf());
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

    private void generateData(final IndexDocumentTypeMessageVO indexDocumentTypeMessageVO, final int size)
    {
        // Before starting data generation, send how much data will be generated, which is size.
        getSender().tell(Integer.valueOf(size), getSelf());
        // LOG.debug("Generating data for IndexDocumentTypeMessageVO: {}, for size: {}", new Object[]{indexDocumentTypeMessageVO, size});
        for (int i = 1; i <= size; i++)
        {
            final IndexDocumentVO indexDocumentVO = new IndexDocumentVO().config(indexDocumentTypeMessageVO.getConfig())
                    .documentType(indexDocumentTypeMessageVO.getIndexDocumentType())
                    .newIndexName(indexDocumentTypeMessageVO.getNewIndexName()).documentId(Long.valueOf(i));
            getSender().tell(indexDocumentVO, getSelf());
        }
    }

    @Override
    public void preRestart(final Throwable reason, final Option<Object> message) throws Exception
    {
        // TODO Auto-generated method stub
        super.preRestart(reason, message);
        LOG.debug("DataGeneratorWorkerActor restarted because of reason: {}", reason);
    }
}
