package org.jai.search.actors;

import org.jai.search.data.SampleDataGeneratorService;
import org.jai.search.exception.DocumentGenerationException;
import org.jai.search.model.Product;
import org.jai.search.model.ProductGroup;
import org.jai.search.model.ProductProperty;

import org.springframework.util.Assert;

import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class DocumentGeneratorWorkerActor extends UntypedActor
{
    final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    private final SampleDataGeneratorService sampleDataGenerator;

    public DocumentGeneratorWorkerActor(final SampleDataGeneratorService sampleDataGenerator)
    {
        this.sampleDataGenerator = sampleDataGenerator;
    }

    @Override
    public void onReceive(final Object message)
    {
        // LOG.debug("Worker Actor message for DocumentGeneratorWorkerActor is:" + message);
        if (message instanceof IndexDocumentVO)
        {
            try
            {
                final IndexDocumentVO indexDocumentVO = (IndexDocumentVO) message;
                switch (indexDocumentVO.getDocumentType())
                {
                    case PRODUCT:
                        final Product product = sampleDataGenerator.generateProductSampleDataFor(indexDocumentVO.getDocumentId());
                        Assert.notNull(product);
                        indexDocumentVO.product(product);
                        getSender().tell(indexDocumentVO, null);
                        break;
                    case PRODUCT_PROPERTY:
                        final ProductProperty productProperty = sampleDataGenerator.generateProductPropertySampleDataFor(indexDocumentVO
                                .getDocumentId());
                        Assert.notNull(productProperty);
                        indexDocumentVO.productProperty(productProperty);
                        getSender().tell(indexDocumentVO, null);
                        break;
                    case PRODUCT_GROUP:
                        final ProductGroup productGroup = sampleDataGenerator.generateProductGroupSampleDataFor(indexDocumentVO
                                .getDocumentId());
                        Assert.notNull(productGroup);
                        indexDocumentVO.productGroup(productGroup);
                        getSender().tell(indexDocumentVO, null);
                        break;
                    default:
                        handleUnhandledMessage(message);
                }
            }
            catch (final Exception e)
            {
                LOG.error(e, "Error occurred while generating document for message: {}", message);
                final DocumentGenerationException documentGenerationException = new DocumentGenerationException(e);
                getSender().tell(documentGenerationException, getSelf());
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

    @Override
    public void postRestart(final Throwable reason) throws Exception
    {
        super.postRestart(reason);
        LOG.info("Restarted because of: {}", reason.getMessage());
    }
}
