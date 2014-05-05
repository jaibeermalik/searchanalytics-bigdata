package org.jai.search.actors;

import static akka.actor.SupervisorStrategy.escalate;
import static akka.actor.SupervisorStrategy.restart;
import static akka.actor.SupervisorStrategy.stop;

import org.jai.search.config.IndexDocumentType;
import org.jai.search.data.SampleDataGeneratorService;
import org.jai.search.exception.DocumentGenerationException;
import org.jai.search.exception.DocumentTypeDataGenerationException;
import org.jai.search.exception.DocumentTypeIndexingException;
import org.jai.search.exception.IndexDataException;
import org.jai.search.index.IndexProductDataService;

import org.springframework.util.Assert;

import akka.actor.ActorInitializationException;
import akka.actor.ActorKilledException;
import akka.actor.ActorRef;
import akka.actor.OneForOneStrategy;
import akka.actor.Props;
import akka.actor.SupervisorStrategy;
import akka.actor.SupervisorStrategy.Directive;
import akka.actor.UntypedActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.japi.Function;
import akka.routing.FromConfig;
import scala.concurrent.duration.Duration;

public class SetupDocumentTypeWorkerActor extends UntypedActor
{
    private final LoggingAdapter LOG = Logging.getLogger(getContext().system(), this);

    private ActorRef dataGeneratorWorkerRouter;

    private ActorRef documentGeneratorWorkerRouter;

    private ActorRef indexDocumentWorkerRouter;

    private int totalDocumentsToIndex = 0;

    private int totalDocumentsToIndexDone = 0;

    private IndexDocumentType indexDocumentType;

    private String parentActorPathString = "../../";

    public SetupDocumentTypeWorkerActor(final SampleDataGeneratorService sampleDataGeneratorService,
            final IndexProductDataService indexProductDataService)
    {
        dataGeneratorWorkerRouter = getContext().actorOf(Props.create(DataGeneratorWorkerActor.class, sampleDataGeneratorService),
                "dataGeneratorWorker");
        documentGeneratorWorkerRouter = getContext().actorOf(
                Props.create(DocumentGeneratorWorkerActor.class, sampleDataGeneratorService).withRouter(new FromConfig())
                        .withDispatcher("documentGenerateWorkerActorDispatcher"), "documentGeneratorWorker");
        indexDocumentWorkerRouter = getContext().actorOf(
                Props.create(IndexProductDataWorkerActor.class, indexProductDataService).withRouter(new FromConfig())
                        .withDispatcher("indexDocumentWorkerActorDispatcher"), "indexDocumentWorker");
    }

    private static SupervisorStrategy strategy = new OneForOneStrategy(10, Duration.create("1 minute"),
            new Function<Throwable, Directive>()
            {
                @Override
                public Directive apply(final Throwable t)
                {
                    if (t instanceof DocumentTypeDataGenerationException)
                    {
                        return restart();
                    }
                    else if (t instanceof DocumentGenerationException)
                    {
                        return restart();
                    }
                    else if (t instanceof IndexDataException)
                    {
                        return restart();
                    }
                    else if (t instanceof ActorInitializationException)
                    {
                        return stop();
                    }
                    else if (t instanceof ActorKilledException)
                    {
                        return stop();
                    }
                    else if (t instanceof Exception)
                    {
                        return restart();
                    }
                    else
                    {
                        return escalate();
                    }
                }
            });

    @Override
    public SupervisorStrategy supervisorStrategy()
    {
        LOG.debug("Custom supervisorStrategy is used for SetupDocumentTypeWorkerActor!");
        return strategy;
    }

    @Override
    public void onReceive(final Object message)
    {
        // LOG.debug("Worker Actor message for SetupDocumentTypeWorkerActor is:" + message);
        try
        {
            // message from master actor
            if (message instanceof IndexDocumentTypeMessageVO)
            {
                handleDocumentTypeForDataGeneration(message);
            }
            // store how much data will be generated
            else if (message instanceof Integer)
            {
                handleTotalDocumentToIndex(message);
            }
            // message from data generator, document generator and indexer
            else if (message instanceof IndexDocumentVO)
            {
                generateDocumentAndIndexDocument(message);
            }
            else if (message instanceof Exception)
            {
                handleExceptionInChildActors(message);
            }
            else
            {
                handleUnhandledMessage(message);
            }
        }
        catch (final Exception exception)
        {
            // TODO: check if need to handle it differently
            final String errorMessage = "Error occured while indexing document type: " + message;
            LOG.error(exception, errorMessage);
            final DocumentTypeIndexingException documentTypeIndexingException = new DocumentTypeIndexingException(indexDocumentType,
                    errorMessage, exception);
            sendMessageToParent(documentTypeIndexingException);
        }
    }

    private void handleTotalDocumentToIndex(final Object message)
    {
        totalDocumentsToIndex = (Integer) message;
        if (totalDocumentsToIndex == 0)
        {
            updateStateAndResetIfAllDone();
        }
    }

    private void handleExceptionInChildActors(final Object message)
    {
        final Exception ex = (Exception) message;
        if (ex instanceof DocumentTypeDataGenerationException)
        {
            // issue in generating data itself for a document type. As each worker only handling one document type, tell parent that it is
            // done.
            // TODO: check proper handling
            final DocumentTypeIndexingException documentTypeIndexingException = new DocumentTypeIndexingException(indexDocumentType,
                    "Data generation failed, failing whole document type itself!", ex);
            sendMessageToParent(documentTypeIndexingException);
            resetActorState();
        }
        else if (ex instanceof DocumentGenerationException)
        {
            // TODO: not handling failure separately , change it.
            totalDocumentsToIndexDone++;
            updateStateAndResetIfAllDone();
        }
        else if (ex instanceof IndexDataException)
        {
            // TODO: not handling failure separately , change it.
            totalDocumentsToIndexDone++;
            updateStateAndResetIfAllDone();
        }
        else
        {
            handleUnhandledMessage(message);
        }
    }

    private void generateDocumentAndIndexDocument(final Object message)
    {
        final IndexDocumentVO indexDocumentVO = (IndexDocumentVO) message;
        // Indexing not done, process the data further.
        if (!indexDocumentVO.isIndexDone())
        {
            // Document not generated yet
            if (indexDocumentVO.getProduct() == null && indexDocumentVO.getProductProperty() == null
                    && indexDocumentVO.getProductGroup() == null)
            {
                documentGeneratorWorkerRouter.tell(indexDocumentVO, getSelf());
                // totalDocumentsToIndex++;
                // TODO: implement supervisor strategy for failing stuff.
            }
            // Document generated, index it.
            else
            {
                indexDocumentWorkerRouter.tell(indexDocumentVO, getSelf());
            }
        }
        else
        {
            totalDocumentsToIndexDone++;
            updateStateAndResetIfAllDone();
        }
    }

    private void handleDocumentTypeForDataGeneration(final Object message)
    {
        final IndexDocumentTypeMessageVO indexDocumentTypeMessageVO = (IndexDocumentTypeMessageVO) message;
        // Check input data
        Assert.notNull(indexDocumentTypeMessageVO.getConfig(), "Indexing config can not be null!");
        Assert.notNull(indexDocumentTypeMessageVO.getIndexDocumentType(), "Document type can not be null!");
        // Check existing state.
        Assert.isNull(indexDocumentType, "Actor already processing document type, this should have not happened!");
        Assert.isTrue(totalDocumentsToIndex == 0, "Existing docs to index should have been zero!");
        Assert.isTrue(totalDocumentsToIndexDone == 0, "Existing docs to indexing done should have been zero!");
        // Each actor is supposed to handle single document type.
        indexDocumentType = indexDocumentTypeMessageVO.getIndexDocumentType();
        dataGeneratorWorkerRouter.tell(indexDocumentTypeMessageVO, getSelf());
    }

    private void updateStateAndResetIfAllDone()
    {
        LOG.debug("Total indexing stats for document type are: totalProductsToIndex: {}, totalProductsToIndexDone: {}", new Object[] {
                totalDocumentsToIndex, totalDocumentsToIndexDone });
        if (totalDocumentsToIndex == totalDocumentsToIndexDone)
        {
            LOG.debug("All products indexing done for total document types {} sending message {} to parent!", new Object[] {
                    indexDocumentType, indexDocumentType });
            // Find parent actor in the hierarchy.
            // akka://SearchIndexingSystem/user/setupIndexMasterActor/setupIndexWorkerActor/$a
            // Send the document type done for all the handling types, for now total products done means all types done, change it.
            // sendMessageToParent(IndexingMessage.DOCUMENTTYPE_DONE);
            sendMessageToParent(indexDocumentType);
            resetActorState();
            stopTheActor();
        }
    }

    private void resetActorState()
    {
        totalDocumentsToIndex = 0;
        totalDocumentsToIndexDone = 0;
        indexDocumentType = null;
    }

    private void stopTheActor()
    {
        // Stop the actors
        // TODO: check if message order processing etc. allow this right way.
        // getContext().stop(getSelf());
    }

    private void sendMessageToParent(final Object message)
    {
        // Using actor selection to find parent , as message of indexing completion is received from child actors.
        // To find parent, because of $val syntax and routee, using actor path for the same.
        getContext().actorSelection(parentActorPathString).tell(message, null);
    }

    private void handleUnhandledMessage(final Object message)
    {
        // No local state the Actor, so can be restarted etc. no issues.
        LOG.error("Unhandled message encountered in SetupDocumentTypeWorkerActor: {}", message);
        unhandled(message);
    }

    // /
    // / For testing purpose
    // /
    public int getTotalDocumentsToIndex()
    {
        return totalDocumentsToIndex;
    }

    public int getTotalDocumentsToIndexDone()
    {
        return totalDocumentsToIndexDone;
    }

    public IndexDocumentType getIndexDocumentType()
    {
        return indexDocumentType;
    }

    public void setDataGeneratorWorkerRouter(final ActorRef dataGeneratorWorkerRouter)
    {
        this.dataGeneratorWorkerRouter = dataGeneratorWorkerRouter;
    }

    public void setDocumentGeneratorWorkerRouter(final ActorRef documentGeneratorWorkerRouter)
    {
        this.documentGeneratorWorkerRouter = documentGeneratorWorkerRouter;
    }

    public void setIndexDocumentWorkerRouter(final ActorRef indexDocumentWorkerRouter)
    {
        this.indexDocumentWorkerRouter = indexDocumentWorkerRouter;
    }

    public void setParentActorPathString(final String parentActorPathString)
    {
        this.parentActorPathString = parentActorPathString;
    }
}
