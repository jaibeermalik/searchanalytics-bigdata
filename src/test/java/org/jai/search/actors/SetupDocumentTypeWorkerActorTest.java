package org.jai.search.actors;

import static org.junit.Assert.assertEquals;

import org.jai.search.config.ElasticSearchIndexConfig;
import org.jai.search.config.IndexDocumentType;
import org.jai.search.exception.DocumentGenerationException;
import org.jai.search.exception.DocumentTypeDataGenerationException;
import org.jai.search.exception.DocumentTypeIndexingException;
import org.jai.search.exception.IndexDataException;
import org.jai.search.model.Product;

import com.typesafe.config.ConfigFactory;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UnhandledMessage;
import akka.testkit.TestActor;
import akka.testkit.TestActorRef;
import akka.testkit.TestProbe;
import scala.concurrent.duration.FiniteDuration;

public class SetupDocumentTypeWorkerActorTest
{
    private static ActorSystem system;

    @BeforeClass
    public static void prepareBeforeClass()
    {
        system = ActorSystem.create("TestSearchIndexingSystem", ConfigFactory.load().getConfig("TestSearchIndexingSystem"));
    }

    @Before
    public void prepareTest()
    {
    }

    @Test
    public void handleIndexDocumentTypeMessageVO()
    {
        final Props props = Props.create(SetupDocumentTypeWorkerActor.class, null, null);
        final TestActorRef<SetupDocumentTypeWorkerActor> ref = TestActorRef.create(system, props);
        final SetupDocumentTypeWorkerActor actor = ref.underlyingActor();
        // Mock the behavior of child/worker actors.
        TestProbe testProbeDataGeneratorWorker = TestProbe.apply(system);
        actor.setDataGeneratorWorkerRouter(testProbeDataGeneratorWorker.ref());
        ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
        IndexDocumentType documentType = IndexDocumentType.PRODUCT;
        String indexName = "trialindexName";
        IndexDocumentTypeMessageVO indexDocumentTypeMessageVO = new IndexDocumentTypeMessageVO().config(config).documentType(documentType)
                .newIndexName(indexName);
        TestProbe testProbeOriginalSender = TestProbe.apply(system);
        // Actor state check
        assertEquals(null, actor.getIndexDocumentType());
        ref.tell(indexDocumentTypeMessageVO, testProbeOriginalSender.ref());
        testProbeDataGeneratorWorker.expectMsgClass(IndexDocumentTypeMessageVO.class);
        TestActor.Message message = testProbeDataGeneratorWorker.lastMessage();
        IndexDocumentTypeMessageVO resultMsg = (IndexDocumentTypeMessageVO) message.msg();
        assertEquals(config, resultMsg.getConfig());
        assertEquals(documentType, resultMsg.getIndexDocumentType());
        assertEquals(indexName, resultMsg.getNewIndexName());
        assertEquals(documentType, actor.getIndexDocumentType());
    }

    @Test
    public void handleIndexDocumentVODocumentGeneration()
    {
        final Props props = Props.create(SetupDocumentTypeWorkerActor.class, null, null);
        final TestActorRef<SetupDocumentTypeWorkerActor> ref = TestActorRef.create(system, props);
        final SetupDocumentTypeWorkerActor actor = ref.underlyingActor();
        // Mock the behavior of child/worker actors.
        TestProbe testProbeDocumentGeneratorWorker = TestProbe.apply(system);
        actor.setDocumentGeneratorWorkerRouter(testProbeDocumentGeneratorWorker.ref());
        ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
        IndexDocumentType documentType = IndexDocumentType.PRODUCT;
        String indexName = "trialindexName";
        Long documentId = 1l;
        IndexDocumentVO indexDocumentVO = new IndexDocumentVO().config(config).documentType(documentType).newIndexName(indexName)
                .documentId(documentId);
        // Send data back to the sender, generate document
        assertEquals(0, actor.getTotalDocumentsToIndex());
        ref.tell(indexDocumentVO, null);
        testProbeDocumentGeneratorWorker.expectMsgClass(IndexDocumentVO.class);
        TestActor.Message messageDoc = testProbeDocumentGeneratorWorker.lastMessage();
        IndexDocumentVO resultMsgDoc = (IndexDocumentVO) messageDoc.msg();
        assertEquals(config, resultMsgDoc.getConfig());
        assertEquals(documentType, resultMsgDoc.getDocumentType());
        assertEquals(indexName, resultMsgDoc.getNewIndexName());
        assertEquals(documentId, resultMsgDoc.getDocumentId());
        assertEquals(0, actor.getTotalDocumentsToIndex());
    }
    
    @Test
    public void handleIndexDataSizeToIndex()
    {
        final Props props = Props.create(SetupDocumentTypeWorkerActor.class, null, null);
        final TestActorRef<SetupDocumentTypeWorkerActor> ref = TestActorRef.create(system, props);
        final SetupDocumentTypeWorkerActor actor = ref.underlyingActor();
        // Mock the behavior of child/worker actors.
        TestProbe testProbeDataGeneratorWorker = TestProbe.apply(system);
        actor.setDataGeneratorWorkerRouter(testProbeDataGeneratorWorker.ref());
        //Let's say total data to generate to 1
        ref.tell(Integer.valueOf(1), testProbeDataGeneratorWorker.ref());
        assertEquals(1, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
    }
    
    @Test
    public void handleIndexDocumentVOIndexData()
    {
        final Props props = Props.create(SetupDocumentTypeWorkerActor.class, null, null);
        final TestActorRef<SetupDocumentTypeWorkerActor> ref = TestActorRef.create(system, props);
        final SetupDocumentTypeWorkerActor actor = ref.underlyingActor();
        // Mock the behavior of child/worker actors.
        TestProbe testProbeIndexDataWorker = TestProbe.apply(system);
        actor.setIndexDocumentWorkerRouter(testProbeIndexDataWorker.ref());
        ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
        IndexDocumentType documentType = IndexDocumentType.PRODUCT;
        String indexName = "trialindexName";
        Long documentId = 1l;
        Product product = new Product();
        product.setId(documentId);
        IndexDocumentVO indexDocumentVO = new IndexDocumentVO().config(config).documentType(documentType).newIndexName(indexName)
                .documentId(documentId);
        // send data back, index document.
        indexDocumentVO.product(product);
        // This is controlled for doc generation, won;t change here.
        assertEquals(0, actor.getTotalDocumentsToIndex());
        ref.tell(indexDocumentVO, null);
        testProbeIndexDataWorker.expectMsgClass(IndexDocumentVO.class);
        TestActor.Message messageDocIndex = testProbeIndexDataWorker.lastMessage();
        IndexDocumentVO resultMsgDocIndex = (IndexDocumentVO) messageDocIndex.msg();
        assertEquals(config, resultMsgDocIndex.getConfig());
        assertEquals(documentType, resultMsgDocIndex.getDocumentType());
        assertEquals(indexName, resultMsgDocIndex.getNewIndexName());
        assertEquals(documentId, resultMsgDocIndex.getDocumentId());
        assertEquals(product, resultMsgDocIndex.getProduct());
        assertEquals(0, actor.getTotalDocumentsToIndex());
    }

    @Test
    public void handleIndexDocumentVOIndexDone()
    {
        final Props props = Props.create(SetupDocumentTypeWorkerActor.class, null, null);
        final TestActorRef<SetupDocumentTypeWorkerActor> ref = TestActorRef.create(system, props);
        final SetupDocumentTypeWorkerActor actor = ref.underlyingActor();
        // Mock the behavior of child/worker actors.
        TestProbe testProbeIndexDataWorker = TestProbe.apply(system);
        actor.setIndexDocumentWorkerRouter(testProbeIndexDataWorker.ref());
        ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
        IndexDocumentType documentType = IndexDocumentType.PRODUCT;
        String indexName = "trialindexName";
        Long documentId = 1l;
        Product product = new Product();
        product.setId(documentId);
        IndexDocumentVO indexDocumentVO = new IndexDocumentVO().config(config).documentType(documentType).newIndexName(indexName)
                .documentId(documentId);
        // send data back, index document.
        indexDocumentVO.product(product);
        indexDocumentVO.indexDone(true);
        // Actor state check
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        ref.tell(indexDocumentVO, null);
        testProbeIndexDataWorker.expectNoMsg();
        ;
        assertEquals(1, actor.getTotalDocumentsToIndexDone());
    }

    @Test
    public void handleFullFlow()
    {
        final Props props = Props.create(SetupDocumentTypeWorkerActor.class, null, null);
        final TestActorRef<SetupDocumentTypeWorkerActor> ref = TestActorRef.create(system, props);
        final SetupDocumentTypeWorkerActor actor = ref.underlyingActor();
        // Mock the behavior of child/worker actors.
        TestProbe testProbeDataGeneratorWorker = TestProbe.apply(system);
        TestProbe testProbeDocumentGeneratorWorker = TestProbe.apply(system);
        TestProbe testProbeIndexDataWorker = TestProbe.apply(system);
        actor.setDataGeneratorWorkerRouter(testProbeDataGeneratorWorker.ref());
        actor.setDocumentGeneratorWorkerRouter(testProbeDocumentGeneratorWorker.ref());
        actor.setIndexDocumentWorkerRouter(testProbeIndexDataWorker.ref());
        ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
        IndexDocumentType documentType = IndexDocumentType.PRODUCT;
        String indexName = "trialindexName";
        IndexDocumentTypeMessageVO indexDocumentTypeMessageVO = new IndexDocumentTypeMessageVO().config(config).documentType(documentType)
                .newIndexName(indexName);
        assertEquals(0, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(null, actor.getIndexDocumentType());
        TestProbe testProbeOriginalSender = TestProbe.apply(system);
        ref.tell(indexDocumentTypeMessageVO, testProbeOriginalSender.ref());
        testProbeDataGeneratorWorker.expectMsgClass(IndexDocumentTypeMessageVO.class);
        TestActor.Message message = testProbeDataGeneratorWorker.lastMessage();
        IndexDocumentTypeMessageVO resultMsg = (IndexDocumentTypeMessageVO) message.msg();
        assertEquals(config, resultMsg.getConfig());
        assertEquals(documentType, resultMsg.getIndexDocumentType());
        assertEquals(indexName, resultMsg.getNewIndexName());
        //updated actor state
        assertEquals(0, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(documentType, actor.getIndexDocumentType());
        //Let's say total data to generate to 1
        ref.tell(Integer.valueOf(1), testProbeDataGeneratorWorker.ref());
        assertEquals(1, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(documentType, actor.getIndexDocumentType());
        Long documentId = 1l;
        IndexDocumentVO indexDocumentVO = new IndexDocumentVO().config(resultMsg.getConfig())
                .documentType(indexDocumentTypeMessageVO.getIndexDocumentType()).newIndexName(indexDocumentTypeMessageVO.getNewIndexName())
                .documentId(documentId);
        // Send data back to the sender, generate document
        ref.tell(indexDocumentVO, testProbeDataGeneratorWorker.ref());
        testProbeDocumentGeneratorWorker.expectMsgClass(IndexDocumentVO.class);
        TestActor.Message messageDoc = testProbeDocumentGeneratorWorker.lastMessage();
        IndexDocumentVO resultMsgDoc = (IndexDocumentVO) messageDoc.msg();
        assertEquals(config, resultMsgDoc.getConfig());
        assertEquals(documentType, resultMsgDoc.getDocumentType());
        assertEquals(indexName, resultMsgDoc.getNewIndexName());
        assertEquals(documentId, resultMsgDoc.getDocumentId());
        assertEquals(1, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(documentType, actor.getIndexDocumentType());
        // send data back, index document.
        Product product = new Product();
        product.setId(documentId);
        resultMsgDoc.product(product);
        ref.tell(resultMsgDoc, testProbeDocumentGeneratorWorker.ref());
        testProbeIndexDataWorker.expectMsgClass(IndexDocumentVO.class);
        TestActor.Message messageDocIndex = testProbeIndexDataWorker.lastMessage();
        IndexDocumentVO resultMsgDocIndex = (IndexDocumentVO) messageDocIndex.msg();
        assertEquals(config, resultMsgDocIndex.getConfig());
        assertEquals(documentType, resultMsgDocIndex.getDocumentType());
        assertEquals(indexName, resultMsgDocIndex.getNewIndexName());
        assertEquals(documentId, resultMsgDocIndex.getDocumentId());
        assertEquals(product, resultMsgDocIndex.getProduct());
        assertEquals(1, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(documentType, actor.getIndexDocumentType());
        // Send back message, indexdone.
        resultMsgDocIndex.indexDone(true);
        TestProbe testProbeParent = TestProbe.apply(system);
        String string = testProbeParent.ref().path().toString();
        actor.setParentActorPathString(string);
        ref.tell(resultMsgDocIndex, null);
        // state is set back to initial and message sent to parent.
        testProbeParent.expectMsgClass(IndexDocumentType.class);
        TestActor.Message messageDocIndexDone = testProbeParent.lastMessage();
        IndexDocumentType resultMsgDocIndexDone = (IndexDocumentType) messageDocIndexDone.msg();
        //state reset 
        assertEquals(documentType, resultMsgDocIndexDone);
        assertEquals(0, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(null, actor.getIndexDocumentType());
    }

    @Test
    public void handleUnhandledMessage()
    {
        final Props props = Props.create(SetupDocumentTypeWorkerActor.class, null, null);
        final TestActorRef<SetupDocumentTypeWorkerActor> ref = TestActorRef.create(system, props);
        final SetupDocumentTypeWorkerActor actor = ref.underlyingActor();
        // Mock the behavior of child/worker actors.
        TestProbe testProbeIndexDataWorker = TestProbe.apply(system);
        actor.setIndexDocumentWorkerRouter(testProbeIndexDataWorker.ref());
        // Actor state check
        assertEquals(null, actor.getIndexDocumentType());
        // Subscribe first
        TestProbe subscriber = TestProbe.apply(system);
        system.eventStream().subscribe(subscriber.ref(), UnhandledMessage.class);
        // garbage
        String invalidMessage = "blah blah";
        ref.tell(invalidMessage, null);
        // Expect the unhandled message now.
        FiniteDuration duration = FiniteDuration.create(1, TimeUnit.SECONDS);
        subscriber.expectMsgClass(duration, UnhandledMessage.class);
        TestActor.Message message = subscriber.lastMessage();
        UnhandledMessage resultMsg = (UnhandledMessage) message.msg();
        assertEquals(invalidMessage, (String) resultMsg.getMessage());
        // Actor state check
        assertEquals(null, actor.getIndexDocumentType());
    }

    @Test
    public void handleChildActorsException()
    {
        final Props props = Props.create(SetupDocumentTypeWorkerActor.class, null, null);
        final TestActorRef<SetupDocumentTypeWorkerActor> ref = TestActorRef.create(system, props);
        final SetupDocumentTypeWorkerActor actor = ref.underlyingActor();
        // Mock the behavior of child/worker actors.
        TestProbe testProbeDataGeneratorWorker = TestProbe.apply(system);
        TestProbe testProbeDocumentGeneratorWorker = TestProbe.apply(system);
        TestProbe testProbeIndexDataWorker = TestProbe.apply(system);
        actor.setDataGeneratorWorkerRouter(testProbeDataGeneratorWorker.ref());
        actor.setDocumentGeneratorWorkerRouter(testProbeDocumentGeneratorWorker.ref());
        actor.setIndexDocumentWorkerRouter(testProbeIndexDataWorker.ref());
        ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
        IndexDocumentType documentType = IndexDocumentType.PRODUCT;
        String indexName = "trialindexName";
        IndexDocumentTypeMessageVO indexDocumentTypeMessageVO = new IndexDocumentTypeMessageVO().config(config).newIndexName(indexName).documentType(documentType);
        //initial actor state
        assertEquals(0, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(null, actor.getIndexDocumentType());
        //send initial message
        ref.tell(indexDocumentTypeMessageVO, null);
        testProbeDataGeneratorWorker.expectMsgClass(IndexDocumentTypeMessageVO.class);
        TestActor.Message message = testProbeDataGeneratorWorker.lastMessage();
        IndexDocumentTypeMessageVO resultMsg = (IndexDocumentTypeMessageVO) message.msg();
        assertEquals(config, resultMsg.getConfig());
        assertEquals(documentType, resultMsg.getIndexDocumentType());
        assertEquals(indexName, resultMsg.getNewIndexName());
        
        //Updated actor state
        assertEquals(0, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(documentType, actor.getIndexDocumentType());
        
        //send Data generate error from child
        DocumentTypeDataGenerationException documentTypeDataGenerationException = new DocumentTypeDataGenerationException(documentType, "Testing", new RuntimeException());
        // parent Exception, tell parent that exception occurred.
        TestProbe testProbeParentActor = TestProbe.apply(system);
        String parentString = testProbeParentActor.ref().path().toString();
        actor.setParentActorPathString(parentString);

        ref.tell(documentTypeDataGenerationException, testProbeDataGeneratorWorker.ref());
        
        //Updated actor state, state reset
        assertEquals(0, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(null, actor.getIndexDocumentType());
        
        testProbeParentActor.expectMsgClass(DocumentTypeIndexingException.class);
        TestActor.Message testProbeParentActorMessage = testProbeParentActor.lastMessage();
        DocumentTypeIndexingException testProbeParentActorMessageType = (DocumentTypeIndexingException) testProbeParentActorMessage.msg();
        assertEquals(documentType, testProbeParentActorMessageType.getIndexDocumentType());
        //Let's say total data to generate to 1
        ref.tell(indexDocumentTypeMessageVO, null);
        ref.tell(Integer.valueOf(1), testProbeDataGeneratorWorker.ref());
        assertEquals(1, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(documentType, actor.getIndexDocumentType());
        
        Long documentId = 1l;
        IndexDocumentVO indexDocumentVO = new IndexDocumentVO().config(resultMsg.getConfig())
                .documentType(indexDocumentTypeMessageVO.getIndexDocumentType()).newIndexName(indexDocumentTypeMessageVO.getNewIndexName())
                .documentId(documentId);
        // Send data back to the sender, generate document
        ref.tell(indexDocumentVO, testProbeDataGeneratorWorker.ref());
        testProbeDocumentGeneratorWorker.expectMsgClass(IndexDocumentVO.class);
        TestActor.Message messageDoc = testProbeDocumentGeneratorWorker.lastMessage();
        IndexDocumentVO resultMsgDoc = (IndexDocumentVO) messageDoc.msg();
        assertEquals(config, resultMsgDoc.getConfig());
        assertEquals(documentType, resultMsgDoc.getDocumentType());
        assertEquals(indexName, resultMsgDoc.getNewIndexName());
        assertEquals(documentId, resultMsgDoc.getDocumentId());
        //updated actor state
        assertEquals(1, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(documentType, actor.getIndexDocumentType());

        DocumentGenerationException documentGenerationException = new DocumentGenerationException("Error generation document!");
        ref.tell(documentGenerationException, testProbeDocumentGeneratorWorker.ref());
        
        //Updated actor state, decreases doc count by 1. and state reset as all is done.
        assertEquals(0, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(null, actor.getIndexDocumentType());
        //tell to parent that it is done.
        testProbeParentActor.expectMsgClass(IndexDocumentType.class);
        testProbeParentActorMessage = testProbeParentActor.lastMessage();
        IndexDocumentType testProbeParentActorMessageType2 = (IndexDocumentType) testProbeParentActorMessage.msg();
        assertEquals(documentType, testProbeParentActorMessageType2);
        
        // send data back, index document.
        //last exception occurred, send it again.
        //Let's say total data to generate to 1
        ref.tell(indexDocumentTypeMessageVO, null);
        ref.tell(Integer.valueOf(1), testProbeDataGeneratorWorker.ref());
        Product product = new Product();
        product.setId(documentId);
        resultMsgDoc.product(product);
        ref.tell(resultMsgDoc, testProbeDocumentGeneratorWorker.ref());
        testProbeIndexDataWorker.expectMsgClass(IndexDocumentVO.class);
        TestActor.Message messageDocIndex = testProbeIndexDataWorker.lastMessage();
        IndexDocumentVO resultMsgDocIndex = (IndexDocumentVO) messageDocIndex.msg();
        assertEquals(config, resultMsgDocIndex.getConfig());
        assertEquals(documentType, resultMsgDocIndex.getDocumentType());
        assertEquals(indexName, resultMsgDocIndex.getNewIndexName());
        assertEquals(documentId, resultMsgDocIndex.getDocumentId());
        assertEquals(product, resultMsgDocIndex.getProduct());
        assertEquals(1, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(documentType, actor.getIndexDocumentType());
        
        IndexDataException indexDataException = new IndexDataException("Error while indexing document!");
        ref.tell(indexDataException, testProbeIndexDataWorker.ref());
        //Updated actor state, decreases doc count by 1.
        //The state is reset back to initial and informed parent.
        assertEquals(0, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(null, actor.getIndexDocumentType());
        //tell to parent that it is done.
        testProbeParentActor.expectMsgClass(IndexDocumentType.class);
        testProbeParentActorMessage = testProbeParentActor.lastMessage();
        IndexDocumentType testProbeParentActorMessageType3 = (IndexDocumentType) testProbeParentActorMessage.msg();
        assertEquals(documentType, testProbeParentActorMessageType3);
       //Updated actor state, increase index doc count by 1.
        assertEquals(0, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(null, actor.getIndexDocumentType());  
        // Send back message, indexdone.
        //last exception occred, send msg again
        //Let's say total data to generate to 1
        ref.tell(indexDocumentTypeMessageVO, null);
        ref.tell(Integer.valueOf(1), testProbeDataGeneratorWorker.ref());
        resultMsgDocIndex.indexDone(true);
        ref.tell(resultMsgDocIndex, null);
        // state is set back to initial and message sent to parent.
        testProbeParentActor.expectMsgClass(IndexDocumentType.class);
        testProbeParentActorMessage = testProbeParentActor.lastMessage();
        IndexDocumentType testProbeParentActorMessageType4 = (IndexDocumentType) testProbeParentActorMessage.msg();
        assertEquals(documentType, testProbeParentActorMessageType4);
        assertEquals(0, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(null, actor.getIndexDocumentType());
    }

    @Test
    public void sendExceptionToParent()
    {
        final Props props = Props.create(SetupDocumentTypeWorkerActor.class, null, null);
        final TestActorRef<SetupDocumentTypeWorkerActor> ref = TestActorRef.create(system, props);
        final SetupDocumentTypeWorkerActor actor = ref.underlyingActor();
        // Mock the behavior of child/worker actors.
        TestProbe testProbeDocumentGeneratorWorker = TestProbe.apply(system);
        TestProbe testProbeIndexDataWorker = TestProbe.apply(system);
        //No data generator
        actor.setDataGeneratorWorkerRouter(null);
        actor.setDocumentGeneratorWorkerRouter(testProbeDocumentGeneratorWorker.ref());
        actor.setIndexDocumentWorkerRouter(testProbeIndexDataWorker.ref());
        ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
        IndexDocumentType documentType = IndexDocumentType.PRODUCT;
        String indexName = "trialindexName";
        // no document type
        IndexDocumentTypeMessageVO indexDocumentTypeMessageVO = new IndexDocumentTypeMessageVO().config(config).newIndexName(indexName).documentType(documentType);
        assertEquals(0, actor.getTotalDocumentsToIndex());
        assertEquals(0, actor.getTotalDocumentsToIndexDone());
        assertEquals(null, actor.getIndexDocumentType());
        // parent Exception, NPE in data generator
        TestProbe testProbeParentActor = TestProbe.apply(system);
        String parentString = testProbeParentActor.ref().path().toString();
        actor.setParentActorPathString(parentString);
        ref.tell(indexDocumentTypeMessageVO, null);
        testProbeParentActor.expectMsgClass(DocumentTypeIndexingException.class);
        TestActor.Message testProbeParentActorMessage = testProbeParentActor.lastMessage();
        DocumentTypeIndexingException testProbeParentActorMessageType = (DocumentTypeIndexingException) testProbeParentActorMessage.msg();
        // doc type is not yet set
        assertEquals(documentType, testProbeParentActorMessageType.getIndexDocumentType());
    }
}
