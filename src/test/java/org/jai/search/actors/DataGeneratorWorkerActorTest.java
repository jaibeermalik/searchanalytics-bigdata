package org.jai.search.actors;

import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;

import org.jai.search.config.ElasticSearchIndexConfig;
import org.jai.search.config.IndexDocumentType;
import org.jai.search.data.SampleDataGeneratorService;
import org.jai.search.exception.DocumentTypeDataGenerationException;
import org.jai.search.model.Product;
import org.jai.search.model.ProductGroup;
import org.jai.search.model.ProductProperty;

import com.typesafe.config.ConfigFactory;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.UnhandledMessage;
import akka.testkit.TestActor;
import akka.testkit.TestActorRef;
import akka.testkit.TestProbe;
import scala.concurrent.duration.FiniteDuration;

public class DataGeneratorWorkerActorTest
{
    private static ActorSystem system;

    private SampleDataGeneratorService sampleDataGeneratorService;

    @BeforeClass
    public static void prepareBeforeClass()
    {
        system = ActorSystem.create("TestSearchIndexingSystem", ConfigFactory.load().getConfig("TestSearchIndexingSystem"));
    }

    @Before
    public void prepareTest()
    {
        sampleDataGeneratorService = createNiceMock(SampleDataGeneratorService.class);
    }

    @Test
    public void testProductDataGeneration()
    {
        final Props props = Props.create(DataGeneratorWorkerActor.class, sampleDataGeneratorService);
        final TestActorRef<DataGeneratorWorkerActor> ref = TestActorRef.create(system, props);
        
        ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
        IndexDocumentType documentType = IndexDocumentType.PRODUCT;
        String indexName = "trialindexName";
        IndexDocumentTypeMessageVO indexDocumentTypeMessageVO = new IndexDocumentTypeMessageVO()
                .config(config).documentType(documentType).newIndexName(indexName);

        List<Product> productsList = new ArrayList<Product>();
        Product product = new Product();
        //For now ids hard coded in generator for testing
        Long productId = 1l;
        product.setId(productId);
        productsList.add(product);
        
        expect(sampleDataGeneratorService.generateProductsSampleData()).andReturn(productsList);
        replay(sampleDataGeneratorService);
        
        TestProbe testProbe = TestProbe.apply(system);
        ref.tell(indexDocumentTypeMessageVO, testProbe.ref());
        verify(sampleDataGeneratorService);
        
        testProbe.expectMsgClass(Integer.class);     
        TestActor.Message sizeMessage = testProbe.lastMessage();
        Integer resultMsgCount = (Integer) sizeMessage.msg();
        assertEquals(1, resultMsgCount.intValue());
        
        testProbe.expectMsgClass(IndexDocumentVO.class);     
        TestActor.Message message = testProbe.lastMessage();
        IndexDocumentVO resultMsg = (IndexDocumentVO) message.msg();
        assertEquals(productId, resultMsg.getDocumentId());
        assertEquals(config, resultMsg.getConfig());
        assertEquals(documentType, resultMsg.getDocumentType());
        assertEquals(indexName, resultMsg.getNewIndexName());
    }
    
    @Test
    public void testProductPropertyDataGeneration()
    {
        final Props props = Props.create(DataGeneratorWorkerActor.class, sampleDataGeneratorService);
        final TestActorRef<DataGeneratorWorkerActor> ref = TestActorRef.create(system, props);
        
        ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
        IndexDocumentType documentType = IndexDocumentType.PRODUCT_PROPERTY;
        String indexName = "trialindexName";
        IndexDocumentTypeMessageVO indexDocumentTypeMessageVO = new IndexDocumentTypeMessageVO()
                .config(config).documentType(documentType).newIndexName(indexName);

        Set<ProductProperty> productPropertList = new HashSet<ProductProperty>();
        ProductProperty productProperty = new ProductProperty();
        //For now ids hard coded in generator for testing
        Long productPropertyId = 1l;
        productProperty.setId(productPropertyId);
        productProperty.setColor(SampleDataGeneratorService.PRODUCTPROPERTY_COLOR_BLACK);
        productProperty.setSize(SampleDataGeneratorService.PRODUCTPROPERTY_SIZE_13_INCH);
        productPropertList.add(productProperty);
        
        expect(sampleDataGeneratorService.generateProductPropertySampleData()).andReturn(productPropertList);
        replay(sampleDataGeneratorService);
        
        TestProbe testProbe = TestProbe.apply(system);
        ref.tell(indexDocumentTypeMessageVO, testProbe.ref());
        verify(sampleDataGeneratorService);
        
        testProbe.expectMsgClass(Integer.class);     
        TestActor.Message sizeMessage = testProbe.lastMessage();
        Integer resultMsgCount = (Integer) sizeMessage.msg();
        assertEquals(1, resultMsgCount.intValue());
        
        testProbe.expectMsgClass(IndexDocumentVO.class);     
        TestActor.Message message = testProbe.lastMessage();
        IndexDocumentVO resultMsg = (IndexDocumentVO) message.msg();
        assertEquals(productPropertyId, resultMsg.getDocumentId());
        assertEquals(config, resultMsg.getConfig());
        assertEquals(documentType, resultMsg.getDocumentType());
        assertEquals(indexName, resultMsg.getNewIndexName());
    }
    
    @Test
    public void testProductGroupDataGeneration()
    {
        final Props props = Props.create(DataGeneratorWorkerActor.class, sampleDataGeneratorService);
        final TestActorRef<DataGeneratorWorkerActor> ref = TestActorRef.create(system, props);
        
        ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
        IndexDocumentType documentType = IndexDocumentType.PRODUCT_GROUP;
        String indexName = "trialindexName";
        IndexDocumentTypeMessageVO indexDocumentTypeMessageVO = new IndexDocumentTypeMessageVO()
                .config(config).documentType(documentType).newIndexName(indexName);

        List<ProductGroup> productGroupList = new ArrayList<ProductGroup>();
        ProductGroup productGroup = new ProductGroup();
        //For now ids hard coded in generator for testing
        Long productGroupId = 1l;
        productGroup.setId(productGroupId);
        productGroupList.add(productGroup);
        
        expect(sampleDataGeneratorService.generateProductGroupSampleData()).andReturn(productGroupList);
        replay(sampleDataGeneratorService);
        
        TestProbe testProbe = TestProbe.apply(system);
        ref.tell(indexDocumentTypeMessageVO, testProbe.ref());
        verify(sampleDataGeneratorService);
        
        testProbe.expectMsgClass(Integer.class);     
        TestActor.Message sizeMessage = testProbe.lastMessage();
        Integer resultMsgCount = (Integer) sizeMessage.msg();
        assertEquals(1, resultMsgCount.intValue());
        
        testProbe.expectMsgClass(IndexDocumentVO.class);     
        TestActor.Message message = testProbe.lastMessage();
        IndexDocumentVO resultMsg = (IndexDocumentVO) message.msg();
        assertEquals(productGroupId, resultMsg.getDocumentId());
        assertEquals(config, resultMsg.getConfig());
        assertEquals(documentType, resultMsg.getDocumentType());
        assertEquals(indexName, resultMsg.getNewIndexName());
    }
    
    @Test
    public void testExceptionForInvalidDocumentTypeMessage()
    {
        final Props props = Props.create(DataGeneratorWorkerActor.class, sampleDataGeneratorService);
        final TestActorRef<DataGeneratorWorkerActor> ref = TestActorRef.create(system, props);
        
        ElasticSearchIndexConfig config = ElasticSearchIndexConfig.COM_WEBSITE;
        
        //invalid document type
        IndexDocumentType documentType = null;
        String indexName = "trialindexName";
        IndexDocumentTypeMessageVO indexDocumentTypeMessageVO = new IndexDocumentTypeMessageVO()
                .config(config).documentType(documentType).newIndexName(indexName);

        replay(sampleDataGeneratorService);
        
        TestProbe testProbe = TestProbe.apply(system);
        ref.tell(indexDocumentTypeMessageVO, testProbe.ref());
        verify(sampleDataGeneratorService);
        
        testProbe.expectMsgClass(DocumentTypeDataGenerationException.class);     
        TestActor.Message message = testProbe.lastMessage();
        DocumentTypeDataGenerationException resultMsg = (DocumentTypeDataGenerationException) message.msg();
        assertEquals(documentType, resultMsg.getIndexDocumentType());
    }
    
    @Test
    public void testExceptionForInvalidMessage()
    {
        final Props props = Props.create(DataGeneratorWorkerActor.class, sampleDataGeneratorService);
        final TestActorRef<DataGeneratorWorkerActor> ref = TestActorRef.create(system, props);
        
        //Subscribe first
        TestProbe subscriber = TestProbe.apply(system);  
        system.eventStream().subscribe(subscriber.ref(), UnhandledMessage.class);
        
        replay(sampleDataGeneratorService);
        
        TestProbe testProbe = TestProbe.apply(system);
        //Send message
        String invalidMessage = "blah blah";
        ref.tell(invalidMessage, testProbe.ref());
        verify(sampleDataGeneratorService);
        
        //Expect the unhandled message now.  
        FiniteDuration duration = FiniteDuration.create(1, TimeUnit.SECONDS);
        subscriber.expectMsgClass(duration, UnhandledMessage.class);
        TestActor.Message message = subscriber.lastMessage();
        UnhandledMessage resultMsg = (UnhandledMessage) message.msg();
        assertEquals(invalidMessage, (String)resultMsg.getMessage());
    }
}
