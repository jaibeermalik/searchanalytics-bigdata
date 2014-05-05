package org.jai.search.actors;

import org.jai.search.data.SampleDataGeneratorService;
import org.jai.search.setup.SetupIndexService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;

import akka.actor.ActorRef;
import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

@Service
public class BootStrapIndexingServiceImpl implements BootStrapIndexService
{
    private static final Logger logger = LoggerFactory.getLogger(BootStrapIndexingServiceImpl.class);

    @Autowired
    private SampleDataGeneratorService sampleDataGenerator;

    @Autowired
    private SetupIndexService setupIndexService;

    @Autowired
    private ActorRef setupIndexMasterActor;

    @Override
    public void preparingIndexes()
    {
        logger.info("Starting index preparation for {}", IndexingMessage.REBUILD_ALL_INDICES);
        setupIndexMasterActor.tell(IndexingMessage.REBUILD_ALL_INDICES, null);
        final Timeout timeout = new Timeout(Duration.create(10, "seconds"));
        final StopWatch stopWatch = new StopWatch();
        Future<Object> future = Patterns.ask(setupIndexMasterActor, IndexingMessage.REBUILD_ALL_INDICES_DONE, timeout);
        try
        {
            // Allow only to run for max 5 min. and check every few sec usign thread sleep.
            stopWatch.start();
            while (!(Boolean) Await.result(future, timeout.duration()) && stopWatch.getTotalTimeSeconds() < 5 * 60)
            {
                future = Patterns.ask(setupIndexMasterActor, IndexingMessage.REBUILD_ALL_INDICES_DONE, timeout);
                logger.debug("Index setup status check, Got back " + false);
                // TODO: use this, based on your time taken by ur env. here, 100 ms
                Thread.sleep(100);
            }
            logger.debug("Index setup status check, Got back " + true);
        }
        catch (final Exception e)
        {
            logger.debug("Index setup status check, Failed getting result: " + e.getMessage());
        }
        logger.debug("All indexing setup finished using Akka system, Enjoy!");
    }
}
