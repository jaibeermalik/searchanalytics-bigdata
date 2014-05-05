package org.jai.search.config;

import org.jai.search.actors.SetupIndexMasterActor;
import org.jai.search.data.SampleDataGeneratorService;
import org.jai.search.index.IndexProductDataService;
import org.jai.search.setup.SetupIndexService;

import com.typesafe.config.ConfigFactory;

import org.springframework.beans.factory.annotation.Autowire;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

/**
 * The application configuration.
 */
@Configuration
class AppConfiguration
{
    @Autowired
    private ApplicationContext applicationContext;

    /**
     * Actor system singleton for this application.
     */
    @Bean(autowire = Autowire.BY_NAME, name = "actorSystem")
    public ActorSystem actorSystem()
    {
        return ActorSystem.create("SearchIndexingSystem", ConfigFactory.load().getConfig("SearchIndexingSystem"));
    }

    @Bean(autowire = Autowire.BY_NAME, name = "setupIndexMasterActor")
    // @Scope("prototype")
    @DependsOn(value = { "actorSystem" })
    public ActorRef setupIndexMasterActor()
    {
        final ActorSystem system = applicationContext.getBean(ActorSystem.class);
        final SetupIndexService setupIndexService = applicationContext.getBean(SetupIndexService.class);
        final SampleDataGeneratorService sampleDataGeneratorService = applicationContext.getBean(SampleDataGeneratorService.class);
        final IndexProductDataService indexProductData = applicationContext.getBean(IndexProductDataService.class);
        return system.actorOf(Props.create(SetupIndexMasterActor.class, setupIndexService, sampleDataGeneratorService, indexProductData)
                .withDispatcher("setupIndexMasterActorDispatch"), "setupIndexMasterActor");
    }
}
