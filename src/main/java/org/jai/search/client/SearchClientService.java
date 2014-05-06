package org.jai.search.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;

/**
 * Interface for getting client for working with search engine
 *
 */
public interface SearchClientService
{
    /**
     * Get Search engine client
     * 
     * @return
     */
    Client getClient();

    void setup();
    
    void shutdown();
}
