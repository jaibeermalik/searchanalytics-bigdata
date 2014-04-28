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

    void addNewNode(String name);

    void removeNode(String nodeName);
    
    Node getNode(String name);
}
