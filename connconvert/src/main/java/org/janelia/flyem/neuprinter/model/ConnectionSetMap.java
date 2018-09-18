package org.janelia.flyem.neuprinter.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A class representing a map of all ConnectionSet nodes for a given dataset.
 * Keys are strings in the format "presynapticBodyId:postsynapticBodyId".
 *
 */
public class ConnectionSetMap {

    private Map<String, ConnectionSet> connectionSetMap = new HashMap<>();

    /**
     * Class constructor.
     */
    public ConnectionSetMap() {
    }

    /**
     *
     * @return this ConnectionSetMap
     */
    public Map<String, ConnectionSet> getConnectionSetMap() {
        return this.connectionSetMap;
    }

    /**
     *
     * @return the keys in this ConnectionSetMap
     */
    public Set<String> getConnectionSetKeys() {
        return this.connectionSetMap.keySet();
    }

    /**
     *
     * @param key connection key to query
     * @return ConnectionSet for this key
     */
    public ConnectionSet getConnectionSetForKey(String key) {
        return this.connectionSetMap.get(key);
    }

    /**
     * Add the specified connection to the ConnectionSetMap.
     *
     * @param presynapticBodyId presynaptic bodyId
     * @param postsynapticBodyId postsynaptic bodyId
     * @param presynapseLocationString presynaptic location string ("x:y:z")
     * @param postsynapseLocationString postsynaptic location string ("x:y:z")
     */
    public void addConnection(long presynapticBodyId, long postsynapticBodyId, String presynapseLocationString, String postsynapseLocationString) {
        ConnectionSet currentConnectionSet = this.connectionSetMap.get(presynapticBodyId + ":" + postsynapticBodyId);
        if (currentConnectionSet == null) {

            ConnectionSet connectionSet = new ConnectionSet(presynapticBodyId, postsynapticBodyId);
            connectionSet.addPreAndPostsynapticLocations(presynapseLocationString, postsynapseLocationString);

            this.connectionSetMap.put(presynapticBodyId + ":" + postsynapticBodyId, connectionSet);
        } else {
            currentConnectionSet.addPreAndPostsynapticLocations(presynapseLocationString, postsynapseLocationString);
        }
    }
}
