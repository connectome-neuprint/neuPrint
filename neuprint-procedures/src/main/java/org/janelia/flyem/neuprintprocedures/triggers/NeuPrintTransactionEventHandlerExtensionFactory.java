package org.janelia.flyem.neuprintprocedures.triggers;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NeuPrintTransactionEventHandlerExtensionFactory extends KernelExtensionFactory<NeuPrintTransactionEventHandlerExtensionFactory.Dependencies> {

    @Override
    public Lifecycle newInstance(KernelContext kernelContext, final Dependencies dependencies) {
        return new LifecycleAdapter() {

            private NeuPrintTransactionEventHandler handler;
            private ExecutorService executor;

            @Override
            public void start() {
                executor = Executors.newFixedThreadPool(1);
                handler = new NeuPrintTransactionEventHandler(dependencies.getGraphDatabaseService(), executor);
                dependencies.getGraphDatabaseService().registerTransactionEventHandler(handler);
            }

            @Override
            public void shutdown() {
                executor.shutdown();
                dependencies.getGraphDatabaseService().unregisterTransactionEventHandler(handler);
            }
        };
    }

    interface Dependencies {
        GraphDatabaseService getGraphDatabaseService();
    }

    public NeuPrintTransactionEventHandlerExtensionFactory() {
        super("registerTransactionEventHandler");
    }

}

