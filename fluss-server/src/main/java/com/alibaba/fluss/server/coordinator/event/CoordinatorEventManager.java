/*
 * Copyright (c) 2024 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.fluss.server.coordinator.event;

import com.alibaba.fluss.annotation.Internal;
import com.alibaba.fluss.utils.concurrent.ShutdownableThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.fluss.utils.concurrent.LockUtils.inLock;

/**
 * A manager for the events happens in Coordinator Server. It will poll the event from a queue and
 * then process it.
 */
@Internal
public final class CoordinatorEventManager implements EventManager {

    private static final Logger LOG = LoggerFactory.getLogger(CoordinatorEventManager.class);

    private static final String COORDINATOR_EVENT_THREAD_NAME = "coordinator-event-thread";

    private final EventProcessor eventProcessor;

    private final LinkedBlockingQueue<CoordinatorEvent> queue = new LinkedBlockingQueue<>();
    private final CoordinatorEventThread thread =
            new CoordinatorEventThread(COORDINATOR_EVENT_THREAD_NAME);
    private final Lock putLock = new ReentrantLock();

    public CoordinatorEventManager(EventProcessor eventProcessor) {
        this.eventProcessor = eventProcessor;
    }

    public void start() {
        thread.start();
    }

    public void close() {
        try {
            thread.initiateShutdown();
            clearAndPut(new ShutdownEventThreadEvent());
            thread.awaitShutdown();
        } catch (InterruptedException e) {
            LOG.error("Fail to close coordinator event thread.");
        }
    }

    public void put(CoordinatorEvent event) {
        inLock(
                putLock,
                () -> {
                    try {
                        queue.put(event);
                    } catch (InterruptedException e) {
                        LOG.error("Fail to put coordinator event {}.", event, e);
                    }
                });
    }

    public void clearAndPut(CoordinatorEvent event) {
        inLock(
                putLock,
                () -> {
                    queue.clear();
                    put(event);
                });
    }

    private class CoordinatorEventThread extends ShutdownableThread {

        public CoordinatorEventThread(String name) {
            super(name, false);
        }

        @Override
        public void doWork() throws Exception {
            CoordinatorEvent event = queue.take();
            try {
                if (!(event instanceof ShutdownEventThreadEvent)) {
                    eventProcessor.process(event);
                }
            } catch (Throwable e) {
                LOG.error("Uncaught error processing event {}.", event, e);
            }
        }
    }
}
