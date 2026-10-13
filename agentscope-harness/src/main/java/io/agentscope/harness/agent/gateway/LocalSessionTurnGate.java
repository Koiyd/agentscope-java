/*
 * Copyright 2024-2026 the original author or authors.
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
package io.agentscope.harness.agent.gateway;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

/**
 * Process-local fair per-key mutual exclusion for gateway turns.
 *
 * <p>Uses a fair {@link Semaphore} per key. {@link #acquire(String)} blocks until the slot is
 * available; it never throws {@link TurnBusyException}.
 */
public final class LocalSessionTurnGate implements SessionTurnGate {

    private final ConcurrentHashMap<String, Semaphore> gates = new ConcurrentHashMap<>();

    @Override
    public TurnLease acquire(String key) throws InterruptedException {
        Semaphore semaphore = gates.computeIfAbsent(key, k -> new Semaphore(1, true));
        semaphore.acquire();
        return () -> {
            semaphore.release();
            // No thread is waiting for this key: the entry is idle and can be evicted.
            // ConcurrentHashMap.remove(key, value) is a conditional remove — it only deletes
            // the entry when its current value is still this semaphore instance, so a
            // concurrent computeIfAbsent that already inserted a fresh instance is unaffected.
            if (semaphore.getQueueLength() == 0) {
                gates.remove(key, semaphore);
            }
        };
    }

    @Override
    public boolean isRunning(String key) {
        Semaphore semaphore = gates.get(key);
        return semaphore != null && semaphore.availablePermits() == 0;
    }
}
