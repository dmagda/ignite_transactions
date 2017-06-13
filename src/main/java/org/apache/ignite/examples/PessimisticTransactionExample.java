/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples;

import java.io.Serializable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Demonstrates how to use pessimistic transactions in Ignite.
 */
public class PessimisticTransactionExample {
    /** Cache name. */
    private static final String CACHE_NAME = PessimisticTransactionExample.class.getSimpleName();

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache transaction example started.");

            CacheConfiguration<Integer, Account> cfg = new CacheConfiguration<>(CACHE_NAME);

            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, Account> cache = ignite.getOrCreateCache(cfg)) {
                // Initialize.
                cache.put(1, new Account(1, 100));
                cache.put(2, new Account(1, 200));

                System.out.println();
                System.out.println(">>> Accounts before deposit: ");
                System.out.println(">>> " + cache.get(1));
                System.out.println(">>> " + cache.get(2));

                // Make transactional deposits.
                deposit(cache, 1, 100);
                deposit(cache, 2, 200);

                System.out.println();
                System.out.println(">>> Accounts after transfer: ");
                System.out.println(">>> " + cache.get(1));
                System.out.println(">>> " + cache.get(2));

                System.out.println(">>> Cache transaction example finished.");
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }

    /**
     * Make deposit into specified account.
     *
     * @param acctId Account ID.
     * @param amount Amount to deposit.
     * @throws IgniteException If failed.
     */
    private static void deposit(IgniteCache<Integer, Account> cache, int acctId, double amount) throws IgniteException {
        try (Transaction tx = Ignition.ignite().transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Account acct = cache.get(acctId);

            assert acct != null;

            // Deposit into account.
            acct.update(amount);

            // Store updated account in cache.
            cache.put(acctId, acct);

            tx.commit();
        }

        System.out.println();
        System.out.println(">>> Transferred amount: $" + amount);
    }
}
