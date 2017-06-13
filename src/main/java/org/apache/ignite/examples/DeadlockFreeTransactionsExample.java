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

import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionOptimisticException;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Demonstrates how to use the deadlock-free transactions in Ignite that can even help to avoid deadlocks like
 * it's shown in this example.
 */
public class DeadlockFreeTransactionsExample {
    /** Cache name. */
    private static final String CACHE_NAME = DeadlockFreeTransactionsExample.class.getSimpleName();

    /** Total number of entries to use in the example. */
    private static int ENTRIES_COUNT = 10;

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException, InterruptedException {
        try (Ignite ignite = Ignition.start("config/example-ignite.xml")) {
            System.out.println();
            System.out.println(">>> Cache transaction example started.");

            CacheConfiguration<Integer, Account> cfg = new CacheConfiguration<>(CACHE_NAME);

            cfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            // Auto-close cache at the end of the example.
            try (IgniteCache<Integer, Account> cache = ignite.getOrCreateCache(cfg)) {
                // Initializing the cache.
                for (int i = 1; i <= ENTRIES_COUNT; i++)
                    cache.put(i, new Account(i, i * 100));

                System.out.println();
                System.out.println(">>> Accounts before deposit: ");
                printAccounts(cache);

                // Make transactional deposits from multiple threads in reverse order explicitly.
                // This will cause a distributed deadlock and as a result:
                // - one of the transactions will fail and it will be restarted.
                Thread th1 = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            deposit(cache, 100, false);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });

                Thread th2 = new Thread(new Runnable() {
                    @Override public void run() {
                        try {
                            deposit(cache, 200, true);
                        }
                        catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });

                // Triggering transactions with the keys obtained in the reverse order to cause the deadlock.
                th1.start();
                th2.start();

                // Waiting for the completion.
                th1.join();
                th2.join();

                System.out.println();
                System.out.println(">>> Accounts after deposit: ");
                printAccounts(cache);
            }
            finally {
                // Distributed cache could be removed from cluster only by #destroyCache() call.
                ignite.destroyCache(CACHE_NAME);
            }
        }
    }

    /**
     * Make deposit for all of the accounts.
     *
     * @param amount Amount to deposit.
     * @param reverse Whether to deposit in direct or reverse order.
     * @throws IgniteException If failed.
     */
    private static void deposit(IgniteCache<Integer, Account> cache, double amount,
        boolean reverse) throws InterruptedException {
        System.out.println(">>> Trying to deposit: " + amount);

        while (true) {
            // Starting the deadlock-free transaction (OPTIMISTIC & SERIALIZABLE).
            try (Transaction tx = Ignition.ignite().transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                if (reverse) {
                    // Preparing the updates in the reverse keys order.
                    for (int id = ENTRIES_COUNT; id > 0; id--) {
                        deposit(cache, id, amount);

                        Thread.sleep(10);
                    }
                }
                else {
                    // Preparing the updates in the natural keys order.
                    for (int id = 1; id <= ENTRIES_COUNT; id++) {
                        deposit(cache, id, amount);

                        Thread.sleep(10);
                    }
                }

                // Stop the thread deliberately to increase the chances of getting the deadlock.
                Thread.sleep(2000);

                // Committing the transaction.
                tx.commit();

                return;
            }
            catch (TransactionOptimisticException e) {

                System.out.println();
                System.out.println(">>> Transaction has failed due to the locks conflict. Restarting it...");
                System.out.println();
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
        Account acct = cache.get(acctId);

        // Deposit into account.
        acct.update(amount);

        // Store updated account in cache.
        cache.put(acctId, acct);
    }

    private static void printAccounts(IgniteCache<Integer, Account> cache) {
        for (int i = 1; i <= ENTRIES_COUNT; i++)
            System.out.println(">>> [" + i + "] = " + cache.get(i));
    }
}
