package org.apache.ignite.examples;

import java.io.Serializable;

/**
 * Account.
 */
class Account implements Serializable {
    /** Account ID. */
    private int id;

    /** Account balance. */
    private double balance;

    /**
     * @param id Account ID.
     * @param balance Balance.
     */
    Account(int id, double balance) {
        this.id = id;
        this.balance = balance;
    }

    /**
     * Change balance by specified amount.
     *
     * @param amount Amount to add to balance (may be negative).
     */
    void update(double amount) {
        balance += amount;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Account [id=" + id + ", balance=$" + balance + ']';
    }
}
