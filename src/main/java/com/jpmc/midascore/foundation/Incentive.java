package com.jpmc.midascore.foundation;

// This class stores the response from the Incentive API
public class Incentive {

    // The incentive amount returned by the API
    private float amount;

    // Empty constructor needed by Spring to convert JSON into Java
    public Incentive() {
    }

    // Lets us read the incentive amount
    public float getAmount() {
        return amount;
    }

    // Lets Spring set the incentive amount from the API response
    public void setAmount(float amount) {
        this.amount = amount;
    }
}