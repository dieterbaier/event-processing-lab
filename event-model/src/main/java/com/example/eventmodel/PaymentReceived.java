package com.example.eventmodel;

import java.math.BigDecimal;

/**
 * Event emitted when payment for an order is successfully received.
 * This typically follows an OrderCreated event and indicates successful payment processing.
 * [semantic-anchor: scenario.payment-processing]
 */
public class PaymentReceived extends Event {
    
    private String orderId;
    private String paymentId;
    private BigDecimal amount;
    private String currency;
    private String paymentMethod;
    private String transactionId;
    private String paymentGateway;
    
    public PaymentReceived() {
        super("PaymentReceived");
    }
    
    public PaymentReceived(String orderId, String paymentId, BigDecimal amount, 
                           String currency, String paymentMethod, String transactionId, 
                           String paymentGateway) {
        super("PaymentReceived");
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.amount = amount;
        this.currency = currency;
        this.paymentMethod = paymentMethod;
        this.transactionId = transactionId;
        this.paymentGateway = paymentGateway;
    }
    
    public String getOrderId() {
        return orderId;
    }
    
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }
    
    public String getPaymentId() {
        return paymentId;
    }
    
    public void setPaymentId(String paymentId) {
        this.paymentId = paymentId;
    }
    
    public BigDecimal getAmount() {
        return amount;
    }
    
    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }
    
    public String getCurrency() {
        return currency;
    }
    
    public void setCurrency(String currency) {
        this.currency = currency;
    }
    
    public String getPaymentMethod() {
        return paymentMethod;
    }
    
    public void setPaymentMethod(String paymentMethod) {
        this.paymentMethod = paymentMethod;
    }
    
    public String getTransactionId() {
        return transactionId;
    }
    
    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }
    
    public String getPaymentGateway() {
        return paymentGateway;
    }
    
    public void setPaymentGateway(String paymentGateway) {
        this.paymentGateway = paymentGateway;
    }
    
    @Override
    public String toString() {
        return super.toString() + 
            String.format(", PaymentReceived{orderId='%s', paymentId='%s', amount=%s, currency='%s', " +
                          "paymentMethod='%s', transactionId='%s', paymentGateway='%s'}",
                orderId, paymentId, amount, currency, paymentMethod, transactionId, paymentGateway);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        PaymentReceived that = (PaymentReceived) o;
        return orderId.equals(that.orderId) && 
               paymentId.equals(that.paymentId);
    }
    
    @Override
    public int hashCode() {
        return super.hashCode() + orderId.hashCode() + paymentId.hashCode();
    }
}