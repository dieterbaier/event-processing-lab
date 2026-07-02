package com.example.eventmodel;

import java.math.BigDecimal;

/**
 * Event emitted when payment for an order fails.
 * This indicates that the payment processing was unsuccessful.
 * [semantic-anchor: scenario.payment-processing]
 */
public class PaymentFailed extends Event {
    
    private String orderId;
    private String paymentId;
    private BigDecimal amount;
    private String currency;
    private String paymentMethod;
    private String failureReason;
    private String failureCode;
    private String paymentGateway;
    
    public PaymentFailed() {
        super("PaymentFailed");
    }
    
    public PaymentFailed(String orderId, String paymentId, BigDecimal amount, 
                        String currency, String paymentMethod, String failureReason,
                        String failureCode, String paymentGateway) {
        super("PaymentFailed");
        this.orderId = orderId;
        this.paymentId = paymentId;
        this.amount = amount;
        this.currency = currency;
        this.paymentMethod = paymentMethod;
        this.failureReason = failureReason;
        this.failureCode = failureCode;
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
    
    public String getFailureReason() {
        return failureReason;
    }
    
    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }
    
    public String getFailureCode() {
        return failureCode;
    }
    
    public void setFailureCode(String failureCode) {
        this.failureCode = failureCode;
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
            String.format(", PaymentFailed{orderId='%s', paymentId='%s', amount=%s, currency='%s', " +
                          "paymentMethod='%s', failureReason='%s', failureCode='%s', paymentGateway='%s'}",
                orderId, paymentId, amount, currency, paymentMethod, 
                failureReason, failureCode, paymentGateway);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        PaymentFailed that = (PaymentFailed) o;
        return orderId.equals(that.orderId) && 
               paymentId.equals(that.paymentId);
    }
    
    @Override
    public int hashCode() {
        return super.hashCode() + orderId.hashCode() + paymentId.hashCode();
    }
}