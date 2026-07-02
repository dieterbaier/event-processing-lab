package com.example.eventmodel;

/**
 * Event emitted when an order is cancelled.
 * This can happen due to customer request, payment failure, or timeout.
 * [semantic-anchor: scenario.order-cancellation]
 */
public class OrderCancelled extends Event {
    
    private String orderId;
    private String customerId;
    private String cancellationReason;
    private String cancellationCode;
    private boolean refundEligible;
    private String cancelledBy;
    
    public OrderCancelled() {
        super("OrderCancelled");
    }
    
    public OrderCancelled(String orderId, String customerId, String cancellationReason,
                         String cancellationCode, boolean refundEligible, String cancelledBy) {
        super("OrderCancelled");
        this.orderId = orderId;
        this.customerId = customerId;
        this.cancellationReason = cancellationReason;
        this.cancellationCode = cancellationCode;
        this.refundEligible = refundEligible;
        this.cancelledBy = cancelledBy;
    }
    
    public String getOrderId() {
        return orderId;
    }
    
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }
    
    public String getCustomerId() {
        return customerId;
    }
    
    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }
    
    public String getCancellationReason() {
        return cancellationReason;
    }
    
    public void setCancellationReason(String cancellationReason) {
        this.cancellationReason = cancellationReason;
    }
    
    public String getCancellationCode() {
        return cancellationCode;
    }
    
    public void setCancellationCode(String cancellationCode) {
        this.cancellationCode = cancellationCode;
    }
    
    public boolean isRefundEligible() {
        return refundEligible;
    }
    
    public void setRefundEligible(boolean refundEligible) {
        this.refundEligible = refundEligible;
    }
    
    public String getCancelledBy() {
        return cancelledBy;
    }
    
    public void setCancelledBy(String cancelledBy) {
        this.cancelledBy = cancelledBy;
    }
    
    @Override
    public String toString() {
        return super.toString() + 
            String.format(", OrderCancelled{orderId='%s', customerId='%s', cancellationReason='%s', " +
                          "cancellationCode='%s', refundEligible=%s, cancelledBy='%s'}",
                orderId, customerId, cancellationReason, 
                cancellationCode, refundEligible, cancelledBy);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        OrderCancelled that = (OrderCancelled) o;
        return orderId.equals(that.orderId);
    }
    
    @Override
    public int hashCode() {
        return super.hashCode() + orderId.hashCode();
    }
}