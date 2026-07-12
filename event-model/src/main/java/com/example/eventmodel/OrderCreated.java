package com.example.eventmodel;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.math.BigDecimal;
import java.util.List;

/**
 * Event emitted when a new order is created in the system.
 * This is the starting point of the order lifecycle.
 * [semantic-anchor: scenario.order-creation]
 */
public class OrderCreated extends Event {
    
    private String orderId;
    private String customerId;
    private List<OrderItem> orderItems;
    private BigDecimal totalAmount;
    private String currency;
    
    public OrderCreated() {
        super("OrderCreated");
    }
    
    public OrderCreated(String orderId, String customerId, List<OrderItem> orderItems, 
                       BigDecimal totalAmount, String currency) {
        super("OrderCreated");
        this.orderId = orderId;
        this.customerId = customerId;
        this.orderItems = orderItems;
        this.totalAmount = totalAmount;
        this.currency = currency;
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
    
    public List<OrderItem> getOrderItems() {
        return orderItems;
    }
    
    public void setOrderItems(List<OrderItem> orderItems) {
        this.orderItems = orderItems;
    }
    
    public BigDecimal getTotalAmount() {
        return totalAmount;
    }
    
    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }
    
    public String getCurrency() {
        return currency;
    }
    
    public void setCurrency(String currency) {
        this.currency = currency;
    }
    
    @Override
    public String toString() {
        return super.toString() + 
            String.format(", OrderCreated{orderId='%s', customerId='%s', orderItems=%s, totalAmount=%s, currency='%s'}",
                orderId, customerId, orderItems, totalAmount, currency);
    }
    
    /**
     * Represents an item in an order.
     */
    public static class OrderItem {
        private String productId;
        private int quantity;
        private BigDecimal unitPrice;
        
        public OrderItem() {
        }
        
        public OrderItem(String productId, int quantity, BigDecimal unitPrice) {
            this.productId = productId;
            this.quantity = quantity;
            this.unitPrice = unitPrice;
        }
        
        public String getProductId() {
            return productId;
        }
        
        public void setProductId(String productId) {
            this.productId = productId;
        }
        
        public int getQuantity() {
            return quantity;
        }
        
        public void setQuantity(int quantity) {
            this.quantity = quantity;
        }
        
        public BigDecimal getUnitPrice() {
            return unitPrice;
        }
        
        public void setUnitPrice(BigDecimal unitPrice) {
            this.unitPrice = unitPrice;
        }
        
        @JsonIgnore
        public BigDecimal getTotalPrice() {
            return unitPrice.multiply(BigDecimal.valueOf(quantity));
        }
        
        @Override
        public String toString() {
            return String.format("OrderItem{productId='%s', quantity=%d, unitPrice=%s}",
                productId, quantity, unitPrice);
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OrderItem orderItem = (OrderItem) o;
            return quantity == orderItem.quantity &&
                   productId.equals(orderItem.productId) &&
                   unitPrice.equals(orderItem.unitPrice);
        }
        
        @Override
        public int hashCode() {
            return productId.hashCode() + quantity + unitPrice.hashCode();
        }
    }
}