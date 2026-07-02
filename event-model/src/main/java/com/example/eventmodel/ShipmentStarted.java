package com.example.eventmodel;

/**
 * Event emitted when shipment for an order starts.
 * This indicates that the order has been processed and is now being shipped.
 * [semantic-anchor: scenario.shipment-start]
 */
public class ShipmentStarted extends Event {
    
    private String orderId;
    private String shipmentId;
    private String trackingNumber;
    private String carrier;
    private String carrierService;
    private String originAddress;
    private String destinationAddress;
    private long estimatedDeliveryDays;
    
    public ShipmentStarted() {
        super("ShipmentStarted");
    }
    
    public ShipmentStarted(String orderId, String shipmentId, String trackingNumber,
                          String carrier, String carrierService, String originAddress,
                          String destinationAddress, long estimatedDeliveryDays) {
        super("ShipmentStarted");
        this.orderId = orderId;
        this.shipmentId = shipmentId;
        this.trackingNumber = trackingNumber;
        this.carrier = carrier;
        this.carrierService = carrierService;
        this.originAddress = originAddress;
        this.destinationAddress = destinationAddress;
        this.estimatedDeliveryDays = estimatedDeliveryDays;
    }
    
    public String getOrderId() {
        return orderId;
    }
    
    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }
    
    public String getShipmentId() {
        return shipmentId;
    }
    
    public void setShipmentId(String shipmentId) {
        this.shipmentId = shipmentId;
    }
    
    public String getTrackingNumber() {
        return trackingNumber;
    }
    
    public void setTrackingNumber(String trackingNumber) {
        this.trackingNumber = trackingNumber;
    }
    
    public String getCarrier() {
        return carrier;
    }
    
    public void setCarrier(String carrier) {
        this.carrier = carrier;
    }
    
    public String getCarrierService() {
        return carrierService;
    }
    
    public void setCarrierService(String carrierService) {
        this.carrierService = carrierService;
    }
    
    public String getOriginAddress() {
        return originAddress;
    }
    
    public void setOriginAddress(String originAddress) {
        this.originAddress = originAddress;
    }
    
    public String getDestinationAddress() {
        return destinationAddress;
    }
    
    public void setDestinationAddress(String destinationAddress) {
        this.destinationAddress = destinationAddress;
    }
    
    public long getEstimatedDeliveryDays() {
        return estimatedDeliveryDays;
    }
    
    public void setEstimatedDeliveryDays(long estimatedDeliveryDays) {
        this.estimatedDeliveryDays = estimatedDeliveryDays;
    }
    
    @Override
    public String toString() {
        return super.toString() + 
            String.format(", ShipmentStarted{orderId='%s', shipmentId='%s', trackingNumber='%s', " +
                          "carrier='%s', carrierService='%s', originAddress='%s', " +
                          "destinationAddress='%s', estimatedDeliveryDays=%d}",
                orderId, shipmentId, trackingNumber, carrier, carrierService, 
                originAddress, destinationAddress, estimatedDeliveryDays);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ShipmentStarted that = (ShipmentStarted) o;
        return orderId.equals(that.orderId) && 
               shipmentId.equals(that.shipmentId);
    }
    
    @Override
    public int hashCode() {
        return super.hashCode() + orderId.hashCode() + shipmentId.hashCode();
    }
}