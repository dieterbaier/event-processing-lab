# Living documentation for the order-lifecycle use case that every processing
# technology in this lab implements (kafka-consumer, esper-cep, flink-basic,
# flink-cep).
#
# Test bridge: PENDING (roadmap Phase 4). These scenarios describe observable
# behaviour, not implementation. When tests are added, each scenario maps to at
# least one automated test named after the sanitized scenario title, with
# Given / When / Then comment anchors, per the architecture-knowledge-toolkit
# `bdd-specification` skill. Until then these specs are proposed and unverified.
#
# Traceability: runtime scenarios in
# src/docs/arc42/doc-06000-runtime-view.adoc; quality scenarios in
# src/docs/arc42/doc-10000-quality-requirements.adoc.

@order-lifecycle
Feature: Order lifecycle event processing
  As an integration architect comparing event processing technologies
  I want each technology to detect the same order-lifecycle patterns
  So that their behaviour can be compared on equal terms

  Background:
    Given a running processor consuming the order-events topic

  Scenario: Order creation is detected
    When an OrderCreated event is published for an order
    Then the processor detects the new order

  Scenario: Payment processing confirms an order
    Given an OrderCreated event has been published for an order
    When a PaymentReceived event is published for the same order
    Then the processor detects the order as paid

  Scenario: Payment timeout cancels an unpaid order
    Given an OrderCreated event has been published for an order
    When no PaymentReceived event arrives within the payment timeout window
    Then the processor detects a payment timeout for the order
    And the order is treated as cancelled

  Scenario: Order cancellation is handled
    Given an OrderCreated event has been published for an order
    When an OrderCancelled event is published for the same order
    Then the processor detects the cancellation of the order

  Scenario: Shipment starts after payment
    Given an order has been created and paid
    When a ShipmentStarted event is published for the same order
    Then the processor detects the shipment for the order
