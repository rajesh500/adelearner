# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
order = {
    "id": "ORD102",
    "customerId": "C101",
    "product": "Mouse",
    "quantity": 1,
    "status": "New"
}

created = container.create_item(body=order)

## accessing the item key
print(created["id"])      # ORD101
print(created["status"])  # New