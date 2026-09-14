---
layout: post
title: "Building a Multi-Source Lakehouse Pipeline with Python, REST APIs and Databricks"
date: 2026-09-14
categories: [Data Engineering, Databricks, AWS, Python, PostgreSQL]
tags: [Python, FastAPI, PostgreSQL, AWS S3, Databricks, PySpark, Delta Lake, Medallion Architecture, ETL, REST API]
---

# Building a Multi-Source Lakehouse Pipeline with Python, REST APIs and Databricks

## Introduction

This project demonstrates an end-to-end data engineering pipeline that brings customer, product and cart data into a Databricks lakehouse through a common Python REST API.

The underlying source systems are deliberately different:

- **Customers** are stored in PostgreSQL
- **Products** are stored in PostgreSQL
- **Carts** are stored as JSON data in an Amazon S3 bucket

A Python FastAPI application provides a consistent interface to all three datasets. Databricks consumes the API and processes the data through a medallion architecture consisting of Bronze, Silver and Gold layers.

The project demonstrates several common data engineering patterns:

- Relational database integration
- REST API development
- Object storage integration
- Semi-structured JSON processing
- PySpark transformations
- Delta Lake
- Incremental processing
- Data quality
- Workflow orchestration
- Source-system abstraction
- CI/CD and operational considerations
