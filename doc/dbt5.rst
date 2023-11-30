=====================================
Database Test 5 (DBT-5) Documentation
=====================================

.. contents:: Table of Contents

------------
Introduction
------------

This document provides instructions on how to set up and use the Open Source
Development Lab's Database Test 5 (DBT-5) kit.  This kit is an open source
fair-use implementation of the TPC Benchmark(TM) E (TPC-E) specification, which
is an on-line transaction processing benchmark.

An introduction and complete details on the TPC-E can be found at:
https://tpc.org/tpce/

------
Design
------

DBT-5 implements the workload with three binaries: **BrokerageHouse**,
**MarketExchange**, and **Driver**.

BrokerageHouse
==============

**BrokerageHouse** is responsible for executing all of the database
transactions.  It implements the **EGenTxnHarness** and **EGenDriverDM**.

MarketExchange
==============

**MarketExchange** emulates stock exchanges and sends messages to the
**BrokerageHouse** for initial Trade Request and Mark Feed transactions.  It
implements the **EGenDriverMEE**.

Driver
======

**Driver** emulates customers and drives the workload.  It implements
**EGenDriverCE**.

----------------
Installing DBT-5
----------------

The latest stable and development version of the kit can be found on GitHub:
https://github.com/osdldbt/dbt5

The TPC's TPC-E Tools cannot be redistributed with DBT-5 and must be downloaded
separately :
https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp

Required Software
=================

In addition to the database management system software, the following is also
required:

* C++ compiler
* patch
* Database development libraries and header files
    * PostgreSQL https://www.postgresql.org
        * psql
* SQLite https://www.sqlite.org/index.html
* **sar**, **pidstat** http://pagesperso-orange.fr/sebastien.godard/ (While the
  scripts assume this particular version of **sar** and **pidstat**, it is
  possible to run on non-Linux based operating systems with some modifications
  to the kit.)

.. include:: user-guide.rst

--------------------------------
Database Management System Notes
--------------------------------

.. include:: postgresql.rst

.. include:: dev-guide.rst
