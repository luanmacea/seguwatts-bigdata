-- Databricks notebook source
-- Rodar uma vez, antes do primeiro upload.
CREATE SCHEMA IF NOT EXISTS workspace.gs;

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS workspace.gs.chegada;
