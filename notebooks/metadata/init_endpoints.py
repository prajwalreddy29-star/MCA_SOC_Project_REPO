# Databricks notebook source
# MAGIC %run ../_bootstrap

# COMMAND ----------

# DBTITLE 1,Set environment variables
# Set environment variables as widgets for ${...} substitution in SQL cells
dbutils.widgets.text("target_env", "")
dbutils.widgets.text("country_code", "GSOC")
dbutils.widgets.text("api_country_code", "GC_EU")

from generic.config.environment import get_environment

# Use the parameter when provided.
# Otherwise, detect the environment from the compute tags.
target_env = get_environment(
    spark=spark,
    job_environment=dbutils.widgets.get("target_env"),
)

# Update the widget with the resolved value so that SQL cells
# can continue to use :target_env.
dbutils.widgets.remove("target_env")
dbutils.widgets.text("target_env", target_env)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO IDENTIFIER(
# MAGIC     CONCAT('cdp_data_gsoc_', :target_env, '.control_gsoc.endpoint_config ')
# MAGIC )
# MAGIC (
# MAGIC     endpoint_id,
# MAGIC     country_code,
# MAGIC     endpoint_name,
# MAGIC     entity_name,
# MAGIC     api_country_code,
# MAGIC     controller_type,
# MAGIC     ingestion_mode,
# MAGIC     load_pattern,
# MAGIC     initial_lookback_days,
# MAGIC     initial_window_days,
# MAGIC     ssae_api_class_method,
# MAGIC     target_bronze_table,
# MAGIC     max_fetch_size,
# MAGIC     is_active
# MAGIC )
# MAGIC VALUES
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- SETUP / REFERENCE TABLES - DAILY FULL LOAD
# MAGIC -- ============================================================
# MAGIC (CONCAT(:country_code, '_agencies'), :country_code, 'agencies', 'agency', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.agency'), 10000, true),
# MAGIC (CONCAT(:country_code, '_agencyTypes'), :country_code, 'agencyTypes', 'agencytype', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.agencytype'), 10000, true),
# MAGIC (CONCAT(:country_code, '_branches'), :country_code, 'branches', 'branch', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.branch'), 10000, true),
# MAGIC (CONCAT(:country_code, '_branding'), :country_code, 'branding', 'branding', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.branding'), 10000, true),
# MAGIC (CONCAT(:country_code, '_callDispositions'), :country_code, 'callDispositions', 'calldisposition', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.calldisposition'), 10000, true),
# MAGIC (CONCAT(:country_code, '_csPartitions'), :country_code, 'csPartitions', 'cspartition', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.cspartition'), 10000, true),
# MAGIC (CONCAT(:country_code, '_daylightSavingTimes'), :country_code, 'daylightSavingTimes', 'daylightsavingstime', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.daylightsavingstime'), 10000, true),
# MAGIC (CONCAT(:country_code, '_equipmentTypes'), :country_code, 'equipmentTypes', 'equipmenttype', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.equipmenttype'), 10000, true),
# MAGIC (CONCAT(:country_code, '_events'), :country_code, 'events', 'event', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.event'), 10000, true),
# MAGIC (CONCAT(:country_code, '_eventReportCodes'), :country_code, 'eventReportCodes', 'eventreportcode', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.eventreportcode'), 10000, true),
# MAGIC (CONCAT(:country_code, '_languages'), :country_code, 'languages', 'language', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.language'), 10000, true),
# MAGIC (CONCAT(:country_code, '_oosCategories'), :country_code, 'oosCategories', 'ooscategory', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.ooscategory'), 10000, true),
# MAGIC (CONCAT(:country_code, '_operatorActionDispositions'), :country_code, 'operatorActionDispositions', 'operatoractiondisposition', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.operatoractiondisposition'), 10000, true),
# MAGIC (CONCAT(:country_code, '_options'), :country_code, 'options', 'option', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.option'), 10000, true),
# MAGIC (CONCAT(:country_code, '_phoneTypes'), :country_code, 'phoneTypes', 'phonetype', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.phonetype'), 10000, true),
# MAGIC (CONCAT(:country_code, '_serviceCompanies'), :country_code, 'serviceCompanies', 'servicecompany', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.servicecompany'), 10000, true),
# MAGIC (CONCAT(:country_code, '_serviceTypes'), :country_code, 'serviceTypes', 'servicetype', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.servicetype'), 10000, true),
# MAGIC (CONCAT(:country_code, '_siteLocations'), :country_code, 'siteLocations', 'sitelocation', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.sitelocation'), 10000, true),
# MAGIC (CONCAT(:country_code, '_siteStatuses'), :country_code, 'siteStatuses', 'sitestatus', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.sitestatus'), 10000, true),
# MAGIC (CONCAT(:country_code, '_siteTypes'), :country_code, 'siteTypes', 'sitetype', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.sitetype'), 10000, true),
# MAGIC (CONCAT(:country_code, '_states'), :country_code, 'states', 'state', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.state'), 10000, true),
# MAGIC (CONCAT(:country_code, '_systemTypes'), :country_code, 'systemTypes', 'systemtype', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.systemtype'), 10000, true),
# MAGIC (CONCAT(:country_code, '_timeZones'), :country_code, 'timeZones', 'timezone', :api_country_code, 'setup', 'full', '', NULL, NULL, 'get_setup_table', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_reference.timezone'), 10000, true);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO IDENTIFIER(
# MAGIC     CONCAT('cdp_data_gsoc_', :target_env, '.control_gsoc.endpoint_config ')
# MAGIC )
# MAGIC (
# MAGIC     endpoint_id,
# MAGIC     country_code,
# MAGIC     endpoint_name,
# MAGIC     entity_name,
# MAGIC     api_country_code,
# MAGIC     controller_type,
# MAGIC     ingestion_mode,
# MAGIC     load_pattern,
# MAGIC     initial_lookback_days,
# MAGIC     initial_window_days,
# MAGIC     ssae_api_class_method,
# MAGIC     target_bronze_table,
# MAGIC     max_fetch_size,
# MAGIC     is_active
# MAGIC )
# MAGIC VALUES
# MAGIC -- ============================================================
# MAGIC -- OPERATIONAL / TRANSACTIONAL TABLES - INITIAL LOAD (ALL DATA OR HISTORICAL)
# MAGIC -- ============================================================
# MAGIC (CONCAT(:country_code, '_sites'), :country_code, 'sites', 'site', :api_country_code, 'cdp', 'initial', 'snapshot', NULL, NULL, 'get_sites', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.site'), 10000, true),
# MAGIC (CONCAT(:country_code, '_systems'), :country_code, 'systems', 'system', :api_country_code, 'cdp', 'initial', 'snapshot', NULL, NULL, 'get_systems', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.system'), 10000, true),
# MAGIC (CONCAT(:country_code, '_alarmIncidents'), :country_code, 'alarmIncidents', 'alarm_incident', :api_country_code, 'cdp', 'initial', 'historical', 20, 10, 'get_alarm_incidents', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.alarm_incident'), 10000, true),
# MAGIC (CONCAT(:country_code, '_alarmIncidentCalls'), :country_code, 'alarmIncidentCalls', 'alarm_incident_call', :api_country_code, 'cdp', 'initial', 'historical', 20, 10, 'get_alarm_incidentcalls', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.alarm_incident_call'), 10000, true),
# MAGIC (CONCAT(:country_code, '_eventHistories'), :country_code, 'eventHistories', 'event_history', :api_country_code, 'cdp', 'initial', 'historical', 20, 10, 'get_event_histories', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.event_history'), 10000, true),
# MAGIC (CONCAT(:country_code, '_restrictionInclusions'), :country_code, 'restrictionInclusions', 'restriction', :api_country_code, 'cdp', 'initial', 'snapshot', NULL, NULL, 'get_restriction_inclusions', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.restriction_inclusion'), 10000, true),
# MAGIC (CONCAT(:country_code, '_restrictionExclusions'), :country_code, 'restrictionExclusions', 'restriction', :api_country_code, 'cdp', 'initial', 'snapshot', NULL, NULL, 'get_restriction_exclusions', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.restriction_exclusion'), 10000, true), 
# MAGIC -- ============================================================
# MAGIC -- OPERATIONAL / TRANSACTIONAL TABLES - INCREMENTAL - CHANGES / DELETE
# MAGIC -- ============================================================
# MAGIC (CONCAT(:country_code, '_sites_changes'), :country_code, 'sites_changes', 'site', :api_country_code, 'cdp', 'incremental', 'dates_based', NULL, NULL, 'get_site_changes', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.site_change'), 10000, true),
# MAGIC (CONCAT(:country_code, '_sites_deletes'), :country_code, 'sites_deletes', 'site', :api_country_code, 'cdp', 'incremental', 'dates_based', NULL, NULL, 'get_site_deletes', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.site_delete'), 10000, true),
# MAGIC (CONCAT(:country_code, '_systems_changes'), :country_code, 'systems_changes', 'system', :api_country_code, 'cdp', 'incremental', 'dates_based', NULL, NULL, 'get_system_changes', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.system_change'), 10000, true),
# MAGIC (CONCAT(:country_code, '_systems_deletes'), :country_code, 'systems_deletes', 'system', :api_country_code, 'cdp', 'incremental', 'dates_based', NULL, NULL, 'get_system_deletes', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.system_delete'), 10000, true),
# MAGIC (CONCAT(:country_code, '_alarmIncidents_changes'), :country_code, 'alarmIncidents_changes', 'alarm_incident', :api_country_code, 'cdp', 'incremental', 'dates_based', NULL, NULL, 'get_alarm_incident_changes', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.alarm_incident_change'), 10000, true),
# MAGIC (CONCAT(:country_code, '_alarmIncidentCalls_changes'), :country_code, 'alarmIncidentCalls_changes', 'alarm_incident_call', :api_country_code, 'cdp', 'incremental', 'dates_based', NULL, NULL, 'get_alarm_incidentcalls_changes', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.alarm_incident_call_change'), 10000, true),
# MAGIC (CONCAT(:country_code, '_eventHistories_changes'), :country_code, 'eventHistories_changes', 'event_history', :api_country_code, 'cdp', 'incremental', 'cursor_based', NULL, NULL, 'get_event_history_changes', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.event_history'), 10000, true),
# MAGIC (CONCAT(:country_code, '_restrictions_changes'), :country_code, 'restrictions_changes', 'restriction', :api_country_code, 'cdp', 'incremental', 'dates_based', NULL, NULL, 'get_restriction_changes', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.restriction_change'), 10000, true)
# MAGIC ;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- INITIALIZE INGESTION STATE FOR ALL ACTIVE ENDPOINTS
# MAGIC -- ============================================================
# MAGIC INSERT INTO IDENTIFIER(
# MAGIC     CONCAT('cdp_data_gsoc_', :target_env, '.control_gsoc.ingestion_state')
# MAGIC )
# MAGIC (
# MAGIC     endpoint_id,
# MAGIC     initial_load_completed,
# MAGIC     last_status,
# MAGIC     last_success_bronze_run_id,
# MAGIC     last_success_at,
# MAGIC     last_cursor,
# MAGIC     last_from_datetime,
# MAGIC     last_to_datetime,
# MAGIC     last_records_fetched,
# MAGIC     last_records_written,
# MAGIC     last_error_message
# MAGIC )
# MAGIC SELECT
# MAGIC     cfg.endpoint_id,
# MAGIC     CASE WHEN cfg.ingestion_mode = 'initial' THEN false ELSE true END,
# MAGIC     'NOT_STARTED',
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL,
# MAGIC     NULL
# MAGIC FROM IDENTIFIER(CONCAT('cdp_data_gsoc_', :target_env, '.control_gsoc.endpoint_config')) cfg
# MAGIC LEFT JOIN IDENTIFIER(CONCAT('cdp_data_gsoc_', :target_env, '.control_gsoc.ingestion_state')) st
# MAGIC     ON cfg.endpoint_id = st.endpoint_id  
# MAGIC WHERE cfg.is_active = true AND st.endpoint_id IS NULL;
# MAGIC

# COMMAND ----------

# MAGIC %sql    
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- INITIALIZE silver_config
# MAGIC -- ============================================================
# MAGIC INSERT INTO IDENTIFIER(
# MAGIC     CONCAT('cdp_data_gsoc_', :target_env, '.control_gsoc.silver_config')
# MAGIC )
# MAGIC (
# MAGIC     silver_task_id,
# MAGIC     country_code,
# MAGIC     entity_name,
# MAGIC     source_bronze_table,
# MAGIC     target_silver_table,
# MAGIC     source_endpoint_id,
# MAGIC     processing_pattern,
# MAGIC     processing_order,
# MAGIC     primary_key_columns,
# MAGIC     dedup_order_columns,
# MAGIC     is_active
# MAGIC )
# MAGIC -- ============================================================
# MAGIC -- -- SETUP / REFERENCE TABLES - DAILY FULL LOAD
# MAGIC -- ============================================================
# MAGIC SELECT
# MAGIC     CONCAT(:country_code, '_setup_', cfg.entity_name) AS silver_task_id,
# MAGIC     cfg.country_code,
# MAGIC     cfg.entity_name,
# MAGIC     cfg.target_bronze_table AS source_bronze_table,
# MAGIC     REPLACE(cfg.target_bronze_table, '.bronze_gsoc_reference.', '.silver_gsoc_reference.') AS target_silver_table,
# MAGIC     cfg.endpoint_id AS source_endpoint_id,
# MAGIC     'replace' AS processing_pattern,
# MAGIC     1 as processing_order,
# MAGIC     array() AS primary_key_columns,
# MAGIC     array() AS dedup_order_columns,
# MAGIC     true AS is_active
# MAGIC FROM IDENTIFIER(CONCAT('cdp_data_gsoc_', :target_env, '.control_gsoc.endpoint_config')) cfg
# MAGIC WHERE cfg.controller_type = 'setup'
# MAGIC   AND cfg.is_active = true

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC INSERT INTO IDENTIFIER(
# MAGIC     CONCAT('cdp_data_gsoc_', :target_env, '.control_gsoc.silver_config')
# MAGIC )
# MAGIC (
# MAGIC     silver_task_id,
# MAGIC     country_code,
# MAGIC     entity_name,
# MAGIC     source_bronze_table,
# MAGIC     target_silver_table,
# MAGIC     source_endpoint_id,
# MAGIC     processing_pattern,
# MAGIC     processing_order,
# MAGIC     primary_key_columns,
# MAGIC     dedup_order_columns,
# MAGIC     is_active
# MAGIC )
# MAGIC VALUES
# MAGIC -- ============================================================
# MAGIC -- OPERATIONAL / TRANSACTIONAL TABLES - INITIAL LOAD (ALL DATA OR HISTORICAL)
# MAGIC -- ============================================================
# MAGIC
# MAGIC (CONCAT(:country_code, '_intial_sites'), :country_code, 'site', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.site'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_operational.site'), CONCAT(:country_code, '_sites'), 'merge_scd1', 1, array('site_no'),array('ingested_at'), true),
# MAGIC (CONCAT(:country_code, '_initial_systems'), :country_code, 'system', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.system'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_operational.system'), CONCAT(:country_code, '_systems'), 'merge_scd1', 1, array('system_no'), array('ingested_at'), true),
# MAGIC (CONCAT(:country_code, '_initial_alarm_incidents'), :country_code, 'alarm_incident', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.alarm_incident'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_transactional.alarm_incident'), CONCAT(:country_code, '_alarmIncidents'), 'merge_scd1', 1, array('alarminc_no'), array('ingested_at'), true),
# MAGIC (CONCAT(:country_code, '_initial_alarm_incident_calls'), :country_code, 'alarm_incident_call', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.alarm_incident_call'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_transactional.alarm_incident_call'), CONCAT(:country_code, '_alarmIncidentCalls'), 'merge_scd1', 1, array('alarminc_no','seqno'), array('ingested_at'), true),
# MAGIC (CONCAT(:country_code, '_initial_event_histories'), :country_code, 'event_history', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.event_history'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_transactional.event_history'), CONCAT(:country_code, '_eventHistories'), 'append', 1, array('event_date','seqno','server_id','system_no'), array('ingested_at'), true)
# MAGIC ,
# MAGIC -- ============================================================
# MAGIC -- OPERATIONAL / TRANSACTIONAL TABLES - INCREMENTAL - CHANGES / DELETE
# MAGIC -- ============================================================
# MAGIC (CONCAT(:country_code, '_incremental_sites_changes'), :country_code, 'site', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.site_change'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_operational.site'), CONCAT(:country_code, '_sites_changes'), 'merge_changes', 2, array('site_no'), array('ingested_at'), true),
# MAGIC (CONCAT(:country_code, '_incremental_sites_deletes'), :country_code, 'site', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.site_delete'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_operational.site'), CONCAT(:country_code, '_sites_deletes'), 'delete', 3, array('site_no'), array('ingested_at'), true),
# MAGIC (CONCAT(:country_code, '_incremental_systems_changes'), :country_code, 'system', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.system_change'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_operational.system'), CONCAT(:country_code, '_systems_changes'), 'merge_changes', 2 , array('system_no'), array('ingested_at'), true),
# MAGIC (CONCAT(:country_code, '_incremental_systems_deletes'), :country_code, 'system', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_operational.system_delete'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_operational.system'), CONCAT(:country_code, '_systems_deletes'), 'delete', 3 , array('system_no'), array('ingested_at'), true),
# MAGIC (CONCAT(:country_code, '_incremental_alarmIncidents_changes'), :country_code, 'alarm_incident', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.alarm_incident_change'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_transactional.alarm_incident'), CONCAT(:country_code, '_alarmIncidents_changes'), 'merge_changes', 2, array('alarminc_no'), array('ingested_at'), true),
# MAGIC (CONCAT(:country_code, '_incremental_alarmIncidentCalls_changes'), :country_code, 'alarm_incident_call', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.alarm_incident_call_change'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_transactional.alarm_incident_call'), CONCAT(:country_code, '_alarmIncidentCalls_changes'), 'merge_changes', 2, array('alarminc_no','seqno'), array('ingested_at'), true),
# MAGIC (CONCAT(:country_code, '_incremental_eventHistories_changes'), :country_code, 'event_history', CONCAT('cdp_data_gsoc_', :target_env, '.bronze_gsoc_transactional.event_history'), CONCAT('cdp_data_gsoc_', :target_env, '.silver_gsoc_transactional.event_history'), CONCAT(:country_code, '_eventHistories_changes'), 'append', 2, array('event_date','seqno','server_id','system_no'), array('ingested_at'), true)
# MAGIC
