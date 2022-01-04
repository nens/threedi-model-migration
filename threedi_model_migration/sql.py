from .file import RasterOptions

import logging
import sqlite3


logger = logging.getLogger(__name__)

# build sql queries
GLOBAL_SETTINGS_SQL = "FROM v2_global_settings"
INTERFLOW_SQL = "FROM v2_global_settings LEFT JOIN v2_interflow ON v2_interflow.id = v2_global_settings.interflow_settings_id"
SIMPLE_INFILTRATION_SQL = "FROM v2_global_settings LEFT JOIN v2_simple_infiltration ON v2_simple_infiltration.id = v2_global_settings.simple_infiltration_settings_id"
GROUNDWATER_SQL = "FROM v2_global_settings LEFT JOIN v2_groundwater ON v2_groundwater.id = v2_global_settings.groundwater_settings_id"
ORDER_BY = "ORDER BY v2_global_settings.id"
RASTER_SQL_MAP = {}
for option in (
    RasterOptions.dem_file,
    RasterOptions.frict_coef_file,
    RasterOptions.interception_file,
    RasterOptions.initial_waterlevel_file,
    RasterOptions.initial_groundwater_level_file,
):
    RASTER_SQL_MAP[option] = " ".join([option.name, GLOBAL_SETTINGS_SQL, ORDER_BY])
for option in (
    RasterOptions.porosity_file,
    RasterOptions.hydraulic_conductivity_file,
):
    RASTER_SQL_MAP[option] = " ".join([option.name, INTERFLOW_SQL, ORDER_BY])
for option in (
    RasterOptions.infiltration_rate_file,
    RasterOptions.max_infiltration_capacity_file,
):
    RASTER_SQL_MAP[option] = " ".join([option.name, SIMPLE_INFILTRATION_SQL, ORDER_BY])
for option in (
    RasterOptions.equilibrium_infiltration_rate_file,
    RasterOptions.groundwater_hydro_connectivity_file,
    RasterOptions.groundwater_impervious_layer_level_file,
    RasterOptions.infiltration_decay_period_file,
    RasterOptions.initial_infiltration_rate_file,
    RasterOptions.phreatic_storage_capacity_file,
    RasterOptions.leakage_file,
):
    RASTER_SQL_MAP[option] = " ".join([option.name, GROUNDWATER_SQL, ORDER_BY])

SETTINGS_SQL = " ".join(["id, name", GLOBAL_SETTINGS_SQL, ORDER_BY])
DELETE_GLOBAL_SETTING = "DELETE FROM v2_global_settings WHERE id <> {settings_id}"
DELETE_AGG_SETTING = (
    "DELETE FROM v2_aggregation_settings WHERE global_settings_id <> {settings_id}"
)


def select(full_path, query):
    con = sqlite3.connect(full_path)
    try:
        with con:
            cursor = con.execute("SELECT " + query)
        records = cursor.fetchall()
    finally:
        con.close()

    return records


def filter_global_settings(full_path, settings_id):
    """Remove global settings, keeping only `settings_id`"""
    settings_id = int(settings_id)  # prevents SQL injection
    con = sqlite3.connect(full_path)
    try:
        with con:
            con.execute(DELETE_GLOBAL_SETTING.format(settings_id=settings_id))
            try:
                con.execute(DELETE_AGG_SETTING.format(settings_id=settings_id))
            except sqlite3.OperationalError:
                pass
    finally:
        con.close()
