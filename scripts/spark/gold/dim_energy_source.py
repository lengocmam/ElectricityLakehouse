"""Canonical generation source names and keys for Gold."""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.errors import AnalysisException
from pyspark.sql.window import Window

from bronze.nsmo_power import EXPECTED_SOURCES
from silver.silver_utils import normalize_text, stable_hash


# Exact aliases only; distinct fuels, estimates and totals remain separate.
SOURCE_ALIASES = {
    "Nhiệt điện dầu:": "Nhiệt điện dầu",
    "Nhiệt điện than:": "Nhiệt điện than",
    "Thủy điện:": "Thủy điện",
    "Điện gió:": "Điện gió",
    "Gió": "Điện gió",
    "Điện mặt trời:": "Điện mặt trời",
    "Nhập khẩu điện:": "Nhập khẩu điện",
    "Mặt trời farm": "ĐMT trang trại",
    "Tuabin khí (GAS + Dầu DO)": "Tuabin khí (Gas + Dầu DO)",
    "Tuabin khí (Gas + Dầu DO):": "Tuabin khí (Gas + Dầu DO)",
    "Tuabin khí (Gas + dầu DO)": "Tuabin khí (Gas + Dầu DO)",
    "*Tuabin khí (Chạy dầu)": "*Tuabin khí (Chạy Dầu)",
    "Toàn quốc": "Toàn Quốc",
    "Quốc gia": "Toàn Quốc",
    "ĐMTMN (ĐC)": "ĐMT mái nhà (ước tính đầu cực)",
    "Loại khác:": "Loại khác",
}

# A null renewable flag means the label does not establish the energy mix.
SOURCE_CATEGORIES = {
    "Nhiệt điện dầu": ("OIL", False),
    "Nhiệt điện dầu DO": ("OIL_DO", False),
    "Nhiệt điện dầu FO": ("OIL_FO", False),
    "Nhiệt điện than": ("COAL", False),
    "Nhiệt điện khác": ("THERMAL_OTHER", None),
    "Thủy điện": ("HYDRO", True),
    "Điện gió": ("WIND", True),
    "Điện mặt trời": ("SOLAR_UNSPECIFIED", True),
    "ĐMT trang trại": ("SOLAR_FARM", True),
    "ĐMT mái nhà": ("SOLAR_ROOFTOP", True),
    "ĐMT mái nhà (ước tính thương phẩm)": ("SOLAR_ROOFTOP_COMMERCIAL", True),
    "ĐMT mái nhà (ước tính đầu cực)": ("SOLAR_ROOFTOP_TERMINAL", True),
    "Sinh khối": ("BIOMASS", True),
    "Tuabin khí": ("GAS_TURBINE", False),
    "Tuabin khí (GAS)": ("GAS_TURBINE", False),
    "Tuabin khí (DO)": ("OIL_TURBINE", False),
    "Tuabin khí (Gas + Dầu DO)": ("GAS_OIL_TURBINE", False),
    "*Tuabin khí (Chạy GAS)": ("GAS_TURBINE", False),
    "*Tuabin khí (Chạy Dầu)": ("OIL_TURBINE", False),
    "Nhập khẩu điện": ("IMPORT_UNSPECIFIED", None),
    "Mua TQ": ("PURCHASE_UNRESOLVED", None),
    "Loại khác": ("OTHER_SOURCE", None),
    "Toàn Quốc": ("SYSTEM_TOTAL", None),
    "Quốc gia + ĐMT mái nhà (ước tính thương phẩm)": ("SYSTEM_TOTAL", None),
    "Quốc gia + ĐMT mái nhà (ước tính đầu cực)": ("SYSTEM_TOTAL", None),
    "Công suất lớn nhất trong ngày:": ("REPORT_METRIC", None),
    "Sản lượng điện sản xuất và nhập khẩu:": ("REPORT_METRIC", None),
    "UNKNOWN_SOURCE": ("UNKNOWN", None),
}


def _lookup(entries: dict, identity: F.Column) -> F.Column:
    normalized = {key.lower(): item for key, item in entries.items()}
    pairs = [value for key, item in normalized.items()
             for value in (F.lit(key), F.lit(item))]
    return F.element_at(F.create_map(*pairs), identity)


def source_display_name(source_name: F.Column) -> F.Column:
    """Resolve documented aliases and retain blank historical labels."""
    cleaned = normalize_text(source_name)
    cleaned = F.when(cleaned.isNull() | (cleaned == ""), F.lit("UNKNOWN_SOURCE")).otherwise(cleaned)
    canonical = {name: name for name in SOURCE_CATEGORIES}
    canonical.update(SOURCE_ALIASES)
    return F.when(
        F.lower(cleaned).rlike(r"^khác\s*\(.*\):?$"), F.lit("Loại khác")
    ).otherwise(F.coalesce(_lookup(canonical, F.lower(cleaned)), cleaned))


def source_identity(source_name: F.Column) -> F.Column:
    """Use the canonical name to identify one business source."""
    return F.lower(source_display_name(source_name))


def source_key_from_name(source_name: F.Column) -> F.Column:
    """Use the first 60 bits of the canonical name's SHA-256 hash."""
    digest = F.sha2(source_identity(source_name), 256)
    return F.conv(F.substring(digest, 1, 15), 16, 10).cast("long")


def generate_dim_energy_source(spark: SparkSession) -> DataFrame:
    """Build distinct source members from EVN reports and NSMO source labels."""
    names = [spark.createDataFrame(
        [(name,) for name in sorted(EXPECTED_SOURCES)], ["source_name"]
    )]
    for table, column in (
        ("nessie.silver.generation", "energy_source"),
        ("nessie.silver.power_daily", "generation_source"),
    ):
        try:
            if spark.catalog.tableExists(table):
                names.append(
                    spark.read.option("vectorization-enabled", "false").table(table)
                    .select(F.col(column).alias("source_name"))
                    .distinct()
                )
        except AnalysisException as exc:
            missing_catalog = ("CATALOG_NOT_FOUND", "SCHEMA_NOT_FOUND",
                               "REQUIRES_SINGLE_PART_NAMESPACE")
            if not any(code in str(exc) for code in missing_catalog):
                raise

    categories = {name: category for name, (category, _) in SOURCE_CATEGORIES.items()}
    renewables = {name: renewable for name, (_, renewable) in SOURCE_CATEGORIES.items()
                  if renewable is not None}
    distinct = (
        reduce(lambda left, right: left.unionByName(right), names)
        .withColumn("source_name", source_display_name(F.col("source_name")))
        .withColumn("identity", F.lower("source_name"))
        .groupBy("identity")
        .agg(F.min("source_name").alias("source_name"))
        .withColumn("source_code", F.sha2(F.col("identity"), 256))
        .withColumn("energy_source_key", source_key_from_name(F.col("source_name")))
        .withColumn("renewable_flag", _lookup(renewables, F.col("identity")).cast("boolean"))
        .withColumn("source_category", F.coalesce(
            _lookup(categories, F.col("identity")), F.lit("UNCLASSIFIED")))
    )
    if distinct.groupBy("energy_source_key").count().where(F.col("count") > 1).limit(1).count():
        raise ValueError("Energy source hash collision; choose a wider source key")
    distinct = (distinct.withColumn("canonical_energy_source_key", F.col("energy_source_key"))
                .withColumn("is_active", F.lit(True)))
    return distinct.select("energy_source_key", "source_code", "source_name",
                           "renewable_flag", "source_category",
                           "canonical_energy_source_key", "is_active")


def migrate_energy_source_aliases(spark: SparkSession) -> None:
    """Repoint related facts; preserve old dimension rows as historical aliases."""
    dim_table = "nessie.gold.dim_energy_source"
    members = spark.table(dim_table)
    aliases = (members.where(F.col("is_active").isNull() | F.col("is_active"))
               .select(F.col("energy_source_key").alias("_old_key"), "source_name")
               .withColumn("_new_key", source_key_from_name(F.col("source_name")))
               .where(F.col("_old_key") != F.col("_new_key"))
               .select("_old_key", "_new_key"))
    mapping = aliases.collect()
    if not mapping:
        return
    if aliases.join(members.select(F.col("energy_source_key").alias("_new_key")),
                    "_new_key", "left_anti").limit(1).count():
        raise ValueError("Canonical energy source member missing; migration aborted")
    old_keys = [row._old_key for row in mapping]

    for fact_name, keys in (
        ("fact_generation_daily", ("date_key", "energy_source_key")),
        ("fact_generation_power_daily", ("date_key", "period_key", "energy_source_key")),
        ("fact_generation_power_hourly", ("date_key", "generation_mix_key")),
    ):
        table = f"nessie.gold.{fact_name}"
        if not spark.catalog.tableExists(table):
            continue
        original = spark.table(table)
        mapped = (original.join(F.broadcast(aliases),
                                original.energy_source_key == aliases._old_key, "left")
                  .drop("_old_key")
                  .withColumn("_original_energy_key", F.col("energy_source_key"))
                  .withColumn("energy_source_key",
                              F.coalesce(F.col("_new_key"), F.col("energy_source_key")))
                  .drop("_new_key"))
        if fact_name != "fact_generation_power_hourly":
            if mapped.groupBy(*keys).count().where(F.col("count") > 1).limit(1).count():
                raise ValueError(f"Canonical source duplicates the grain of {fact_name}")
            if fact_name == "fact_generation_daily":
                previous = F.lag("generation_mwh").over(
                    Window.partitionBy("energy_source_key").orderBy("date_key"))
                mapped = (mapped.withColumn("previous_day_generation_mwh", previous.cast("float"))
                          .withColumn("generation_change_mwh", F.round(
                              F.col("generation_mwh") - F.col("previous_day_generation_mwh"), 2).cast("float"))
                          .withColumn("generation_growth_pct", F.when(
                              F.col("previous_day_generation_mwh").isNotNull() &
                              (F.col("previous_day_generation_mwh") != 0),
                              F.round(F.col("generation_change_mwh") /
                                      F.col("previous_day_generation_mwh") * 100, 2)
                          ).otherwise(None).cast("float"))
                          .withColumn("generation_fact_key", F.abs(F.hash(
                              "date_key", "energy_source_key")).cast("int")))
            else:
                previous = F.lag("capacity_mw").over(
                    Window.partitionBy("energy_source_key", "period_key").orderBy("date_key"))
                mapped = (mapped.withColumn("previous_capacity_mw", previous.cast("float"))
                          .withColumn("capacity_change_mw", F.round(
                              F.col("capacity_mw") - F.col("previous_capacity_mw"), 2).cast("float"))
                          .withColumn("capacity_growth_pct", F.when(
                              F.col("previous_capacity_mw").isNotNull() &
                              (F.col("previous_capacity_mw") != 0),
                              F.round(F.col("capacity_change_mw") /
                                      F.col("previous_capacity_mw") * 100, 2)
                          ).otherwise(None).cast("float"))
                          .withColumn("capacity_key", F.abs(F.hash(
                              "date_key", "period_key", "energy_source_key")).cast("int")))
        else:
            mapped = mapped.withColumn("mapping_version", F.lit("SOURCE_NAME_KEY_V3"))
            mapped = mapped.withColumn("gold_row_hash", stable_hash([
                "generation_mix_key", "row_hash", "mapping_version",
                "energy_source_key", "sampling_policy",
            ]))

        # This migration reads and writes the same fact, requiring a lineage break.
        staged = mapped.localCheckpoint(eager=True)
        view = f"_energy_alias_migration_{fact_name}"
        staged.createOrReplaceTempView(view)
        try:
            match = " AND ".join(
                f"target.{key} = source.{key}" if key != "energy_source_key"
                else "target.energy_source_key = source._original_energy_key"
                for key in keys
            )
            updates = ", ".join(
                f"target.{column} = source.{column}" for column in original.columns)
            spark.sql(f"MERGE INTO {table} target USING {view} source ON {match} "
                      f"WHEN MATCHED THEN UPDATE SET {updates}")
        finally:
            spark.catalog.dropTempView(view)
            staged.unpersist()
        if spark.table(table).where(F.col("energy_source_key").isin(old_keys)).limit(1).count():
            raise ValueError(f"Old energy source keys remain in {fact_name}")

    for fact_name in ("fact_generation_daily", "fact_generation_power_daily",
                      "fact_generation_power_hourly"):
        table = f"nessie.gold.{fact_name}"
        if (spark.catalog.tableExists(table) and
                spark.table(table).where(F.col("energy_source_key").isin(old_keys)).limit(1).count()):
            raise ValueError(f"Historical alias still referenced by {fact_name}")

    # Keep alias names and keys for audit; flatten older alias pointers too.
    for row in mapping:
        spark.sql(
            f"UPDATE {dim_table} SET canonical_energy_source_key = {row._new_key} "
            f"WHERE is_active = false AND canonical_energy_source_key = {row._old_key}"
        )
        spark.sql(
            f"UPDATE {dim_table} SET canonical_energy_source_key = {row._new_key}, "
            f"is_active = false, source_category = 'HISTORICAL_ALIAS' "
            f"WHERE energy_source_key = {row._old_key}"
        )
