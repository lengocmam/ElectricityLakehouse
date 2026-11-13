"""Energy-source members retain provider and published labels; groups are descriptive."""

from functools import reduce

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession

from bronze.nsmo_power import EXPECTED_SOURCES
from silver.silver_utils import normalize_text
from pyspark.sql.functions import (
    Column, coalesce, col, conv, create_map, element_at, lit, lower, min, 
    regexp_replace, sha2, struct, substring, to_json, trim, upper, when,
)


DIMENSION_COLUMNS = [
    "energy_source_key", "data_provider", "source_name",
    "source_group", "reporting_basis", "row_type",
]
UNKNOWN = "Chưa rõ"

# These rules describe labels, not equivalence of measurements across providers.
# Tuple: reporting group, reporting basis, role within the published breakdown.
SOURCE_CLASSIFICATIONS = {
    "Thủy điện": ("Thủy điện", UNKNOWN, "Chi tiết"),
    "Nhiệt điện than": ("Nhiệt điện than", UNKNOWN, "Chi tiết"),
    "Gió": ("Điện gió", UNKNOWN, "Chi tiết"),
    "Điện gió": ("Điện gió", UNKNOWN, "Chi tiết"),
    "Điện mặt trời": ("Điện mặt trời", UNKNOWN, "Tổng nhóm"),
    "Mặt trời farm": ("Điện mặt trời trang trại", UNKNOWN, "Chi tiết"),
    "ĐMT trang trại": ("Điện mặt trời trang trại", UNKNOWN, "Chi tiết"),
    "ĐMT mái nhà": ("Điện mặt trời mái nhà", UNKNOWN, "Chi tiết"),
    "ĐMTMN (ĐC)": ("Điện mặt trời mái nhà", "Ước tính đầu cực", "Chi tiết"),
    "ĐMT mái nhà (ước tính đầu cực)": (
        "Điện mặt trời mái nhà", "Ước tính đầu cực", "Chi tiết",
    ),
    "ĐMT mái nhà (ước tính thương phẩm)": (
        "Điện mặt trời mái nhà", "Ước tính thương phẩm", "Chi tiết",
    ),
    "Tuabin khí": ("Tuabin khí", UNKNOWN, "Tổng nhóm"),
    "Tuabin khí (Gas + Dầu DO)": ("Tuabin khí", UNKNOWN, "Tổng nhóm"),
    "Tuabin khí (GAS)": ("Tuabin khí", UNKNOWN, "Chi tiết"),
    "Tuabin khí (DO)": ("Tuabin khí", UNKNOWN, "Chi tiết"),
    "Tuabin khí (Chạy GAS)": ("Tuabin khí", UNKNOWN, "Chi tiết"),
    "Tuabin khí (Chạy Dầu)": ("Tuabin khí", UNKNOWN, "Chi tiết"),
    "Nhiệt điện dầu": ("Nhiệt điện dầu", UNKNOWN, "Tổng nhóm"),
    "Nhiệt điện dầu DO": ("Nhiệt điện dầu", UNKNOWN, "Chi tiết"),
    "Nhiệt điện dầu FO": ("Nhiệt điện dầu", UNKNOWN, "Chi tiết"),
    "Nhiệt điện khác": ("Nhiệt điện khác", UNKNOWN, "Tổng nhóm"),
    "Sinh khối": ("Sinh khối", UNKNOWN, "Chi tiết"),
    "Nhập khẩu điện": ("Điện nhập khẩu", UNKNOWN, "Tổng nhóm"),
    "Mua TQ": ("Chưa xác định", UNKNOWN, UNKNOWN),
    "Loại khác": ("Nguồn khác", UNKNOWN, "Tổng nhóm"),
    "Toàn quốc": ("Toàn hệ thống", UNKNOWN, "Tổng hệ thống"),
    "Quốc gia": ("Toàn hệ thống", UNKNOWN, "Tổng hệ thống"),
    "Quốc gia + ĐMT mái nhà (ước tính thương phẩm)": (
        "Toàn hệ thống", "Ước tính thương phẩm", "Tổng hệ thống",
    ),
    "Quốc gia + ĐMT mái nhà (ước tính đầu cực)": (
        "Toàn hệ thống", "Ước tính đầu cực", "Tổng hệ thống",
    ),
}


def source_identity(source_name: Column) -> Column:
    """Ignore case and extra whitespace without collapsing published source labels."""
    return lower(normalize_text(source_name))


def source_key_from_name(
    source_name: Column, data_provider: str | Column,
) -> Column:
    """Hash provider and original label; grouping rules never affect this key."""
    provider = lit(data_provider) if isinstance(data_provider, str) else data_provider
    identity = to_json(struct(
        upper(trim(provider)).alias("data_provider"),
        source_identity(source_name).alias("source_name"),
    ))
    digest = sha2(identity, 256)
    return when(
        source_name.isNotNull(),
        conv(substring(digest, 1, 15), 16, 10).cast("long"),
    )


def _classification(source_name: Column) -> Column:
    """Normalize punctuation only for metadata lookup, keeping the source identity."""
    label = lower(regexp_replace(
        regexp_replace(normalize_text(source_name), r"^\*\s*", ""), r":$", "",
    ))
    pairs = []
    for name, (group, basis, row_type) in SOURCE_CLASSIFICATIONS.items():
        pairs.extend((
            lit(name.lower()),
            struct(
                lit(group).alias("source_group"),
                lit(basis).alias("reporting_basis"),
                lit(row_type).alias("row_type"),
            ),
        ))
    return when(
        label.rlike(r"^khác\s*\(.*\)$"),
        struct(
            lit("Nguồn khác").alias("source_group"),
            lit(UNKNOWN).alias("reporting_basis"),
            lit("Tổng nhóm").alias("row_type"),
        ),
    ).otherwise(element_at(create_map(*pairs), label))


def build_energy_source_dimension(names: DataFrame) -> DataFrame:
    """Build one member per provider and source label, including unclassified labels."""
    members = (
        names.withColumn("source_name", normalize_text(col("source_name")))
        .withColumn("data_provider", upper(trim(col("data_provider"))))
        .where(col("source_name").isNotNull())
        .withColumn("_identity", source_identity(col("source_name")))
        .groupBy("data_provider", "_identity")
        .agg(min("source_name").alias("source_name"))
    )
    members = (
        members.withColumn("energy_source_key", source_key_from_name(
            col("source_name"), col("data_provider"),
        ))
        .withColumn("_classification", _classification(col("source_name")))
        .withColumn("source_group", coalesce(
            col("_classification.source_group"), lit("Chưa xác định"),
        ))
        .withColumn("reporting_basis", coalesce(
            col("_classification.reporting_basis"), lit(UNKNOWN),
        ))
        .withColumn("row_type", coalesce(
            col("_classification.row_type"), lit(UNKNOWN),
        ))
        .select(*DIMENSION_COLUMNS)
    )
    if members.groupBy("energy_source_key").count().where(col("count") > 1).limit(1).count():
        raise ValueError("Energy source hash collision; choose a wider source key")
    return members


def require_energy_source_dimension(spark: SparkSession) -> None:
    """Do not mix provider-specific fact keys with the previous dimension layout."""
    table = "nessie.gold.dim_energy_source"
    if not spark.catalog.tableExists(table):
        raise ValueError("Build dim_energy_source before running generation facts")
    if spark.table(table).columns != DIMENSION_COLUMNS:
        raise ValueError(
            "Existing energy-source keys require a coordinated rebuild. "
            "Run maintenance/rebuild_energy_sources.py to preview, then --apply; "
            "original Gold tables are retained as backups."
        )


def generate_dim_energy_source(spark: SparkSession) -> DataFrame:
    """Read observed labels from Silver and seed the known NSMO chart labels."""
    names = [spark.createDataFrame(
        [("NSMO", name) for name in sorted(EXPECTED_SOURCES)],
        ["data_provider", "source_name"],
    )]
    for table, column, provider in (
        ("nessie.silver.generation", "energy_source", "EVN"),
        ("nessie.silver.power_daily", "generation_source", "EVN"),
        ("nessie.silver.power_hourly", "generation_source_name", "NSMO"),
    ):
        try:
            if spark.catalog.tableExists(table):
                names.append(
                    spark.read.option("vectorization-enabled", "false").table(table)
                    .select(
                        lit(provider).alias("data_provider"),
                        col(column).alias("source_name"),
                    ).distinct()
                )
        except AnalysisException as exc:
            missing_catalog = (
                "CATALOG_NOT_FOUND", "SCHEMA_NOT_FOUND", "REQUIRES_SINGLE_PART_NAMESPACE",
            )
            if not any(code in str(exc) for code in missing_catalog):
                raise
    return build_energy_source_dimension(reduce(
        lambda left, right: left.unionByName(right), names,
    ))
