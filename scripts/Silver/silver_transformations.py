from pyspark import pipelines as dp
from pyspark.sql import functions as F



# SILVER TITLES
@dp.table(
    name="titles",
    comment="Cleaned and validated Netflix titles"
)
@dp.expect_or_drop(
    "valid_show_id",
    "show_id IS NOT NULL AND show_id <> ''"
)
@dp.expect_or_drop(
    "valid_type",
    "type IN ('Movie', 'TV Show')"
)
@dp.expect_or_drop(
    "valid_title",
    "title IS NOT NULL AND title <> ''"
)
@dp.expect_or_drop(
    "valid_release_year",
    "release_year IS NOT NULL"
)
def titles():

    df = spark.readStream.table(
        "netflix_catalog.bronze.titles"
    )

    # Trim strings and convert blank values to NULL
    string_columns = [
        "show_id",
        "type",
        "title",
        "date_added",
        "rating",
        "duration_minutes",
        "duration_seasons",
        "description"
    ]

    for column in string_columns:
        df = df.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == "",
                None
            ).otherwise(F.trim(F.col(column)))
        )

    # Convert data types
    return (
        df
        .withColumn(
            "release_year",
            F.col("release_year").cast("int")
        )
        .withColumn(
            "date_added",
            F.to_date(
                F.col("date_added"),
                "M/d/yyyy"
            )
        )
    )



# SILVER CAST
@dp.table(
    name="cast",
    comment="Cleaned and validated Netflix cast data"
)
@dp.expect_or_drop(
    "valid_show_id",
    "show_id IS NOT NULL AND show_id <> ''"
)
@dp.expect_or_drop(
    "valid_cast_member",
    "cast IS NOT NULL AND cast <> ''"
)
def cast():

    df = spark.readStream.table(
        "netflix_catalog.bronze.cast"
    )

    return (
        df
        .withColumn("show_id", F.trim(F.col("show_id")))
        .withColumn(
            "cast_member",
            F.trim(F.col("cast"))
        )
    )



# SILVER CATEGORY
@dp.table(
    name="category",
    comment="Cleaned and validated Netflix category data"
)
@dp.expect_or_drop(
    "valid_show_id",
    "show_id IS NOT NULL AND show_id <> ''"
)
@dp.expect_or_drop(
    "valid_category",
    "category IS NOT NULL AND category <> ''"
)
def category():

    df = spark.readStream.table(
        "netflix_catalog.bronze.category"
    )

    return (
        df
        .withColumn("show_id", F.trim(F.col("show_id")))
        .withColumn(
            "category",
            F.trim(F.col("listed_in"))
        )
    )



# SILVER COUNTRIES
@dp.table(
    name="countries",
    comment="Cleaned and validated Netflix country data"
)
@dp.expect_or_drop(
    "valid_show_id",
    "show_id IS NOT NULL AND show_id <> ''"
)
@dp.expect_or_drop(
    "valid_country",
    "country IS NOT NULL AND country <> ''"
)
def countries():

    df = spark.readStream.table(
        "netflix_catalog.bronze.countries"
    )

    return (
        df
        .withColumn("show_id", F.trim(F.col("show_id")))
        .withColumn(
            "country",
            F.trim(F.col("country"))
        )
    )



# SILVER DIRECTORS
@dp.table(
    name="directors",
    comment="Cleaned and validated Netflix director data"
)
@dp.expect_or_drop(
    "valid_show_id",
    "show_id IS NOT NULL AND show_id <> ''"
)
@dp.expect_or_drop(
    "valid_director",
    "director IS NOT NULL AND director <> ''"
)
def directors():

    df = spark.readStream.table(
        "netflix_catalog.bronze.directors"
    )

    return (
        df
        .withColumn("show_id", F.trim(F.col("show_id")))
        .withColumn(
            "director",
            F.trim(F.col("director"))
        )
    )