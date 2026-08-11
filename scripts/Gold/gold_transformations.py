from pyspark import pipelines as dp
from pyspark.sql import functions as F



# GOLD - CONTENT OVERVIEW
@dp.table(
    name="content_overview",
    comment="Title-level Netflix catalog for general analytics"
)
def content_overview():

    return (
        spark.readStream.table("netflix_catalog.silver.titles")
        .select(
            "show_id",
            "title",
            "type",
            "date_added",
            "release_year",
            "rating",
            "duration_seasons",
            "duration_minutes",
            "description"
        )
    )



# GOLD - CONTENT BY COUNTRY
@dp.table(
    name="content_by_country",
    comment="Netflix titles mapped to their associated countries"
)
def content_by_country():

    titles_df = spark.readStream.table(
        "netflix_catalog.silver.titles"
    )

    countries_df = spark.read.table(
        "netflix_catalog.silver.countries"
    )

    return (
        titles_df
        .join(
            countries_df,
            on="show_id",
            how="left"
        )
        .select(
            titles_df.show_id,
            titles_df.title,
            titles_df.type,
            titles_df.release_year,
            titles_df.date_added,
            countries_df.country
        )
    )



# GOLD - CONTENT BY CATEGORY
@dp.table(
    name="content_by_category",
    comment="Netflix titles mapped to their associated categories"
)
def content_by_category():

    titles_df = spark.readStream.table(
        "netflix_catalog.silver.titles"
    )

    category_df = spark.read.table(
        "netflix_catalog.silver.category"
    )

    return (
        titles_df
        .join(
            category_df,
            on="show_id",
            how="left"
        )
        .select(
            titles_df.show_id,
            titles_df.title,
            titles_df.type,
            titles_df.release_year,
            titles_df.rating,
            category_df.category
        )
    )



# GOLD - CAST MEMBER ANALYTICS
@dp.materialized_view(
    name="top_cast_members",
    comment="Aggregated Netflix title counts by cast member"
)
def top_cast_members():

    cast_df = spark.read.table(
        "netflix_catalog.silver.cast"
    )

    titles_df = spark.read.table(
        "netflix_catalog.silver.titles"
    )

    return (
        cast_df
        .join(
            titles_df,
            on="show_id",
            how="inner"
        )
        .groupBy("cast_member")
        .agg(
            F.countDistinct("show_id").alias("total_titles"),

            F.countDistinct(
                F.when(
                    F.col("type") == "Movie",
                    F.col("show_id")
                )
            ).alias("movie_count"),

            F.countDistinct(
                F.when(
                    F.col("type") == "TV Show",
                    F.col("show_id")
                )
            ).alias("tv_show_count")
        )
    )



# GOLD - DIRECTOR ANALYTICS
@dp.materialized_view(
    name="top_directors",
    comment="Aggregated Netflix title counts by director"
)
def top_directors():

    directors_df = spark.read.table(
        "netflix_catalog.silver.directors"
    )

    titles_df = spark.read.table(
        "netflix_catalog.silver.titles"
    )

    return (
        directors_df
        .join(
            titles_df,
            on="show_id",
            how="inner"
        )
        .groupBy("director")
        .agg(
            F.countDistinct("show_id").alias("total_titles"),

            F.countDistinct(
                F.when(
                    F.col("type") == "Movie",
                    F.col("show_id")
                )
            ).alias("movie_count"),

            F.countDistinct(
                F.when(
                    F.col("type") == "TV Show",
                    F.col("show_id")
                )
            ).alias("tv_show_count")
        )
    )