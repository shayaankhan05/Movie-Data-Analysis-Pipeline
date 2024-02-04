import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
import concurrent.futures
import re


class GroupFilter:
    def __init__(self, name, filters):
        self.name = name
        self.filters = filters


def apply_group_filter(source_DyF, group):
    return Filter.apply(frame=source_DyF, f=group.filters)


def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {
            executor.submit(apply_group_filter, source_DyF, gf): gf
            for gf in group_filters
        }
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print("%r generated an exception: %s" % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Movies Input data from S3
MoviesInputdatafromS3_node1707040406861 = glueContext.create_dynamic_frame.from_catalog(
    database="movie-data-analysis-db",
    table_name="s3_imdb_movies_rating_csv",
    transformation_ctx="MoviesInputdatafromS3_node1707040406861",
)

# Script generated for node Data Quality Check using Ruleset
DataQualityCheckusingRuleset_node1707040449115_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
        IsComplete "imdb_rating",
        ColumnValues "imdb_rating" between 8.4 and 10.0
    ]
"""

DataQualityCheckusingRuleset_node1707040449115 = EvaluateDataQuality().process_rows(
    frame=MoviesInputdatafromS3_node1707040406861,
    ruleset=DataQualityCheckusingRuleset_node1707040449115_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "DataQualityCheckusingRuleset_node1707040449115",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

# Script generated for node ruleOutcomes
ruleOutcomes_node1707040555943 = SelectFromCollection.apply(
    dfc=DataQualityCheckusingRuleset_node1707040449115,
    key="ruleOutcomes",
    transformation_ctx="ruleOutcomes_node1707040555943",
)

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1707040610948 = SelectFromCollection.apply(
    dfc=DataQualityCheckusingRuleset_node1707040449115,
    key="rowLevelOutcomes",
    transformation_ctx="rowLevelOutcomes_node1707040610948",
)

# Script generated for node Conditional Router
ConditionalRouter_node1707040660749 = threadedRoute(
    glueContext,
    source_DyF=rowLevelOutcomes_node1707040610948,
    group_filters=[
        GroupFilter(
            name="output_group_for_failed_records",
            filters=lambda row: (
                bool(re.match("FAILED", row["DataQualityEvaluationResult"]))
            ),
        ),
        GroupFilter(
            name="default_group",
            filters=lambda row: (
                not (bool(re.match("FAILED", row["DataQualityEvaluationResult"])))
            ),
        ),
    ],
)

# Script generated for node default_group
default_group_node1707040660968 = SelectFromCollection.apply(
    dfc=ConditionalRouter_node1707040660749,
    key="default_group",
    transformation_ctx="default_group_node1707040660968",
)

# Script generated for node output_group_for_failed_records
output_group_for_failed_records_node1707040660969 = SelectFromCollection.apply(
    dfc=ConditionalRouter_node1707040660749,
    key="output_group_for_failed_records",
    transformation_ctx="output_group_for_failed_records_node1707040660969",
)

# Script generated for node Comparing schema between I/P and O/P
ComparingschemabetweenIPandOP_node1707040836202 = ApplyMapping.apply(
    frame=default_group_node1707040660968,
    mappings=[
        ("poster_link", "string", "poster_link", "string"),
        ("series_title", "string", "series_title", "string"),
        ("released_year", "string", "released_year", "string"),
        ("certificate", "string", "certificate", "string"),
        ("runtime", "string", "runtime", "string"),
        ("genre", "string", "genre", "string"),
        ("imdb_rating", "double", "imdb_rating", "double"),
        ("overview", "string", "overview", "string"),
        ("meta_score", "long", "meta_score", "long"),
        ("director", "string", "director", "string"),
        ("star1", "string", "star1", "string"),
        ("star2", "string", "star2", "string"),
        ("star3", "string", "star3", "string"),
        ("star4", "string", "star4", "string"),
        ("no_of_votes", "long", "no_of_votes", "long"),
        ("gross", "string", "gross", "string"),
        ("DataQualityRulesPass", "array", "DataQualityRulesPass", "array"),
        ("DataQualityRulesFail", "array", "DataQualityRulesFail", "array"),
        ("DataQualityRulesSkip", "array", "DataQualityRulesSkip", "array"),
        (
            "DataQualityEvaluationResult",
            "string",
            "DataQualityEvaluationResult",
            "string",
        ),
    ],
    transformation_ctx="ComparingschemabetweenIPandOP_node1707040836202",
)

# Script generated for node Rule Set Outcome to S3
RuleSetOutcometoS3_node1707040575588 = glueContext.write_dynamic_frame.from_options(
    frame=ruleOutcomes_node1707040555943,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://movie-data-analysis-proj/rule_outcome/",
        "partitionKeys": [],
    },
    transformation_ctx="RuleSetOutcometoS3_node1707040575588",
)

# Script generated for node Failed Records dumping to S3
FailedRecordsdumpingtoS3_node1707040718844 = (
    glueContext.write_dynamic_frame.from_options(
        frame=output_group_for_failed_records_node1707040660969,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://movie-data-analysis-proj/bad_records/",
            "partitionKeys": [],
        },
        transformation_ctx="FailedRecordsdumpingtoS3_node1707040718844",
    )
)

# Script generated for node Dumping Passed Data into S3
DumpingPassedDataintoS3_node1707041305643 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=ComparingschemabetweenIPandOP_node1707040836202,
        database="movie-data-analysis-db",
        table_name="redshift_dev_movies_imdb_movies_rating",
        redshift_tmp_dir="s3://movie-data-analysis-proj/temp/",
        additional_options={
            "aws_iam_role": "arn:aws:iam::339712801489:role/redshift_role_new"
        },
        transformation_ctx="DumpingPassedDataintoS3_node1707041305643",
    )
)

job.commit()
