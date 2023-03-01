from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def create_resources_df():
    """
       Creates a dataframe with resources configuration. It's recommended to use the same data as
       in etl/external_resources.yaml, but it's not a restriction.
   """

    schema = StructType([
        StructField('id', IntegerType(), False),
        StructField('name', StringType(), False),
        StructField('label', StringType(), False),
        StructField('type', StringType(), False),
        StructField('link_building_method', StringType(), False),
        StructField('link_template', StringType(), False)
    ])

    spark = SparkSession.builder.getOrCreate()

    data = [(1, "Civic (Genes)", "Civic", "Gene", "referenceLookup", "https://civicdb.org/links/entrez_name/ENTRY_ID"),
            (2, "Civic (Variants)", "Civic", "Variant", "referenceLookup",
             "https://civicdb.org/links?idtype=variant&id=ENTRY_ID"),
            (3, "OncoMx (Genes)", "OncoMx", "Gene", "referenceLookup", "https://oncomx.org/searchview/?gene=ENTRY_ID"),
            (4, "dbSNP (Variants)", "dbSNP", "Variant", "dbSNPInlineLink", "https://www.ncbi.nlm.nih.gov/snp/RS_ID"),
            (5, "COSMIC (Variants)", "COSMIC", "Variant", "COSMICInlineLink",
             "https://cancer.sanger.ac.uk/cosmic/mutation/overview?id=COSMIC_ID"),
            (6, "OpenCravat (Variants)", "OpenCravat", "Variant", "OpenCravatInlineLink",
             "https://run.opencravat.org/webapps/variantreport/index.html?alt_base=ALT_BASE" +
             "&chrom=chrCHROM&pos=POSITION&ref_base=REF_BASE"),
            (7, "ENA (Studies)", "ENA", "Study", "ENAInlineLink",
             "https://www.ebi.ac.uk/ena/browser/view/ENA_ID"),
            (8, "EGA (Studies)", "EGA", "Study", "EGAInlineLink",
             "https://ega-archive.org/studies/EGA_ID")
            ]
    resources_df = spark.createDataFrame(data=data, schema=schema)

    return resources_df


def create_resources_reference_data_df():
    spark = SparkSession.builder.getOrCreate()
    columns = ["entry", "type", "resource", "link"]
    data = [("NUP58", "Gene", "Civic", "https://civicdb.org/links/entrez_name/NUP58"),
            ("BRAF V600E", "Variant", "Civic", "https://civicdb.org/links?idtype=variant&id=12"),
            ("BRAF", "Gene", "OncoMx", "https://oncomx.org/searchview/?gene=BRAF"),
            ("BRAF", "Gene", "Civic", "https://civicdb.org/links/entrez_name/BRAF")]
    df_ref = spark.createDataFrame(data=data, schema=columns)

    return df_ref
