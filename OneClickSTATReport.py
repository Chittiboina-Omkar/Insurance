#!/usr/bin/env python
# coding: utf-8

# ## OneClickSTATReport
# 
# null

# # Base query

# In[1]:


import time
strat_time = time.time()


# In[2]:


data = spark.sql("""
WITH Risk_Level_Coverages AS
(
    SELECT
        PDI.PolicyNumber,
        PDI.PolicyVersion,
        PDI.InTransactionId,
        PDI.ReferencePolicyVersion,
        PDI.PolicySource,
        PDI.BookDate,
        PDI.ProrataPercent,

        /* Risk-level → ProductId must be NULL */
        --CAST(NULL AS INT) AS ProductId,
        PI.LOBCode,

        --R.RiskItemId,
        R.RiskItemType,
        R.UnitNumber,
        R.ClassCode,

       
        PDI.PrimaryRiskState,
     

        CI.CoverageId,
  CI.ParentCoverageId,
  CI.RiskItemId,
  CI.ProductId,
  CI.CoverageCode,
        CI.CoverageExtension,
        CI.CoverageGroup,
        CI.OnsetAmount,
        CI.OffsetAmount,
        CI.Limit1,
        CI.Limit2,
        CI.ASLOB,
        CI.Option1,
        CI.Exposure,
        CI.Deductible1,
        CI.ClaimCode,
        CI.CoverageInfoGUID
    FROM ODS_STATS_TBLS_LH.policy_policy_detail_info PDI 

    JOIN ODS_STATS_TBLS_LH.policy_Product_info PI
        ON PI.InTransactionId = PDI.InTransactionId
       AND PI.LOBCode = 'AC'
       AND PI.ParentProductID IS NULL        

    JOIN ODS_STATS_TBLS_LH.policy_risk_item_info R 
        ON R.InTransactionId = PDI.InTransactionId
       AND R.RiskItemType = 'Vehicle'        
       AND R.ClassCode NOT IN ('9410','9437','9582','9235','9670')

    

    JOIN ODS_STATS_TBLS_LH.policy_coverage_info CI 
        ON CI.InTransactionId = PDI.InTransactionId
       AND CI.RiskItemId      = R.RiskItemId
       AND CI.ProductId       IS NULL        --  enforce risk-level

    WHERE (YEAR(PDI.BookDate) = 2025)
      AND (
            (CI.OnsetAmount  IS NOT NULL AND CI.OnsetAmount  <> 0)
         OR (CI.OffsetAmount IS NOT NULL AND CI.OffsetAmount <> 0)
      )
      AND PDI.TermEffectiveDate > '2023-06-30'
),
Location_Level_Coverages AS
(
    SELECT
        PDI.PolicyNumber,
        PDI.PolicyVersion,
        PDI.InTransactionId,
        PDI.ReferencePolicyVersion,
        PDI.PolicySource,
        PDI.BookDate,
        PDI.ProrataPercent,

        /* Risk-level → ProductId must be NULL */
        --CAST(NULL AS INT) AS ProductId,
        PI.LOBCode,

        --R.RiskItemId,
        R.RiskItemType,
        R.UnitNumber,
        R.ClassCode,

        
        PDI.PrimaryRiskState,
       

        CI.CoverageId,
  CI.ParentCoverageId,
  CI.RiskItemId,
  CI.ProductId,
  CI.CoverageCode,
        CI.CoverageExtension,
        CI.CoverageGroup,
        CI.OnsetAmount,
        CI.OffsetAmount,
        CI.Limit1,
        CI.Limit2,
        CI.ASLOB,
        CI.Option1,
        CI.Exposure,
        CI.Deductible1,
        CI.ClaimCode,
        CI.CoverageInfoGUID
    FROM ODS_STATS_TBLS_LH.policy_policy_detail_info PDI 

    JOIN ODS_STATS_TBLS_LH.policy_Product_info PI
        ON PI.InTransactionId = PDI.InTransactionId
       AND PI.LOBCode = 'AC'
       AND PI.ParentProductID IS NULL        

    JOIN ODS_STATS_TBLS_LH.policy_risk_item_info R 
        ON R.InTransactionId = PDI.InTransactionId
       AND R.RiskItemType = 'Location'        
       --AND R.ClassCode NOT IN ('9410','9437','9582')

    

    JOIN ODS_STATS_TBLS_LH.policy_coverage_info CI 
        ON CI.InTransactionId = PDI.InTransactionId
       AND CI.RiskItemId      = R.RiskItemId
       AND CI.ProductId       IS NULL        --  enforce risk-level

    WHERE (YEAR(PDI.BookDate) = 2025)
      AND (
            (CI.OnsetAmount  IS NOT NULL AND CI.OnsetAmount  <> 0)
        OR (CI.OffsetAmount IS NOT NULL AND CI.OffsetAmount <> 0)
      )
      AND PDI.TermEffectiveDate > '2023-06-30'
),
Policy_Level_Coverages AS
(
    SELECT
        PDI.PolicyNumber,
        PDI.PolicyVersion,
        PDI.InTransactionId,
        PDI.ReferencePolicyVersion,
        PDI.PolicySource,
        PDI.BookDate,
        PDI.ProrataPercent,

        --PI.ProductId,
        PI.LOBCode,

        /* Product-level → Risk fields must be NULL */
        --CAST(NULL AS BIGINT)      AS RiskItemId,
        CAST(NULL AS VARCHAR(50)) AS RiskItemType,
        CAST(NULL AS INT)         AS UnitNumber,
        CAST(NULL AS VARCHAR(20)) AS ClassCode,

       
        PDI.PrimaryRiskState,
      

        CI.CoverageId,
  CI.ParentCoverageId,
  CI.RiskItemId,
  CI.ProductId,
  CI.CoverageCode,
        CI.CoverageExtension,
        CI.CoverageGroup,
        CI.OnsetAmount,
        CI.OffsetAmount,
        CI.Limit1,
        CI.Limit2,
        CI.ASLOB,
        CI.Option1,
        CI.Exposure,
        CI.Deductible1,
        CI.ClaimCode,
        CI.CoverageInfoGUID
    FROM ODS_STATS_TBLS_LH.policy_policy_detail_info PDI 

    JOIN ODS_STATS_TBLS_LH.policy_Product_info PI
        ON PI.InTransactionId = PDI.InTransactionId
       AND PI.LOBCode = 'AC'
       AND PI.ParentProductID IS NULL        

    JOIN ODS_STATS_TBLS_LH.policy_coverage_info CI
        ON CI.InTransactionId = PDI.InTransactionId
       AND CI.ProductId       = PI.ProductId
       AND CI.RiskItemId      IS NULL        -- enforce product-level

    

    WHERE (YEAR(PDI.BookDate) = 2025)
      AND (
            (CI.OnsetAmount  IS NOT NULL AND CI.OnsetAmount  <> 0)
         OR (CI.OffsetAmount IS NOT NULL AND CI.OffsetAmount <> 0)
      )
      AND PDI.TermEffectiveDate > '2023-06-30'
),
Prem_On_Child_Coverages AS
(
    SELECT
        PDI.PolicyNumber,
        PDI.PolicyVersion,
        PDI.InTransactionId,
        PDI.ReferencePolicyVersion,
        PDI.PolicySource,
        PDI.BookDate,
        PDI.ProrataPercent,

        --PI.ProductId,
        PI.LOBCode,

        /* Product-level → Risk fields must be NULL */
        --CAST(NULL AS BIGINT)      AS RiskItemId,
        CAST(NULL AS VARCHAR(50)) AS RiskItemType,
        CAST(NULL AS INT)         AS UnitNumber,
        CAST(NULL AS VARCHAR(20)) AS ClassCode,

       
        PDI.PrimaryRiskState,
      

        CI.CoverageId,
  CI.ParentCoverageId,
  CI.RiskItemId,
  CI.ProductId,
  CI.CoverageCode,
        CI.CoverageExtension,
        CI.CoverageGroup,
        CI.OnsetAmount,
        CI.OffsetAmount,
        CI.Limit1,
        CI.Limit2,
        CI.ASLOB,
        CI.Option1,
        CI.Exposure,
        CI.Deductible1,
        CI.ClaimCode,
        CI.CoverageInfoGUID
    FROM ODS_STATS_TBLS_LH.policy_policy_detail_info PDI 

    JOIN ODS_STATS_TBLS_LH.policy_Product_info PI
        ON PI.InTransactionId = PDI.InTransactionId
       AND PI.LOBCode = 'AC'
       AND PI.ParentProductID IS NULL        

    JOIN ODS_STATS_TBLS_LH.policy_coverage_info CI
        ON CI.InTransactionId = PDI.InTransactionId
       AND CI.ProductId       IS NULL
       AND CI.RiskItemId      IS NULL       

    

    WHERE (YEAR(PDI.BookDate) = 2025)
      AND (
            (CI.OnsetAmount  IS NOT NULL AND CI.OnsetAmount  <> 0)
         OR (CI.OffsetAmount IS NOT NULL AND CI.OffsetAmount <> 0)
      )
      AND PDI.TermEffectiveDate > '2023-06-30'
)
SELECT DISTINCT *
FROM (
    SELECT * FROM Risk_Level_Coverages
 UNION ALL
 SELECT * FROM Location_Level_Coverages
    UNION ALL
    SELECT * FROM Policy_Level_Coverages
 UNION ALL
 SELECT * FROM Prem_On_Child_Coverages
) U
ORDER BY PolicyNumber, PolicyVersion, RiskItemId, CoverageCode
""")
 


# In[3]:


from pyspark.sql import functions as F

# List of (PolicyNumber, PolicyVersion) pairs you want to delete
delete_pairs = [
    ('GMCA100000107', 1449),
    ('GMCA100000080', 4740)
    # Add more pairs here
]

# Convert list of tuples into a DataFrame for proper anti-join
delete_df = spark.createDataFrame(delete_pairs, ["PolicyNumber", "PolicyVersion"])

# Delete all matching rows by performing an anti join
data = data.join(
    delete_df,
    on=["PolicyNumber", "PolicyVersion"],
    how="left_anti"        # this REMOVES matching rows
)
#check=data[(data['PolicyNumber']=='GMCA100000107')&(data['PolicyVersion']==1449)]
#display(check)


# In[4]:


DeleteDuplicateCoveragesFromSTATExtract = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/DeleteDuplicateCoveragesFromSTATExtract.csv")


# In[5]:


# delete duplicate records

join_cols = [
    "PolicyNumber",
    "PolicyVersion",
    "InTransactionId",
    "RiskItemId",
    "CoverageId"
]

data = data.join(
    DeleteDuplicateCoveragesFromSTATExtract,
    on=join_cols,
    how="left_anti"
)


# # Coverage Code Based amount merging

# In[6]:


from pyspark.sql import functions as F

df = data

pairs = [
    ("BODINJLIAB", "BI"),
    ("COLLISION", "COLL"),
    ("OTC", "COMP")
]

result = df

for primary, secondary in pairs:

    # Step 1: Find groups where BOTH primary and secondary exist
    both = (
        result.groupBy("RiskItemId", "InTransactionId")
        .agg(
            F.sum(F.when(F.col("CoverageCode") == primary, 1).otherwise(0)).alias("p_cnt"),
            F.sum(F.when(F.col("CoverageCode") == secondary, 1).otherwise(0)).alias("s_cnt"),
            F.sum(F.when(F.col("CoverageCode") == secondary, F.col("OnsetAmount")).otherwise(0)).alias("sec_onset_sum"),
            F.sum(F.when(F.col("CoverageCode") == secondary, F.col("OffsetAmount")).otherwise(0)).alias("sec_offset_sum")
        )
        .filter((F.col("p_cnt") > 0) & (F.col("s_cnt") > 0))
        .select("RiskItemId", "InTransactionId", "sec_onset_sum", "sec_offset_sum")
    )

    # Step 2: Join only these valid merge-groups
    joined = result.join(both, ["RiskItemId", "InTransactionId"], "left")

    # Step 3: Update primary rows ONLY when both exist
    updated_primary = (
        joined.filter(F.col("CoverageCode") == primary)
        .withColumn(
            "OnsetAmount",
            F.when(F.col("sec_onset_sum").isNotNull(),
                   F.col("OnsetAmount") + F.col("sec_onset_sum"))
             .otherwise(F.col("OnsetAmount"))
        )
        .withColumn(
            "OffsetAmount",
            F.when(F.col("sec_offset_sum").isNotNull(),
                   F.col("OffsetAmount") + F.col("sec_offset_sum"))
             .otherwise(F.col("OffsetAmount"))
        )
        .drop("sec_onset_sum", "sec_offset_sum")
    )

    # Step 4: Keep all rows EXCEPT secondary rows that were merged
    remaining = (
        joined
        .filter(~(
            (F.col("CoverageCode") == secondary) &
            (F.col("sec_onset_sum").isNotNull())
        ))
        .drop("sec_onset_sum", "sec_offset_sum")
    )

    # Step 5: Replace original primary rows with updated primary rows
    result = (
        remaining.filter(F.col("CoverageCode") != primary)
        .unionByName(updated_primary)
    )

final_merged_df = result


# In[7]:


#ProrataPercentage for Origami policies 
from pyspark.sql.functions import col, when
from pyspark.sql import functions as F

# -------------------------------
# 1. Prepare lookup (QA table)
# -------------------------------
qa_df = (
    spark.table("ODS_STATS_TBLS_LH.policy_question_answer")
    .filter(col("QuestionCode") == "ProrataPercentage")
    .select(
        col("InTransactionId").alias("QA_InTransactionId"),
        col("TextValue").alias("Prorata_from_QA")
    )
    #  Ensure NO DUPLICATES (critical to avoid row explosion)
    .dropDuplicates(["QA_InTransactionId"])
)

# -------------------------------
# 2. LEFT JOIN (strict row preservation)
# -------------------------------
joined_df = final_merged_df.join(
    qa_df,
    data["InTransactionId"] == qa_df["QA_InTransactionId"],
    "left"
)

# -------------------------------
# 3. Conditional Update
# -------------------------------
final_df = joined_df.withColumn(
    "ProrataPercent",
    when(
        col("PolicySource") == "Origami",
        col("Prorata_from_QA")
    ).otherwise(col("ProrataPercent"))
)

# -------------------------------
# 4. Cleanup (remove extra columns)
# -------------------------------
final_merged_df = final_df.drop("QA_InTransactionId", "Prorata_from_QA")

# -------------------------------
#(for your validation)
# -------------------------------
#print("Base Count  :", final_merged_df.count())
#print("Final Count :", final_df.count())


# # Offset Reference Records Pulling logic

# 2nd try on offset using by splitting the data

# In[8]:


from pyspark.sql.functions import col, lit
from pyspark import StorageLevel

# =====================================================
# 1. OFFSET EXTRACTION (ADD _org COLUMNS)
# =====================================================
df_offset = final_merged_df \
    .filter(col("OffsetAmount").isNotNull() & (col("OffsetAmount") != 0)) \
    .withColumn("OnsetAmount", lit(0)) \
    .withColumn("PolicyVersion_org", col("PolicyVersion")) \
    .withColumn("InTransactionId_org", col("InTransactionId")) \
    .withColumn("ReferencePolicyVersion_org", col("ReferencePolicyVersion"))

df_offset.createOrReplaceTempView("df_offset")

# =====================================================
# 2. REFERENCE TRANSACTION
# =====================================================
ref_tx_df = spark.sql("""
SELECT
    o.*,
    pdi.InTransactionId AS Ref_InTransactionId,
    pdi.ReferencePolicyVersion AS Ref_ReferencePolicyVersion
FROM df_offset o
LEFT JOIN ODS_STATS_TBLS_LH.policy_policy_detail_info pdi
    ON pdi.PolicyNumber = o.PolicyNumber
   AND pdi.PolicyVersion = o.ReferencePolicyVersion
""")

ref_tx_df.createOrReplaceTempView("ref_tx_df")

# =====================================================
# 3. SPLIT NORMAL / SPECIAL
# =====================================================
normal_df = spark.sql("""
SELECT *
FROM ref_tx_df
WHERE UnitNumber IS NOT NULL
  AND RiskItemId IS NOT NULL
""")

special_df = spark.sql("""
SELECT *
FROM ref_tx_df
WHERE UnitNumber IS NULL
   OR RiskItemId IS NULL
""")

normal_df.createOrReplaceTempView("normal_df")
special_df.createOrReplaceTempView("special_df")

# =====================================================
# 4. NORMAL FLOW
# =====================================================
riskitem_filtered = spark.sql("""
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY InTransactionId, UnitNumber, RiskItemType
               ORDER BY RiskItemId
           ) rn
    FROM ODS_STATS_TBLS_LH.policy_risk_item_info
) WHERE rn = 1
""")

riskitem_filtered.createOrReplaceTempView("riskitem_filtered")

normal_risk_df = spark.sql("""
SELECT
    n.*,
    r.RiskItemId   AS Ref_RiskItemId,
    r.ProductId    AS Ref_ProductId,
    r.ClassCode    AS Ref_ClassCode,
    r.RiskItemType AS Ref_RiskItemType
FROM normal_df n
LEFT JOIN riskitem_filtered r
  ON r.InTransactionId = n.Ref_InTransactionId
 AND r.UnitNumber = n.UnitNumber
 AND r.RiskItemType = n.RiskItemType
""")

normal_risk_df.createOrReplaceTempView("normal_risk_df")

coverage_filtered_risk = spark.sql("""
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY InTransactionId, RiskItemId, CoverageCode
               ORDER BY ParentCoverageId, CoverageId
           ) rn
    FROM ODS_STATS_TBLS_LH.policy_coverage_info
) WHERE rn = 1
""")

coverage_filtered_risk.createOrReplaceTempView("coverage_filtered_risk")

normal_joined = spark.sql("""
SELECT
    n.*,
    c.CoverageId AS Ref_CoverageId,
    c.ParentCoverageId AS Ref_ParentCoverageId,
    c.CoverageExtension AS Ref_CoverageExtension,
    c.CoverageGroup AS Ref_CoverageGroup,
    c.Limit1 AS Ref_Limit1,
    c.Limit2 AS Ref_Limit2,
    c.ASLOB AS Ref_ASLOB,
    c.Option1 AS Option1_ref,
    c.Exposure AS Ref_Exposure,
    c.Deductible1 AS Ref_Deductible1,
    c.ClaimCode AS Ref_ClaimCode,
    c.CoverageInfoGUID AS Ref_CoverageInfoGUID
FROM normal_risk_df n
LEFT JOIN coverage_filtered_risk c
  ON c.InTransactionId = n.Ref_InTransactionId
 AND c.RiskItemId = n.Ref_RiskItemId
 AND c.CoverageCode = n.CoverageCode
""")

normal_joined.createOrReplaceTempView("normal_joined")

# =====================================================
# 5. SPECIAL FLOW
# =====================================================
coverage_filtered_prod = spark.sql("""
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY InTransactionId, ProductId, CoverageCode, CoverageExtension, ClaimCode
               ORDER BY ParentCoverageId, CoverageId
           ) rn
    FROM ODS_STATS_TBLS_LH.policy_coverage_info
) WHERE rn = 1
""")

coverage_filtered_prod.createOrReplaceTempView("coverage_filtered_prod")

special_non_origami = spark.sql("""
SELECT * FROM special_df WHERE PolicySource <> 'Origami'
""")

special_origami = spark.sql("""
SELECT * FROM special_df WHERE PolicySource = 'Origami'
""")

special_non_origami.createOrReplaceTempView("special_non_origami")
special_origami.createOrReplaceTempView("special_origami")

# Non-Origami (ProductId based)
special_non_origami_joined = spark.sql("""
SELECT
    s.*,
    NULL AS Ref_RiskItemId,
    NULL AS Ref_ClassCode,
    NULL AS Ref_RiskItemType,
    s.ProductId AS Ref_ProductId,
    c.CoverageId AS Ref_CoverageId,
    c.ParentCoverageId AS Ref_ParentCoverageId,
    c.CoverageExtension AS Ref_CoverageExtension,
    c.CoverageGroup AS Ref_CoverageGroup,
    c.Limit1 AS Ref_Limit1,
    c.Limit2 AS Ref_Limit2,
    c.ASLOB AS Ref_ASLOB,
    c.Option1 AS Option1_ref,
    c.Exposure AS Ref_Exposure,
    c.Deductible1 AS Ref_Deductible1,
    c.ClaimCode AS Ref_ClaimCode,
    c.CoverageInfoGUID AS Ref_CoverageInfoGUID
FROM special_non_origami s
LEFT JOIN coverage_filtered_prod c
  ON c.InTransactionId = s.Ref_InTransactionId
 AND c.ProductId = s.ProductId
 AND c.CoverageCode = s.CoverageCode
""")

special_non_origami_joined.createOrReplaceTempView("special_non_origami_joined")

# Origami (ClaimCode based logic)
special_origami_joined = spark.sql("""
SELECT
    so.*,
    NULL AS Ref_RiskItemId,
    NULL AS Ref_ClassCode,
    NULL AS Ref_RiskItemType,
    so.ProductId AS Ref_ProductId,
    c.CoverageId AS Ref_CoverageId,
    c.ParentCoverageId AS Ref_ParentCoverageId,
    c.CoverageExtension AS Ref_CoverageExtension,
    c.CoverageGroup AS Ref_CoverageGroup,
    c.Limit1 AS Ref_Limit1,
    c.Limit2 AS Ref_Limit2,
    c.ASLOB AS Ref_ASLOB,
    c.Option1 AS Option1_ref,
    c.Exposure AS Ref_Exposure,
    c.Deductible1 AS Ref_Deductible1,
    c.ClaimCode AS Ref_ClaimCode,
    c.CoverageInfoGUID AS Ref_CoverageInfoGUID
FROM special_origami so
LEFT JOIN coverage_filtered_prod c
  ON c.InTransactionId = so.Ref_InTransactionId
 AND c.ClaimCode = so.ClaimCode
""")

special_origami_joined.createOrReplaceTempView("special_origami_joined")

# Combine SPECIAL
special_joined = special_non_origami_joined.unionByName(special_origami_joined)
special_joined.createOrReplaceTempView("special_joined")

# =====================================================
# 6. FINAL SELECT (ALL FIXES APPLIED)
# =====================================================

# NORMAL → Risk level → ProductId = NULL
normal_final = spark.sql("""
SELECT
    PolicyNumber,
    PolicyVersion_org AS PolicyVersion,
    Ref_InTransactionId AS InTransactionId,
    ReferencePolicyVersion_org AS ReferencePolicyVersion,
    BookDate,
    ProrataPercent,
    PolicySource,
    LOBCode,
    Ref_RiskItemType AS RiskItemType,
    UnitNumber,
    Ref_ClassCode AS ClassCode,
    PrimaryRiskState,
    Ref_CoverageId AS CoverageId,
    Ref_ParentCoverageId AS ParentCoverageId,
    Ref_RiskItemId AS RiskItemId,
    NULL AS ProductId,
    CoverageCode,
    Ref_CoverageExtension AS CoverageExtension,
    Ref_CoverageGroup AS CoverageGroup,
    CAST(0 AS DECIMAL(18,2)) AS OnsetAmount,
    OffsetAmount,
    Ref_Limit1 AS Limit1,
    Ref_Limit2 AS Limit2,
    Ref_ASLOB AS ASLOB,
    Option1_ref AS Option1,
    Ref_Exposure AS Exposure,
    Ref_Deductible1 AS Deductible1,
    Ref_ClaimCode AS ClaimCode,
    Ref_CoverageInfoGUID AS CoverageInfoGUID
FROM normal_joined
""")

# SPECIAL → Product level → RiskItemId = NULL
special_final = spark.sql("""
SELECT
    PolicyNumber,
    PolicyVersion_org AS PolicyVersion,
    Ref_InTransactionId AS InTransactionId,
    ReferencePolicyVersion_org AS ReferencePolicyVersion,
    BookDate,
    ProrataPercent,
    PolicySource,
    LOBCode,
    RiskItemType,
    UnitNumber,
    NULL AS ClassCode,
    PrimaryRiskState,
    Ref_CoverageId AS CoverageId,
    Ref_ParentCoverageId AS ParentCoverageId,
    NULL AS RiskItemId,
    Ref_ProductId AS ProductId,
    CoverageCode,
    Ref_CoverageExtension AS CoverageExtension,
    Ref_CoverageGroup AS CoverageGroup,
    CAST(0 AS DECIMAL(18,2)) AS OnsetAmount,
    OffsetAmount,
    Ref_Limit1 AS Limit1,
    Ref_Limit2 AS Limit2,
    Ref_ASLOB AS ASLOB,
    Option1_ref AS Option1,
    Ref_Exposure AS Exposure,
    Ref_Deductible1 AS Deductible1,
    Ref_ClaimCode AS ClaimCode,
    Ref_CoverageInfoGUID AS CoverageInfoGUID
FROM special_joined
""")

# =====================================================
# FINAL OUTPUT
# =====================================================
final_offset_ref_df = normal_final.unionByName(special_final)


# In[9]:


from pyspark.sql.functions import col
from pyspark import StorageLevel

# =====================================================
# STEP 1: Target only affected records (NOW RESTRICTED)
# =====================================================
target_df = final_offset_ref_df.filter(
    (col("PolicyNumber") == "GMCA001142870") &   
    (col("CoverageCode") == "BROAD PIP") &
    (col("ClaimCode") == "BROAD PIP__NF")
)

# =====================================================
# STEP 2: Keep only child records
# =====================================================
target_filtered = target_df.filter(
    col("ParentCoverageId").isNotNull()
)

# =====================================================
# STEP 3: Keep everything else untouched
# =====================================================
rest_df = final_offset_ref_df.filter(
    ~(
        (col("PolicyNumber") == "GMCA001142870") &  
        (col("CoverageCode") == "BROAD PIP") &
        (col("ClaimCode") == "BROAD PIP__NF")
    )
)

# =====================================================
# STEP 4: Final combine
# =====================================================
# rest_df= rest_df.persist(StorageLevel.MEMORY_AND_DISK)
# target_filtered=target_filtered.persist(StorageLevel.MEMORY_AND_DISK)
# rest_df.count()
# target_filtered.count()

final_offset_ref_df_fixed = (rest_df.unionByName(target_filtered))


# In[10]:


from pyspark.sql.functions import col, lit

df_onset = (
    final_merged_df
    .filter(col("OnsetAmount").isNotNull() & (col("OnsetAmount") != 0))
    .withColumn("OffsetAmount", lit(0))
)

df_onset.createOrReplaceTempView("df_onset")

##############################################################
final_data_Including_offsetOnset = (df_onset.unionByName(
    final_offset_ref_df_fixed
))
#df=final_data_Including_offsetOnset
##############################################################
#spark.sql("DROP TABLE IF EXISTS OnSet_And_Refrence_Offset")
final_data_Including_offsetOnset.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("OnSet_And_Refrence_Offset")


# # Claim Data

# In[11]:


Liability_df=spark.sql("""
WITH CoverageMapping AS (
    SELECT 'ACC DEATH'    AS ClaimCoverageName, 'NF'  AS CoverageExtension, 'ACC DEATH_NF' AS DerivedClaimCoverageName
UNION ALL SELECT   'BA PLUS',   'CM',  'BA PLUS_TE'
UNION ALL SELECT   'BA PLUS',   'RR',  'BA PLUS_RR'
UNION ALL SELECT   'BA PLUS',   'TL',  'BA PLUS_T-L'
UNION ALL SELECT   'BA PLUS_RR',  'CM',  'BA PLUS_RR'
UNION ALL SELECT   'BA PLUS_T-L',       'CM',  'BA PLUS_T-L'
UNION ALL SELECT   'BI',    'BI',  'BODINJLIAB'
UNION ALL SELECT   'BI',    'CBI', 'BODINJLIAB'
UNION ALL SELECT   'BODINJLIAB',  'BI',  'BODINJLIAB'
UNION ALL SELECT   'BODINJLIAB',  'CBI',  'BODINJLIAB'
UNION ALL SELECT   'COLL',    'CL',  'COLLISION'
UNION ALL SELECT   'COLLISION',   'CL',  'COLLISION'
UNION ALL SELECT   'COMB-FPB',   'FPB',  'COMB-FPB'
UNION ALL SELECT   'COMP',    'CM',  'OTC'
UNION ALL SELECT   'FRMTRKPOLL',  'PD',  'FRMTRKPOLL_PRPDMGLIAB'
UNION ALL SELECT   'GK COLL',   'CL',  'GK COLL'
UNION ALL SELECT   'GK COMP',   'CM',  'GK COMP'
UNION ALL SELECT   'PIP LIMIT_INC LOSS', 'NF',  'PIP LIMIT_INC LOSS'
UNION ALL SELECT   'PIP LIMIT_ACC DEATH', 'NF',  'PIP LIMIT_ACC DEATH'
UNION ALL SELECT   'PIP LIMIT_FUN EXP', 'NF',  'PIP LIMIT_FUN EXP'
UNION ALL SELECT   'HIRED AUTO',  'PD',  'HIRED AUTO_PRPDMGLIAB'
UNION ALL SELECT   'HIRED PHYS',  'CM',  'HIRED PHYS_OTC'
UNION ALL SELECT   'INC LOSS',   'NF',  'INC LOSS'
UNION ALL SELECT   'MED EXP',   'FPB',  'MED EXP'
UNION ALL SELECT   'MED EXP',   'NF',  'MED EXP'
UNION ALL SELECT   'MEDPAYMENT',  'MP',  'MEDPAYMENT'
UNION ALL SELECT   'MP',    'MP',  'MEDPAYMENT'
UNION ALL SELECT   'OTC',    'CM',  'OTC'
UNION ALL SELECT   'PD',    'CPD',  'PRPDMGLIAB'
UNION ALL SELECT   'PD',    'PD',  'PRPDMGLIAB'
UNION ALL SELECT   'PIP LIMIT',   'NF',  'PIP LIMIT'
UNION ALL SELECT   'PIP LIMIT_MED EXP', 'NF',  'PIP LIMIT_MED EXP'
UNION ALL SELECT   'PRPDMGLIAB',  'CPD',  'PRPDMGLIAB'
UNION ALL SELECT   'PRPDMGLIAB',  'PD',  'PRPDMGLIAB'
UNION ALL SELECT   'RR',    'CM',  'RR'
UNION ALL SELECT   'RR',    'RR',  'RR'
UNION ALL SELECT   'T&L',    'TL',  'T-L'
UNION ALL SELECT   'T-L',    'CM',  'T-L'
UNION ALL SELECT   'UBI',    'UBI',  'UBI'
UNION ALL SELECT   'UBI NS',   'UBI',  'UBI'
UNION ALL SELECT   'UIM NS',   'UBI',  'UIM'
UNION ALL SELECT   'UIM ST',   'UBI',  'UBI'
UNION ALL SELECT   'UPD',    'UPD',  'UPD'

)

SELECT 	
    a.PolicyNumber,	
    a.TransactionNumber,	
    a.ClaimNumber,	
    a.ClaimantNumber,	
    REPLACE(TRIM(a.ItemNumber), '.0', '') as ItemNumber,	
    a.ClaimCoverageName,	
    a.CoverageExtension,
    LossDate,
	Year(a.LossDate) as AccidentYear,
	m.DerivedClaimCoverageName,
	Cast(0 as decimal(2)) as PaidCount,
	Cast(0 as decimal(2)) as ReserveCount,

	
	
	
    -- Payment amount for the selected year	
    SUM(	
        CASE 	
            WHEN BeginningOfMonth BETWEEN '2025-01-01' AND '2025-12-01' --AND '2026-03-31'	
            THEN PaymentAmount	
            ELSE 0	
        END	
    ) AS PaymentAmountDuration,	

-- Payment amount as of last year	
    SUM(	
        CASE 	
            WHEN BeginningOfMonth < '2025-01-01'	
            THEN PaymentAmount	
            ELSE 0	
        END	
    ) AS PaymentAmountDurationAsOfLastYear,
	 
	 -- Lifetime payment as of end of March 2026 (no BeginningOfMonth filter)	
    SUM(	
        CASE 	
            WHEN BeginningOfMonth < '2026-01-01' --'2026-04-01'	
            THEN PaymentAmount	
            ELSE 0	
        END	
    ) AS PaymentAmountDurationAsOfEndOf2025,
	
	-- LossIncurredNOIBNR for the selected year
SUM(	
        CASE 	
            WHEN BeginningOfMonth BETWEEN '2025-01-01' AND '2026-03-31'	
            THEN LossIncurredNoIBNR	
            ELSE 0	
        END	
    ) AS LossIncurredNOIBNRDuration,	
	
	
	
   

	-- Loss Incurred NO IBNR as of 2024
	SUM(	
        CASE 	
            WHEN BeginningOfMonth < '2025-01-01'
            THEN LossIncurredNoIBNR	
            ELSE 0	
        END	
    ) AS LossIncurredNOIBNRAsOfLastYear,


	
	-- Loss Incurred NO IBNR as of end of March 2026
	SUM(	
        CASE 	
            WHEN BeginningOfMonth < '2026-04-01'
            THEN LossIncurredNoIBNR	
            ELSE 0	
        END	
    ) AS LossIncurredNOIBNRAsOfEndOf2025,

SUM(	
        CASE 	
            WHEN BeginningOfMonth = '2026-03-01'
            THEN NetReserveAmount + ExcessDirectAmount
            ELSE 0	 
        END	
    ) AS FinalReserveAmount
	
FROM DMAnalytics.FactClaimsMonthlyDataStore	a
/* coverage mapping join */
LEFT JOIN CoverageMapping m
    ON a.ClaimCoverageName = m.ClaimCoverageName
   AND a.CoverageExtension = m.CoverageExtension
	
WHERE CompanyCode = '14044'	
  AND EASLobCode in ('193', '194')
  and Year(LossDate) < 2026
	
GROUP BY 	
    a.PolicyNumber,	
    a.TransactionNumber,	
    a.ClaimNumber,	
    a.ClaimantNumber,	
    a.ItemNumber,	
    a.ClaimCoverageName,	
    a.CoverageExtension,
	m.DerivedClaimCoverageName,
    a.LossDate,
    a.PropertyOrLiability;	
 	

""")


# In[12]:


Amount_correction_df=spark.sql("""
	WITH CoverageMapping AS (
		SELECT 'ACC DEATH'    AS ClaimCoverageName, 'NF'  AS CoverageExtension, 'ACC DEATH_NF' AS DerivedClaimCoverageName
	UNION ALL SELECT   'BA PLUS',   'CM',  'BA PLUS_TE'
	UNION ALL SELECT   'BA PLUS',   'RR',  'BA PLUS_RR'
	UNION ALL SELECT   'BA PLUS',   'TL',  'BA PLUS_T-L'
	UNION ALL SELECT   'BA PLUS_RR',  'CM',  'BA PLUS_RR'
	UNION ALL SELECT   'BA PLUS_T-L',       'CM',  'BA PLUS_T-L'
	UNION ALL SELECT   'BI',    'BI',  'BODINJLIAB'
	UNION ALL SELECT   'BI',    'CBI', 'BODINJLIAB'
	UNION ALL SELECT   'BODINJLIAB',  'BI',  'BODINJLIAB'
	UNION ALL SELECT   'BODINJLIAB',  'CBI',  'BODINJLIAB'
	UNION ALL SELECT   'COLL',    'CL',  'COLLISION'
	UNION ALL SELECT   'COLLISION',   'CL',  'COLLISION'
	UNION ALL SELECT   'COMB-FPB',   'FPB',  'COMB-FPB'
	UNION ALL SELECT   'COMP',    'CM',  'OTC'
	UNION ALL SELECT   'FRMTRKPOLL',  'PD',  'FRMTRKPOLL_PRPDMGLIAB'
	UNION ALL SELECT   'GK COLL',   'CL',  'GK COLL'
	UNION ALL SELECT   'GK COMP',   'CM',  'GK COMP'
	UNION ALL SELECT   'HIRED AUTO',  'PD',  'HIRED AUTO_PRPDMGLIAB'
	UNION ALL SELECT   'HIRED PHYS',  'CM',  'HIRED PHYS_OTC'
	UNION ALL SELECT   'INC LOSS',   'NF',  'INC LOSS'
	UNION ALL SELECT   'MED EXP',   'FPB',  'MED EXP'
	UNION ALL SELECT   'MED EXP',   'NF',  'MED EXP'
	UNION ALL SELECT   'MEDPAYMENT',  'MP',  'MEDPAYMENT'
	UNION ALL SELECT   'MP',    'MP',  'MEDPAYMENT'
	UNION ALL SELECT   'OTC',    'CM',  'OTC'
	UNION ALL SELECT   'PD',    'CPD',  'PRPDMGLIAB'
	UNION ALL SELECT   'PD',    'PD',  'PRPDMGLIAB'
	UNION ALL SELECT   'PIP LIMIT',   'NF',  'PIP LIMIT'
	UNION ALL SELECT   'PIP LIMIT_MED EXP', 'NF',  'PIP LIMIT_MED EXP'
	UNION ALL SELECT   'PIP LIMIT_INC LOSS', 'NF',  'PIP LIMIT_INC LOSS'
	UNION ALL SELECT   'PIP LIMIT_ACC DEATH', 'NF',  'PIP LIMIT_ACC DEATH'
	UNION ALL SELECT   'PIP LIMIT_FUN EXP', 'NF',  'PIP LIMIT_FUN EXP'
	UNION ALL SELECT   'PRPDMGLIAB',  'CPD',  'PRPDMGLIAB'
	UNION ALL SELECT   'PRPDMGLIAB',  'PD',  'PRPDMGLIAB'
	UNION ALL SELECT   'RR',    'CM',  'RR'
	UNION ALL SELECT   'RR',    'RR',  'BA PLUS_RR'
	UNION ALL SELECT   'T&L',    'TL',  'BA PLUS_T-L'
	UNION ALL SELECT   'T-L',    'CM',  'T-L'
	UNION ALL SELECT   'UBI',    'UBI',  'UBI'
	UNION ALL SELECT   'UBI NS',   'UBI',  'UBI'
	UNION ALL SELECT   'UIM NS',   'UBI',  'UIM'
	UNION ALL SELECT   'UIM ST',   'UBI',  'UBI'
	UNION ALL SELECT   'UPD',    'UPD',  'UPD'

	)

	SELECT 	
		a.EASLobCode,
		a.PolicyNumber,	
		a.TransactionNumber,	
		a.ClaimNumber,	
		a.ClaimantNumber,	
		REPLACE(TRIM(a.ItemNumber), '.0', '') as ItemNumber,	
		a.ClaimCoverageName,	
		a.CoverageExtension,
		Year(a.LossDate) as AccidentYear,
		m.DerivedClaimCoverageName,
		sum(a.PaymentAmount + a.AllocatedLossAdjExp) as PaidAmountALAE2026
		
		
		
	FROM FactClaimsMonthlyDataStore	a
	/* coverage mapping join */
	LEFT JOIN CoverageMapping m
		ON a.ClaimCoverageName = m.ClaimCoverageName
	   AND a.CoverageExtension = m.CoverageExtension
		
	WHERE CompanyCode = '14044'	
	  AND EASLobCode in ('193', '194')
	  and Year(LossDate) < 2026
	  and BeginningOfMonth BETWEEN '2026-01-01' AND '2026-03-01'
	  
		
	GROUP BY 	
		a.EASLobCode,
		a.PolicyNumber,	
		a.TransactionNumber,	
		a.ClaimNumber,	
		a.ClaimantNumber,	
		a.ItemNumber,	
		a.ClaimCoverageName,	
		a.CoverageExtension,
		m.DerivedClaimCoverageName,
		a.LossDate,
		a.PropertyOrLiability
	Having sum(a.PaymentAmount + a.AllocatedLossAdjExp) !=0;	
		

""")


# In[13]:


from pyspark.sql import functions as F

# 6 join keys
join_cols = [
    "PolicyNumber",
    "TransactionNumber",
    "ClaimNumber",
    "ClaimantNumber",
    "ItemNumber",
    "DerivedClaimCoverageName"
]

# Aggregate correction dataframe to ensure one record per key
amount_corr_agg = (
    Amount_correction_df
    .groupBy(*join_cols)
    .agg(
        F.sum(
            F.coalesce(F.col("PaidAmountALAE2026"), F.lit(0))
        ).alias("PaidAmountALAE2026")
    )
)

# Validation before join
base_count = Liability_df.count()

# Left join
Liability_df = (
    Liability_df.alias("l")
    .join(
        amount_corr_agg.alias("a"),
        on=join_cols,
        how="left"
    )
    .withColumn(
        "FinalReserveAmount",
        F.coalesce(F.col("l.FinalReserveAmount"), F.lit(0))
        + F.coalesce(F.col("a.PaidAmountALAE2026"), F.lit(0))
    )
    .drop("PaidAmountALAE2026")
)

# Validation after join
final_count = Liability_df.count()

print(f"Before Join Count : {base_count}")
print(f"After Join Count  : {final_count}")

if base_count != final_count:
    raise Exception(
        f"Row count changed! Before={base_count}, After={final_count}"
    )


# In[14]:


Property_df= spark.sql("""
WITH CoverageMapping AS (
    SELECT 'ACC DEATH'    AS ClaimCoverageName, 'NF'  AS CoverageExtension, 'ACC DEATH_NF' AS DerivedClaimCoverageName
UNION ALL SELECT   'BA PLUS',   'CM',  'BA PLUS_TE'
UNION ALL SELECT   'BA PLUS',   'RR',  'BA PLUS_RR'
UNION ALL SELECT   'BA PLUS',   'TL',  'BA PLUS_T-L'
UNION ALL SELECT   'BA PLUS_RR',  'CM',  'BA PLUS_RR'
UNION ALL SELECT   'BA PLUS_T-L',       'CM',  'BA PLUS_T-L'
UNION ALL SELECT   'BI',    'BI',  'BODINJLIAB'
UNION ALL SELECT   'BI',    'CBI', 'BODINJLIAB'
UNION ALL SELECT   'BODINJLIAB',  'BI',  'BODINJLIAB'
UNION ALL SELECT   'BODINJLIAB',  'CBI',  'BODINJLIAB'
UNION ALL SELECT   'COLL',    'CL',  'COLLISION'
UNION ALL SELECT   'COLLISION',   'CL',  'COLLISION'
UNION ALL SELECT   'COMB-FPB',   'FPB',  'COMB-FPB'
UNION ALL SELECT   'COMP',    'CM',  'OTC'
UNION ALL SELECT   'PIP LIMIT_INC LOSS', 'NF',  'PIP LIMIT_INC LOSS'
UNION ALL SELECT   'PIP LIMIT_ACC DEATH', 'NF',  'PIP LIMIT_ACC DEATH'
UNION ALL SELECT   'PIP LIMIT_FUN EXP', 'NF',  'PIP LIMIT_FUN EXP'
UNION ALL SELECT   'FRMTRKPOLL',  'PD',  'FRMTRKPOLL_PRPDMGLIAB'
UNION ALL SELECT   'GK COLL',   'CL',  'GK COLL'
UNION ALL SELECT   'GK COMP',   'CM',  'GK COMP'
UNION ALL SELECT   'HIRED AUTO',  'PD',  'HIRED AUTO_PRPDMGLIAB'
UNION ALL SELECT   'HIRED PHYS',  'CM',  'HIRED PHYS_OTC'
UNION ALL SELECT   'INC LOSS',   'NF',  'INC LOSS'
UNION ALL SELECT   'MED EXP',   'FPB',  'MED EXP'
UNION ALL SELECT   'MED EXP',   'NF',  'MED EXP'
UNION ALL SELECT   'MEDPAYMENT',  'MP',  'MEDPAYMENT'
UNION ALL SELECT   'MP',    'MP',  'MEDPAYMENT'
UNION ALL SELECT   'OTC',    'CM',  'OTC'
UNION ALL SELECT   'PD',    'CPD',  'PRPDMGLIAB'
UNION ALL SELECT   'PD',    'PD',  'PRPDMGLIAB'
UNION ALL SELECT   'PIP LIMIT',   'NF',  'PIP LIMIT'
UNION ALL SELECT   'PIP LIMIT_MED EXP', 'NF',  'PIP LIMIT_MED EXP'
UNION ALL SELECT   'PRPDMGLIAB',  'CPD',  'PRPDMGLIAB'
UNION ALL SELECT   'PRPDMGLIAB',  'PD',  'PRPDMGLIAB'
UNION ALL SELECT   'RR',    'CM',  'RR'
UNION ALL SELECT   'RR',    'RR',  'RR'
UNION ALL SELECT   'T&L',    'TL',  'T-L'
UNION ALL SELECT   'T-L',    'CM',  'T-L'
UNION ALL SELECT   'UBI',    'UBI',  'UBI'
UNION ALL SELECT   'UBI NS',   'UBI',  'UBI'
UNION ALL SELECT   'UIM NS',   'UBI',  'UIM'
UNION ALL SELECT   'UIM ST',   'UBI',  'UBI'
UNION ALL SELECT   'UPD',    'UPD',  'UPD'

)

SELECT 	
    a.PolicyNumber,	
    a.TransactionNumber,	
    a.ClaimNumber,	
    a.ClaimantNumber,	
    REPLACE(TRIM(a.ItemNumber), '.0', '') as ItemNumber,	
    a.ClaimCoverageName,	
    a.CoverageExtension,
    LossDate,
	Year(a.LossDate) as AccidentYear,
	m.DerivedClaimCoverageName,
	Cast(0 as decimal(2)) as PaidCount,
	Cast(0 as decimal(2)) as ReserveCount,

	
	
	
    -- Payment amount for the selected year	
    SUM(	
        CASE 	
            WHEN BeginningOfMonth BETWEEN '2025-01-01' AND '2025-12-31'	
            THEN PaymentAmount	
            ELSE 0	
        END	
    ) AS PaymentAmountDuration,	

-- Payment amount as of last year	
    SUM(	
        CASE 	
            WHEN BeginningOfMonth < '2025-01-01'	
            THEN PaymentAmount	
            ELSE 0	
        END	
    ) AS PaymentAmountDurationAsOfLastYear,
	 
	 -- Lifetime payment as of end of 2025 (no BeginningOfMonth filter)	
    SUM(	
        CASE 	
            WHEN BeginningOfMonth < '2026-01-01'	
            THEN PaymentAmount	
            ELSE 0	
        END	
    ) AS PaymentAmountDurationAsOfEndOf2025,
	
	-- LossIncurredNOIBNR for the selected year
SUM(	
        CASE 	
            WHEN BeginningOfMonth BETWEEN '2025-01-01' AND '2025-12-31'	
            THEN LossIncurredNoIBNR	
            ELSE 0	
        END	
    ) AS LossIncurredNOIBNRDuration,	
	
	
	
   

	-- Loss Incurred NO IBNR as of 2024
	SUM(	
        CASE 	
            WHEN BeginningOfMonth < '2025-01-01'
            THEN LossIncurredNoIBNR	
            ELSE 0	
        END	
    ) AS LossIncurredNOIBNRAsOfLastYear,


	
	-- Loss Incurred NO IBNR as of end of 2025
	SUM(	
        CASE 	
            WHEN BeginningOfMonth < '2026-01-01'
            THEN LossIncurredNoIBNR	
            ELSE 0	
        END	
    ) AS LossIncurredNOIBNRAsOfEndOf2025,

SUM(	
        CASE 	
            WHEN BeginningOfMonth = '2025-12-01'
            THEN NetReserveAmount + ExcessDirectAmount
            ELSE 0	
        END	
    ) AS FinalReserveAmount
	
FROM DMAnalytics.FactClaimsMonthlyDataStore	a
/* coverage mapping join */
LEFT JOIN CoverageMapping m
    ON a.ClaimCoverageName = m.ClaimCoverageName
   AND a.CoverageExtension = m.CoverageExtension
	
WHERE CompanyCode = '14044'	
  AND 	EASLobCode = '212'
	
GROUP BY 	
    a.PolicyNumber,	
    a.TransactionNumber,	
    a.ClaimNumber,	
    a.ClaimantNumber,	
    a.ItemNumber,	
    a.ClaimCoverageName,	
    a.CoverageExtension,
	m.DerivedClaimCoverageName,
    a.LossDate,
    a.PropertyOrLiability;	
 	

""")


# In[15]:


Reserved_and_PaidLoss=Property_df.unionByName(
Liability_df
)
from pyspark.sql.functions import col

Reserved_and_PaidLoss = Reserved_and_PaidLoss.withColumn(
    "TransactionNumber",
    col("TransactionNumber").cast("int")
)
# Paid Loss Implementation
from pyspark.sql import functions as F

df = Reserved_and_PaidLoss

PaidLoss_df = (
    df
    # Filter rows
    .filter(F.col("PaymentAmountDuration") != 0)
    
    # Add PaidLossAmount
    .withColumn("PaidLossAmount", F.col("PaymentAmountDuration"))
    
    # Add PaidCount
    .withColumn(
        "PaidCount",
        F.when(
            (F.col("PaymentAmountDuration") == F.col("PaymentAmountDurationAsOfEndOf2025")) &
            (F.col("PaymentAmountDuration") > 0) &
            (F.col("PaymentAmountDurationAsOfEndOf2025") > 0),
            1
        )
        .when(
            (F.col("PaymentAmountDuration") < 0) &
            (F.col("PaymentAmountDurationAsOfLastYear") > 0) &
            (
                (F.col("PaymentAmountDuration") + 
                 F.col("PaymentAmountDurationAsOfLastYear")) == 0
            ),
            -1
        )
        .otherwise(0)
    )
)
# ReserveAmount Implementation
Reserve_Amount_df = (
    df
    # Exclude unwanted rows first
    .filter(
        ~(
            (F.col("PaymentAmountDurationAsOfEndOf2025") == 0) &
            (F.col("LossIncurredNOIBNRAsOfEndOf2025") == 0) &
            (F.year("LossDate") == 2025)
        )
    )

    # Keep rows where ReserveAmount != 0
    .withColumn(
        "ReserveAmount",
            F.col("FinalReserveAmount")
    ).filter(F.col("ReserveAmount") != 0)


    # Add ReserveCount
    .withColumn(
        "ReserveCount",
        F.when(
            (F.col("PaymentAmountDurationAsOfEndOf2025") == 0) &
            (F.col("LossIncurredNOIBNRAsOfEndOf2025") != 0),
            1
        )
        .when(
            (F.col("PaymentAmountDurationAsOfEndOf2025") == 0) &
            (F.col("LossIncurredNOIBNRAsOfEndOf2025") == 0) &
            (F.col("PaymentAmountDurationAsOfLastYear") == 0) &
            (F.col("LossIncurredNoIBNRAsOfLastYear") != 0),
            -1
        )
        .otherwise(0)
    )
)
# selecting the required column
from pyspark.sql import functions as F
final_reserve_df = Reserve_Amount_df.select(
    'PolicyNumber',
 'TransactionNumber',
 'ClaimNumber',
 'ClaimantNumber',
 'ItemNumber',
 'ClaimCoverageName',
 'CoverageExtension',
 'LossDate',
 'AccidentYear',
 'DerivedClaimCoverageName',
 'PaidCount',
 'ReserveCount',
 'ReserveAmount',
 F.lit(None).cast("double").alias("PaidLossAmount"),
 F.lit(None).cast("double").alias("AllocatedLossAdjExp")
)
final_PaidLoss_df=PaidLoss_df.select(
    'PolicyNumber',
 'TransactionNumber',
 'ClaimNumber',
 'ClaimantNumber',
 'ItemNumber',
 'ClaimCoverageName',
 'CoverageExtension',
 'LossDate',
 'AccidentYear',
 'DerivedClaimCoverageName',
 'PaidCount',
 'ReserveCount',
 'PaidLossAmount',
 F.lit(None).cast("double").alias("ReserveAmount"),
 F.lit(None).cast("double").alias("AllocatedLossAdjExp")
)


# In[16]:


#AllocatedAdj Implementation
claim_allocatedLoss=spark.sql("""
WITH CoverageMapping AS (
    SELECT 'ACC DEATH'    AS ClaimCoverageName, 'NF'  AS CoverageExtension, 'ACC DEATH_NF' AS DerivedClaimCoverageName
UNION ALL SELECT   'BA PLUS',			'CM',  'BA PLUS_TE'
UNION ALL SELECT   'BA PLUS',			'RR',  'BA PLUS_RR'
UNION ALL SELECT   'BA PLUS',			'TL',  'BA PLUS_T-L'
UNION ALL SELECT   'BA PLUS_RR',		'CM',  'BA PLUS_RR'
UNION ALL SELECT   'BA PLUS_T-L',       'CM',  'BA PLUS_T-L'
UNION ALL SELECT   'BI',				'BI',  'BODINJLIAB'
UNION ALL SELECT   'BI',				'CBI',	'BODINJLIAB'
UNION ALL SELECT   'BODINJLIAB',		'BI',  'BODINJLIAB'
UNION ALL SELECT   'BODINJLIAB',		'CBI',  'BODINJLIAB'
UNION ALL SELECT   'COLL',				'CL',  'COLLISION'
UNION ALL SELECT   'COLLISION',			'CL',  'COLLISION'
UNION ALL SELECT   'COMB-FPB',			'FPB',  'COMB-FPB'
UNION ALL SELECT   'COMP',				'CM',  'OTC'
UNION ALL SELECT   'FRMTRKPOLL',		'PD',  'FRMTRKPOLL_PRPDMGLIAB'
UNION ALL SELECT   'GK COLL',			'CL',  'GK COLL'
UNION ALL SELECT   'GK COMP',			'CM',  'GK COMP'
UNION ALL SELECT   'HIRED AUTO',		'PD',  'HIRED AUTO_PRPDMGLIAB'
UNION ALL SELECT   'PIP LIMIT_INC LOSS', 'NF',  'PIP LIMIT_INC LOSS'
UNION ALL SELECT   'PIP LIMIT_ACC DEATH', 'NF',  'PIP LIMIT_ACC DEATH'
UNION ALL SELECT   'PIP LIMIT_FUN EXP', 'NF',  'PIP LIMIT_FUN EXP'
UNION ALL SELECT   'HIRED PHYS',		'CM',  'HIRED PHYS_OTC'
UNION ALL SELECT   'INC LOSS',			'NF',  'INC LOSS'
UNION ALL SELECT   'MED EXP',			'FPB',  'MED EXP'
UNION ALL SELECT   'MED EXP',			'NF',  'MED EXP'
UNION ALL SELECT   'MEDPAYMENT',		'MP',  'MEDPAYMENT'
UNION ALL SELECT   'MP',				'MP',  'MEDPAYMENT'
UNION ALL SELECT   'OTC',				'CM',  'OTC'
UNION ALL SELECT   'PD',				'CPD',  'PRPDMGLIAB'
UNION ALL SELECT   'PD',				'PD',  'PRPDMGLIAB'
UNION ALL SELECT   'PIP LIMIT',			'NF',  'PIP LIMIT'
UNION ALL SELECT   'PIP LIMIT_MED EXP', 'NF',  'PIP LIMIT_MED EXP'
UNION ALL SELECT   'PRPDMGLIAB',		'CPD',  'PRPDMGLIAB'
UNION ALL SELECT   'PRPDMGLIAB',		'PD',  'PRPDMGLIAB'
UNION ALL SELECT   'RR',				'CM',  'RR'
UNION ALL SELECT   'RR',				'RR',  'RR'
UNION ALL SELECT   'T&L',				'TL',  'T-L'
UNION ALL SELECT   'T-L',				'CM',  'T-L'
UNION ALL SELECT   'UBI',				'UBI',  'UBI'
UNION ALL SELECT   'UBI NS',			'UBI',  'UBI'
UNION ALL SELECT   'UIM NS',			'UBI',  'UIM'
UNION ALL SELECT   'UIM ST',			'UBI',  'UBI'
UNION ALL SELECT   'UPD',				'UPD',  'UPD'

)

SELECT
    a.PolicyNumber,
    a.TransactionNumber,
    a.ClaimNumber,
    a.ClaimantNumber,
    a.LossDate,
    Year(to_date(a.LossDate,'yyyyMMdd')) as AccidentYear,
    a.PropertyOrLiability,
    a.ClaimCoverageName,
    a.CoverageExtension,
    a.LocationSequenceNumber,
    REPLACE(TRIM(a.ItemNumber), '.0', '') as ItemNumber,
    CAST(NULL AS DECIMAL(18,2)) as PaidLoss, -- Paid loss is same as Payment amount in this scenario
    sum(a.AllocatedLossAdjExp) as AllocatedLossAdjExp,
    CAST(NULL AS DECIMAL(18,2)) as ReserveAmount,
   -- a.ClaimCount,
   -- a.ReserveCount,
	m.DerivedClaimCoverageName
  
FROM FactClaimsMonthlyDataStore a

/* coverage mapping join */
LEFT JOIN CoverageMapping m
    ON a.ClaimCoverageName = m.ClaimCoverageName
   AND a.CoverageExtension = m.CoverageExtension


WHERE EASLobCode IN ('193','194','212')
  AND a.BeginningOfMonth BETWEEN '2025-01-01' AND '2025-12-01'
  AND (a.AllocatedLossAdjExp <> 0 )

  group by 
  a.PolicyNumber,
    a.TransactionNumber,
    a.ClaimNumber,
    a.ClaimantNumber,
    a.ClaimCoverageName,
    a.CoverageExtension,
    a.LocationSequenceNumber,
    REPLACE(TRIM(a.ItemNumber), '.0', ''),
	m.DerivedClaimCoverageName,
    a.LossDate,
    a.PropertyOrLiability
 ;
""")

from pyspark.sql.functions import col

claim_allocatedLoss = claim_allocatedLoss.withColumn(
    "TransactionNumber",
    col("TransactionNumber").cast("int")
)

final_AllocatedLoss_df=claim_allocatedLoss.select(
    'PolicyNumber',
 'TransactionNumber',
 'ClaimNumber',
 'ClaimantNumber',
 'ItemNumber',
 'ClaimCoverageName',
 'CoverageExtension',
 'LossDate',
 'AccidentYear',
 'DerivedClaimCoverageName',
 F.lit(None).cast("integer").alias("PaidCount"),
 F.lit(None).cast("integer").alias("ReserveCount"),
 'AllocatedLossAdjExp',
 F.lit(None).cast("double").alias("ReserveAmount"),
 F.lit(None).cast("double").alias("PaidLossAmount")
)

from functools import reduce 
dfs=[final_PaidLoss_df,final_reserve_df,final_AllocatedLoss_df]
Final_Claim_data= reduce(lambda x,y :x.unionByName(y), dfs)


# Pulling GUIDS and Other Column
mappingGuid_data=spark.sql("""
select PolicyNumber, TransactionNumber, ClaimNumber,
ClaimantNumber, ItemNumber, ClaimCoverageName, CoverageExtension,
CauseOfLoss, ParentCoverageGUID, ChildCoverageGUID, ParentRiskitemGUID
from DMAnalytics.FactClaims
where CompanyCode = '14044'
and LOBCode ='AC'
""")

######################################### Pulling Null TransactionNumber
from pyspark.sql.functions import col, coalesce

# Step 1: Read fact table
fc_df = spark.table("FactClaimsMonthlyDataStore")

# Step 2: (Optional but recommended) remove duplicates
fc_df = fc_df.dropDuplicates([
    "PolicyNumber", 
    "ClaimNumber", 
    "ClaimantNumber"
])

# Step 3: Join and update TransactionNumber
mappingGuid_data = mappingGuid_data.alias("mg").join(
    fc_df.alias("fc"),
    on=[
        col("mg.PolicyNumber") == col("fc.PolicyNumber"),
        col("mg.ClaimNumber") == col("fc.ClaimNumber"),
        col("mg.ClaimantNumber") == col("fc.ClaimantNumber")
    ],
    how="left"
).select(
    # keep all original columns except TransactionNumber
    *[col(f"mg.{c}") for c in mappingGuid_data.columns if c != "TransactionNumber"],
    
    # fill TransactionNumber
    coalesce(
        col("mg.TransactionNumber"),
        col("fc.TransactionNumber")
    ).alias("TransactionNumber")
)
###################################### Updating BA PLUS CoverageExtension
from pyspark.sql.functions import when, col

Final_Claim_data = Final_Claim_data.withColumn(
    "CoverageExtension",
    when(
        (col("ClaimCoverageName") == "BA PLUS") &
        (~col("PolicyNumber").startswith("GMCA")),
        "CM"
    ).otherwise(col("CoverageExtension"))
)
###################################################
from pyspark.sql.functions import when, col

Final_Claim_data = Final_Claim_data.withColumn(
    "CoverageExtension",
    when(
        col("CoverageExtension").isin("RR", "TL"),
        "CM"
    ).otherwise(col("CoverageExtension"))
)
# Changing MED EXP CoverageName
from pyspark.sql.functions import when, col

Final_Claim_data = Final_Claim_data.withColumn(
    "DerivedClaimCoverageName",
    when(
        (col("DerivedClaimCoverageName") == "MED EXP") &
        (col("CoverageExtension") == "NF") &
        (~col("PolicyNumber").startswith("GMCA")),
        "PIP LIMIT_MED EXP"
    ).otherwise(col("DerivedClaimCoverageName"))
)
# ============================================
# Purpose:
# Perform LEFT JOIN between Final_Claim_data and mappingGuid_data
# while:
# Handling datatype mismatch (float vs int TransactionNumber)
# Preventing row duplication
# Preserving row count and amounts
# ============================================

from pyspark.sql.functions import col, sum, broadcast
# Reading Guids file

# --------------------------------------------
# Step 1: Define join keys (excluding TransactionNumber for custom handling)
# --------------------------------------------
base_join_cols = [
    "PolicyNumber",
    "ClaimNumber",
    "ClaimantNumber",
    "ItemNumber",
    "ClaimCoverageName",
    "CoverageExtension"
]

# --------------------------------------------
# Step 2: Deduplicate mapping table
# Prevents duplication issues in join
# --------------------------------------------
mapping_dedup = mappingGuid_data.dropDuplicates([
    "PolicyNumber",
    "TransactionNumber",
    "ClaimNumber",
    "ClaimantNumber",
    "ItemNumber",
    "ClaimCoverageName",
    "CoverageExtension",
    "ParentRiskitemGUID"
])

# --------------------------------------------
# Step 3: Select required columns
# --------------------------------------------
mapping_trimmed = mapping_dedup.select(
    "PolicyNumber",
    "TransactionNumber",
    "ClaimNumber",
    "ClaimantNumber",
    "ItemNumber",
    "ClaimCoverageName",
    "CoverageExtension",
    "ParentCoverageGUID",
    "ChildCoverageGUID",
    "CauseOfLoss",
    "ParentRiskitemGUID"
)

# --------------------------------------------
# Step 4: Perform LEFT JOIN with type-safe comparison
# Cast float -> int ONLY for join condition Keep original column unchanged
# --------------------------------------------
join_condition = (
    (col("f.PolicyNumber") == col("m.PolicyNumber")) &
    (col("f.ClaimNumber") == col("m.ClaimNumber")) &
    (col("f.ClaimantNumber") == col("m.ClaimantNumber")) &
    (col("f.ItemNumber").cast("int") == col("m.ItemNumber").cast("int")) &
    (col("f.DerivedClaimCoverageName") == col("m.ClaimCoverageName")) &
    (col("f.CoverageExtension") == col("m.CoverageExtension")) &
    (col("f.TransactionNumber").cast("int") == col("m.TransactionNumber").cast("int"))
)

Final_joined = Final_Claim_data.alias("f").join(
    mapping_trimmed.alias("m"),  # remove broadcast if large
    on=join_condition,
    how="left"
)

# --------------------------------------------
# Step 5: Select final output
# --------------------------------------------
Final_output = Final_joined.select(
    "f.*",
    "m.ParentCoverageGUID",
    "m.ChildCoverageGUID",
    "m.CauseOfLoss",
    "m.ParentRiskitemGUID"
)




# In[18]:


display(final_claim_data_ISO_NISS)


# In[19]:


######################## TermEffectiveDate Filter
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# Load policy table from Fabric
policy_df = spark.read.table("ODS_STATS_TBLS_LH.policy_policy_detail_info")

# Convert cutoff date
cutoff_date = "2023-07-01"

# Join final_claim_data with policy_df using PolicyNumber + TransactionNumber = PolicyVersion
joined_df = (
    Final_output.alias("c")
    .join(
        policy_df.alias("p"),
        (F.col("c.PolicyNumber") == F.col("p.PolicyNumber")) &
        (F.col("c.TransactionNumber").cast("int") == F.col("p.PolicyVersion").cast("int")),
        "left"
    )
)

# Apply the TermEffectiveDate ≥ 1 July 2023 filter
final_claim_data_ISO_NISS = (
    joined_df
    .filter(F.col("p.TermEffectiveDate") >= F.lit(cutoff_date))
    .select("c.*")     # keep only claim data columns
)
################################# CONDITION TO REMOVE negative FinalReserveAmount
final_claim_data_ISO_NISS=final_claim_data_ISO_NISS.filter(col('ReserveAmount')>=0)

########################### Reverse Claim-> Policy Mapping
from pyspark.sql import functions as F
from pyspark.sql.functions import col, upper

# =====================================================
# 1. PREPARE CLAIM DATA (BASE)
# =====================================================

c = final_claim_data_ISO_NISS.alias("c")

# =====================================================
# 2. LOAD POLICY TABLES
# =====================================================

PDI = spark.table("ODS_STATS_TBLS_LH.policy_policy_detail_info").alias("pdi")
PI  = spark.table("ODS_STATS_TBLS_LH.policy_Product_info").alias("pi")
RI  = spark.table("ODS_STATS_TBLS_LH.policy_risk_item_info").alias("ri")
CI  = spark.table("ODS_STATS_TBLS_LH.policy_coverage_info").alias("ci")

# =====================================================
# 3. JOIN CLAIM → PDI
# =====================================================

df = c.join(
    PDI,
    (col("c.PolicyNumber") == col("pdi.PolicyNumber")) &
    (col("c.TransactionNumber").cast("int") == col("pdi.PolicyVersion").cast("int")),
    "left"
)

# =====================================================
# 4. JOIN → PRODUCT INFO
# =====================================================

df = df.join(
    PI,
    col("pdi.InTransactionId") == col("pi.InTransactionId"),
    "left"
)

# =====================================================
# 5. JOIN → RISK ITEM (USING GUID)
# =====================================================

df = df.join(
    RI,
    (col("pdi.InTransactionId") == col("ri.InTransactionId")) &
    (upper(col("c.ParentRiskitemGUID")) == upper(col("ri.RiskItemInfoGUID"))),
    "left"
)

# =====================================================
# 6. JOIN → COVERAGE (USING GUID)
# =====================================================

df = df.join(
    CI,
    (col("pdi.InTransactionId") == col("ci.InTransactionId")) &
    (upper(col("c.ParentCoverageGUID")) == upper(col("ci.CoverageInfoGUID"))),
    "left"
)

# =====================================================
# 7. FINAL SELECT (CLAIM + POLICY ENRICHMENT)
# =====================================================

final_claim_policy_enriched_df = df.select(

    # =============================
    #  ALL CLAIM COLUMNS
    # =============================
    col("c.*"),

    # =============================
    #  POLICY DETAIL INFO
    # =============================
    col("pdi.PolicyVersion"),
    col("pdi.InTransactionId"),
    col("pdi.ReferencePolicyVersion"),
    col("pdi.PolicySource"),
    col("pdi.BookDate"),
    col("pdi.PrimaryRiskState"),
    col("pdi.ProrataPercent"),

    # =============================
    #  PRODUCT INFO
    # =============================
    col("pi.LOBCode"),

    # =============================
    #  RISK ITEM INFO
    # =============================
    col("ri.RiskItemType"),
    col("ri.UnitNumber"),
    col("ri.ClassCode"),

    # =============================
    #  COVERAGE INFO (RENAMED TO AVOID CONFLICT)
    # =============================
    col("ci.CoverageId"),
    col("ci.ParentCoverageId"),
    col("ci.RiskItemId"),
    col("ci.ProductId"),

    col("ci.CoverageCode"),
    col("ci.CoverageExtension").alias("PolicyCoverageExtension"),

    col("ci.CoverageGroup"),
    col("ci.OnsetAmount"),
    col("ci.OffsetAmount"),
    col("ci.Limit1"),
    col("ci.Limit2"),
    col("ci.ASLOB"),
    col("ci.Option1"),
    col("ci.Exposure"),
    col("ci.Deductible1"),
    col("ci.ClaimCode"),

    col("ci.CoverageInfoGUID").alias("PolicyCoverageInfoGUID")
)

# =====================================================
# DONE
# =====================================================

#display(final_claim_policy_enriched_df)
from pyspark.sql.functions import col, upper, trim, when

# =====================================================
# 1. CLEAN CLAIM DATA
# =====================================================

claim_df = final_claim_data_ISO_NISS \
    .withColumn("ParentCoverageGUID", upper(trim(col("ParentCoverageGUID")))) \
    .withColumn("ParentRiskitemGUID", upper(trim(col("ParentRiskitemGUID")))) \
    .alias("c")

# =====================================================
# 2. POLICY LOOKUPS (ONLY DISTINCT KEYS)
# =====================================================

coverage_guid_df = spark.table("ODS_STATS_TBLS_LH.policy_coverage_info") \
    .select(upper(trim(col("CoverageInfoGUID"))).alias("PolicyCoverageGUID")) \
    .distinct() \
    .alias("cov")

riskitem_guid_df = spark.table("ODS_STATS_TBLS_LH.policy_risk_item_info") \
    .select(upper(trim(col("RiskItemInfoGUID"))).alias("PolicyRiskItemGUID")) \
    .distinct() \
    .alias("risk")

# =====================================================
# 3. JOIN TO CHECK EXISTENCE (LEFT JOIN)
# =====================================================

debug_df = claim_df \
    .join(
        coverage_guid_df,
        col("ParentCoverageGUID") == col("PolicyCoverageGUID"),
        "left"
    ) \
    .join(
        riskitem_guid_df,
        col("ParentRiskitemGUID") == col("PolicyRiskItemGUID"),
        "left"
    )

# =====================================================
# 4. PRECISE CLASSIFICATION LOGIC
# =====================================================

debug_df = debug_df.withColumn(
    "ValidationStatus",

    #  Case 1: Missing in CLAIM
    when(
        col("ParentCoverageGUID").isNull() |
        col("ParentRiskitemGUID").isNull(),
        "MISSING_IN_CLAIM"
    )

    #  Case 2: Missing in POLICY
    .when(
        (col("ParentCoverageGUID").isNotNull() & col("PolicyCoverageGUID").isNull()) |
        (col("ParentRiskitemGUID").isNotNull() & col("PolicyRiskItemGUID").isNull()),
        "MISSING_IN_POLICY"
    )

    #  Case 3: Available but NOT MATCHED
    .when(
        col("PolicyCoverageGUID").isNotNull() &
        col("PolicyRiskItemGUID").isNotNull() &
        (
            (col("ParentCoverageGUID") != col("PolicyCoverageGUID")) |
            (col("ParentRiskitemGUID") != col("PolicyRiskItemGUID"))
        ),
        "AVAILABLE_BUT_NOT_MATCHED"
    )

    #  Case 4: Proper Match
    .otherwise("MATCHED")
)

# =====================================================
# 5. EXTRA FLAGS FOR GRANULAR DEBUGGING
# =====================================================

debug_df = debug_df.withColumn(
    "CoverageStatus",
    when(col("ParentCoverageGUID").isNull(), "MISSING_IN_CLAIM")
    .when(col("PolicyCoverageGUID").isNull(), "MISSING_IN_POLICY")
    .otherwise("AVAILABLE")
).withColumn(
    "RiskItemStatus",
    when(col("ParentRiskitemGUID").isNull(), "MISSING_IN_CLAIM")
    .when(col("PolicyRiskItemGUID").isNull(), "MISSING_IN_POLICY")
    .otherwise("AVAILABLE")
)

# =====================================================
# 6. FILTER PROBLEMATIC RECORDS ONLY
# =====================================================

unmatched_debug_df = debug_df.filter(col("ValidationStatus") != "MATCHED")

# =====================================================
# OUTPUT
# =====================================================

#display(unmatched_debug_df)


# In[20]:


from pyspark.sql.functions import col, when

unmatched_debug_df = unmatched_debug_df.withColumn(
    "DerivedClaimCoverageName",
    when(
        (col("PolicyNumber") == 134028) &
        (col("TransactionNumber") == 26) &
        (col("ClaimNumber") == 596598) &
        (col("ClaimCoverageName") == "MED EXP"),
        "PIP LIMIT"
    ).otherwise(col("DerivedClaimCoverageName"))
)
df_nonzero = unmatched_debug_df.filter(col("ItemNumber") != 0)
from pyspark.sql.functions import col, row_number, broadcast, lit
from pyspark.sql.window import Window

# -------------------------------
# Load Fabric tables
# -------------------------------
pdi = spark.table("ODS_STATS_TBLS_LH.policy_policy_detail_info")
ri  = spark.table("ODS_STATS_TBLS_LH.policy_risk_item_info")
ci  = spark.table("ODS_STATS_TBLS_LH.policy_coverage_info")
qa  = spark.table("ODS_STATS_TBLS_LH.policy_question_answer")

# -------------------------------
# Filter QA (only needed rows)
# -------------------------------
qa_filtered = (
    qa.filter(col("QuestionCode") == "PrintUnitNumber")
      .select(
          "InTransactionId",
          "RiskItemId",
          col("TextValue").alias("ItemNumber")
      )
)

# -------------------------------
# Build policy dataset
# -------------------------------
policy_df = (
    pdi.alias("pdi")
    .join(ri.alias("ri"),
          col("pdi.InTransactionId") == col("ri.InTransactionId"))
    .join(ci.alias("ci"),
          (col("pdi.InTransactionId") == col("ci.InTransactionId")) &
          (col("ri.RiskItemId") == col("ci.RiskItemId")))
    .join(qa_filtered.alias("qa"),
          (col("pdi.InTransactionId") == col("qa.InTransactionId")) &
          (col("ri.RiskItemId") == col("qa.RiskItemId")))
    .select(
        col("pdi.InTransactionId"),
        col("pdi.PolicyNumber"),
        col("pdi.PolicyVersion"),
        col("pdi.ReferencePolicyVersion"),
        col("pdi.PolicySource"),
        col("pdi.BookDate"),
        col("pdi.PrimaryRiskState"),
        col("pdi.ProrataPercent"),
        col("qa.ItemNumber"),
        col("ci.CoverageCode"),
        col("ci.CoverageExtension").alias("PolicyCoverageExtension"),

        col("ri.RiskItemType"),
        col("ri.UnitNumber"),
        col("ri.ClassCode"),
        col("ci.CoverageId"),
        col("ci.ParentCoverageId"),
        col("ci.RiskItemId"),
        col("ci.ProductId"),
        col("ci.CoverageGroup"),
        col("ci.OnsetAmount"),
        col("ci.OffsetAmount"),
        col("ci.Limit1"),
        col("ci.Limit2"),
        col("ci.ASLOB"),
        col("ci.Option1"),
        col("ci.Exposure"),
        col("ci.Deductible1"),
        col("ci.ClaimCode"),
        col("ci.CoverageInfoGUID")
    )
)

# -------------------------------
# Deduplicate (prevent row explosion)
# -------------------------------
window_spec = Window.partitionBy(
    "PolicyNumber", "PolicyVersion", "ItemNumber",
    "CoverageCode", "PolicyCoverageExtension"
).orderBy(col("CoverageId"))

policy_df_dedup = (
    policy_df.withColumn("rn", row_number().over(window_spec))
             .filter(col("rn") == 1)
             .drop("rn")
)

# -------------------------------
# LEFT JOIN (fully qualified aliases)
# -------------------------------
result_df = (
    df_nonzero.alias("udf")
    .join(
        policy_df_dedup.alias("pol"),
        (col("udf.PolicyNumber") == col("pol.PolicyNumber")) &
        (col("udf.TransactionNumber").cast("int") == col("pol.PolicyVersion").cast("int")) &
        (col("udf.ItemNumber").cast("int") == col("pol.ItemNumber").cast("int")) &
        (col("udf.DerivedClaimCoverageName") == col("pol.CoverageCode")) &
        (col("udf.CoverageExtension") == col("pol.PolicyCoverageExtension")),
        "left"
    )
    .select(
        "udf.*",  # preserve original dataset

        # bring only needed columns from policy side
        col("pol.InTransactionId"),
        col("pol.PolicyVersion"),
        col("pol.ReferencePolicyVersion"),
        col("pol.PolicySource"),
        col("pol.BookDate"),
        lit("AC").alias("LOBCode"),
        col("pol.PrimaryRiskState"),
        col("pol.ProrataPercent"),
        col("pol.RiskItemType"),
        col("pol.UnitNumber"),
        col("pol.CoverageCode"),
        col("pol.PolicyCoverageExtension"),
        col("pol.ClassCode"),
        col("pol.CoverageId"),
        col("pol.ParentCoverageId"),
        col("pol.RiskItemId"),
        col("pol.ProductId"),
        col("pol.CoverageGroup"),
        col("pol.OnsetAmount"),
        col("pol.OffsetAmount"),
        col("pol.Limit1"),
        col("pol.Limit2"),
        col("pol.ASLOB"),
        col("pol.Option1"),
        col("pol.Exposure"),
        col("pol.Deductible1"),
        col("pol.ClaimCode"),
        col("pol.CoverageInfoGUID").alias('PolicyCoverageInfoGUID')
    )
)

# -------------------------------
# Validate row count
# -------------------------------
#print("Before:", unmatched_debug_df.count())
#print("After :", result_df.count())
from pyspark.sql import functions as F

# -------------------------------
# Filter ItemNumber = 0
# -------------------------------
df_zero = unmatched_debug_df.filter(F.col("ItemNumber") == 0)

# -------------------------------
# Load tables
# -------------------------------
pdi = spark.table("ODS_STATS_TBLS_LH.policy_policy_detail_info")
ci  = spark.table("ODS_STATS_TBLS_LH.policy_coverage_info")

# -------------------------------
# Step 1: Get InTransactionId from PDI
# -------------------------------
policy_base = (
    df_zero.alias("udf")
    .join(
        pdi.alias("pdi"),
        (F.col("udf.PolicyNumber") == F.col("pdi.PolicyNumber")) &
        (F.col("udf.TransactionNumber") == F.col("pdi.PolicyVersion")),
        "left"
    )
    .select(
        "udf.*",
        F.col("pdi.InTransactionId"),
        F.col("pdi.PolicySource"),
        F.col("pdi.PolicyVersion"),
        F.col("pdi.ReferencePolicyVersion"),
        F.col("pdi.BookDate"),
        F.col("pdi.ProrataPercent"),
        F.col("pdi.PrimaryRiskState")
    )
)

# -------------------------------
# Step 2: Join with CoverageInfo
# -------------------------------
result_zero = (
    policy_base.alias("base")
    .join(
        ci.alias("ci"),
        (F.col("base.InTransactionId") == F.col("ci.InTransactionId")) &
        (F.col("base.ClaimCoverageName") == F.col("ci.CoverageCode")),   
        #(F.col("base.CoverageExtension") == F.col("ci.CoverageExtension")),
        "left"
    )
    .select(
        "base.*",

        # Adding policy + coverage columns
        F.lit("AC").alias("LOBCode"),

        F.lit(None).alias("RiskItemType"),
        F.lit(None).alias("UnitNumber"),
        F.lit(None).alias("ClassCode"),

        F.col("ci.CoverageId"),
        F.col("ci.ParentCoverageId"),
        F.col("ci.RiskItemId"),
        F.col("ci.ProductId"),

        F.col("ci.CoverageCode"),
        F.col("ci.CoverageExtension").alias("PolicyCoverageExtension"),

        F.col("ci.CoverageGroup"),

        F.lit(None).cast("double").alias("OnsetAmount"),
        F.lit(None).cast("double").alias("OffsetAmount"),

        F.col("ci.Limit1"),
        F.col("ci.Limit2"),
        F.col("ci.ASLOB"),
        F.col("ci.Option1"),
        F.col("ci.Exposure"),
        F.col("ci.Deductible1"),
        F.col("ci.ClaimCode"),
        F.col("ci.CoverageInfoGUID").alias("PolicyCoverageInfoGUID")
    )
)

# -------------------------------
# Debug check (will now work )
# -------------------------------
#check = result_zero.filter(F.col("PolicyNumber") == 142633)
#display(check)

#print("Flow 2 count:", result_zero.count())


# In[21]:


unit_fallback_enriched_df=result_df.unionByName(result_zero)
unit_fallback_enriched_df=unit_fallback_enriched_df.select(
    'PolicyNumber',
 'TransactionNumber',
 'ClaimNumber',
 'ClaimantNumber',
 'ItemNumber',
 'ClaimCoverageName',
 'CoverageExtension',
 'LossDate',
 'AccidentYear',
 'DerivedClaimCoverageName',
 'PaidCount',
 'ReserveCount',
 'PaidLossAmount',
 'ReserveAmount',
 'AllocatedLossAdjExp',
 'ParentCoverageGUID',
 'ChildCoverageGUID',
 'CauseOfLoss',
 'ParentRiskitemGUID',
 'PolicyVersion',
 'InTransactionId',
 'ReferencePolicyVersion',
 'PolicySource',
 'BookDate',
 F.lit(None).alias('ProrataPercent'),
 'PrimaryRiskState',
 'LOBCode',
 'RiskItemType',
 'UnitNumber',
 'ClassCode',
 'CoverageId',
 'ParentCoverageId',
 'RiskItemId',
 'ProductId',
 'CoverageCode',
 'PolicyCoverageExtension',
 'CoverageGroup',
  F.lit(None).cast("double").alias('OnsetAmount'),
  F.lit(None).cast("double").alias('OffsetAmount'),
 'Limit1',
 'Limit2',
 'ASLOB',
 'Option1',
 F.lit(None).alias('Exposure'),
 'Deductible1',
 'ClaimCode',
 'PolicyCoverageInfoGUID'
)
#################################### MERGING CLAIM
from pyspark.sql.functions import col

# =====================================================
# 1. DEFINE KEYS
# =====================================================

keys = [
    "PolicyNumber",
    "TransactionNumber",
    "ClaimNumber",
    "ClaimantNumber",
    "ClaimCoverageName",
    "CoverageExtension"
]

# =====================================================
# 2. REMOVE OLD WRONG ROWS
# =====================================================

filtered_original_df = final_claim_policy_enriched_df.alias("orig") \
    .join(
        unit_fallback_enriched_df.select(*keys).distinct().alias("fix"),
        on=keys,
        how="leftanti"
    )

# =====================================================
# 3. UNION CORRECTED DATA
# =====================================================

Final_Claim_Policy_data = filtered_original_df.unionByName(unit_fallback_enriched_df)

# =====================================================
# OUTPUT
# =====================================================

#display(Final_Claim_Policy_data)
############### Column Renaming inorder to merge correct column in policy
Final_Claim_Policy_data = (
    Final_Claim_Policy_data
    .withColumnRenamed("CoverageExtension", "ClaimCoverageExtension")
)
Final_Claim_Policy_data = (
    Final_Claim_Policy_data
    .withColumnRenamed("PolicyCoverageInfoGUID", "CoverageInfoGUID")
    .withColumnRenamed("PolicyCoverageExtension", "CoverageExtension")
)
################ Saving Table
spark.sql("DROP TABLE IF EXISTS Claim_Policy_NewApproachData")
Final_Claim_Policy_data.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Claim_Policy_NewApproachData")


# In[22]:


df = spark.sql("""
select * from OnSet_And_Refrence_Offset
""")


# In[23]:


df_claim_policy=spark.sql("""
SELECT * from Claim_Policy_NewApproachData
""")


# In[24]:


def compare_df_columns(df1, df2, df1_name="df1", df2_name="df2"):
    cols1 = set(df1.columns)
    cols2 = set(df2.columns)

    only_in_df1 = cols1 - cols2
    only_in_df2 = cols2 - cols1
    common_cols = cols1 & cols2

    print(f" Common columns ({len(common_cols)}):")
    print(sorted(common_cols))

    print(f"\n❌ Columns only in {df1_name} ({len(only_in_df1)}):")
    print(sorted(only_in_df1))

    print(f"\n❌ Columns only in {df2_name} ({len(only_in_df2)}):")
    print(sorted(only_in_df2))

compare_df_columns(df,df_claim_policy)


# In[25]:


from pyspark.sql.functions import lit

df_claim_policy = df_claim_policy.withColumn("OffsetAmount", lit(0)) \
       .withColumn("OnsetAmount", lit(0)) \
       .withColumn("Exposure", lit(None)) \
       .withColumn("ProrataPercent", lit(None)) # NULL or Zero need to confirm.



# In[26]:


data = df.unionByName(
    df_claim_policy, allowMissingColumns=True
)
#print(f"Final Data Count: { data.count()}")


# # Address Handling scenarions (Primary Risk Address)

# For IBMi

# In[27]:


## Address Pulling for different InTransection ID
# for only PolicySource = 'IBMi'
tx_df = (
    data
    .filter("PolicySource = 'IBMi'")
    .select("InTransactionId")
    .distinct()
)
#tx_df = data.select("InTransactionId").distinct()


# In[28]:


#Vehicle RiskItemIds per transaction
risk_items_df_Address = spark.sql("""
    SELECT
        InTransactionId,
        RiskItemId
    FROM ODS_STATS_TBLS_LH.policy_risk_item_info
    WHERE RiskItemType = 'Vehicle'
""")


# In[29]:


#Join only relevant transactions:
risk_items_df_Address = risk_items_df_Address.join(
    tx_df,
    on="InTransactionId",
    how="inner"
)


# In[30]:


#Pick RiskItemId where LocationNumber = 1
location_df = spark.sql("""
    SELECT
        InTransactionId,
        RiskItemId,
        TextValue
    FROM ODS_STATS_TBLS_LH.policy_question_answer
    WHERE QuestionCode = 'LocationNumber'
""")


# In[31]:


# Filter TextValue = 1 and valid RiskItems:
location_1_df = (
    risk_items_df_Address
    .join(location_df, ["InTransactionId", "RiskItemId"], "inner")
    .filter("TextValue = '1'")
)


# In[32]:


#Pull ZipCode & ZipExt
address_df = spark.sql("""
    SELECT
        PA.InTransactionId,
        PA.RiskItemId,
        P.ZipCode,
        P.ZipExt
    FROM ODS_STATS_TBLS_LH.policy_policy_address PA
    JOIN ODS_STATS_TBLS_LH.party_address P
      ON P.AddressId = PA.AddressID
""")


# In[33]:


# Join only LocationNumber TextValue = 1 RiskItemId:
zip_df = (
    location_1_df
    .join(address_df, ["InTransactionId", "RiskItemId"], "inner")
    .select("InTransactionId", "ZipCode", "ZipExt")
)
#display(zip_df)


# In[34]:


# For removing duplicate 
from pyspark.sql import functions as F

zip_tx_df = (
    zip_df
    .groupBy("InTransactionId")
    .agg(
        F.first("ZipCode", ignorenulls=True).alias("ZipCode"),
        F.first("ZipExt", ignorenulls=True).alias("ZipExt")
    )
)


# In[35]:


final_df_not_dup= (
    data
    .join(zip_tx_df, on="InTransactionId", how="left")
)
#final_df_not_dup.count()


# For Origami

# In[36]:


###### for Origami policies As the process to pull ZIPCode is different
# for only PolicySource = 'Origami'
tx_df_org = (
    data
    .filter("PolicySource = 'Origami'")
    .select("InTransactionId")
    .distinct()
)
#display(tx_df_org)


# In[37]:


#tx_df_org.count()
#Vehicle RiskItems for Origami Transactions
risk_items_org_df = spark.sql("""
    SELECT
        InTransactionId,
        RiskItemId
    FROM ODS_STATS_TBLS_LH.policy_risk_item_info
    WHERE RiskItemType = 'Vehicle'
""")


# In[38]:


# Restrict strictly to Origami transactions:

risk_items_org_df = (
    risk_items_org_df
    .join(tx_df_org, "InTransactionId", "inner")
)


# In[39]:


# Pull ZIP from Address Tables (Controlled)
address_org_raw_df = spark.sql("""
    SELECT
        PA.InTransactionId,
        PA.RiskItemId,
        P.ZipCode,
        P.ZipExt
    FROM ODS_STATS_TBLS_LH.policy_policy_address PA
    JOIN ODS_STATS_TBLS_LH.party_address P
      ON P.AddressId = PA.AddressID
    WHERE PA.AddressUse IN (
        'Risk Address',
        'Garage Address-PolicyLocation',
        'Garage Address - Other',
        'Garage Address-Other'
    )
""")


# In[40]:


#Join only valid vehicle RiskItems:
address_org_df = (
    address_org_raw_df
    .join(risk_items_org_df, ["InTransactionId", "RiskItemId"], "inner")
)


# In[41]:


# Pick FIRST non‑NULL ZIP per Transaction (CRITICAL) INorder to remove the duplicacy
from pyspark.sql import functions as F

zip_org_tx_df = (
    address_org_df
    .filter(
        F.col("ZipCode").isNotNull() &
        (F.trim(F.col("ZipCode")) != "")
    )
    .groupBy("InTransactionId")
    .agg(
        F.first(F.col("ZipCode"), ignorenulls=True).alias("ZipCode"),
        F.first(F.col("ZipExt"), ignorenulls=True).alias("ZipExt")
    )
)


# In[42]:


#Join Back to Base Origami Data 
from pyspark.sql import functions as F

# Get all columns EXCEPT ZipCode and ZipExt from base
base_cols = [
    c for c in final_df_not_dup.columns
    if c not in ("ZipCode", "ZipExt")
]

final_df_not_dup = (
    final_df_not_dup.alias("base")
    .join(
        zip_org_tx_df.alias("org"),
        on="InTransactionId",
        how="left"
    )
    .select(
        *[F.col(f"base.{c}") for c in base_cols],
        F.coalesce(F.col("base.ZipCode"), F.col("org.ZipCode")).alias("ZipCode"),
        F.coalesce(F.col("base.ZipExt"),  F.col("org.ZipExt")).alias("ZipExt")
    )
)


# In[43]:


# Onset offset table Including ZIPCode and ZipEXt
spark.sql("DROP TABLE IF EXISTS OnSet_And_Refrence_Offset_Address")
final_df_not_dup.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("OnSet_And_Refrence_Offset_Address")


# # Transformation Part

# In[44]:


Final_Policy_Claim=spark.sql("""
select * from OnSet_And_Refrence_Offset_Address
""")
#display(Final_Policy_Claim)


# STAT 83 update

# In[45]:


# Coverage Extension Change Logic
# IBMI & Origami Data fix
# 1.1 IBMI Policies — Coverage Extension Updates
# CBI --> BI only applies for the listed coverages
# Existing extension preserved if conditions don’t match
from pyspark.sql.functions import when, col

df = Final_Policy_Claim.withColumn(
    "CoverageExtension",
    when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UPD DED"), "UPD")
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "PIP LIMIT"), "NF")
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "BI"), "CBI")
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "BA PLUS"), "CM")
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "BROAD PIP"), "BI")
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode").isin(
        "FARMTR",
        "FRMTRKPOLL",
        "HIRED AUTO",
        "NONOWNAUTO",
        "DOC"
    )) & (col("CoverageExtension") == "CBI"), "BI")
    .otherwise(col("CoverageExtension"))
)


# In[46]:


# 1.2 Origami Policies — Coverage Extension Updates
df = df.withColumn(
    "CoverageExtension",
    when(
        (col("PolicySource") == "Origami") &
        (col("CoverageCode") == "PIP LIMIT"),
        "NF"
    ).otherwise(col("CoverageExtension"))
)


# In[47]:


#  IBMI Policies — Coverage Group Updates
df = df.withColumn(
    "CoverageGroup",
    when(
        (col("PolicySource") == "IBMi") &
        (col("CoverageCode") == "BA PLUS"),
        "Property"
    )
    .when(
        (col("PolicySource") == "IBMi") &
        (col("CoverageCode") == "UPD DED"),
        "Liability"
    )
    .otherwise(col("CoverageGroup"))
)
###################################################################################

#ASLOB Change Logic
Final_Policy_Claim = df.withColumn(
    "ASLOB",
    when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "BA PLUS"), 212)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "BROAD PIP"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "BI"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "BLNKT AI"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "DOC"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "FARMTR"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "FRMTRKPOLL"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "GK COLL"), 212)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "GK COMP"), 212)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "HIRED AUTO"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "HIRED PHYS"), 212)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "NONOWNAUTO"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "PIP LIMIT"), 193)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UBI HIRED"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UBI NONOWN"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UIM HIRED"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UIM NONOWN"), 194)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UPD DED"), 194)
    .otherwise(col("ASLOB"))
)


# In[48]:


aslob_manual_update = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/ASOLB_Manual_update.csv")


# In[49]:


from pyspark.sql.functions import col, when

join_cols = ["PolicySource", "CoverageCode", "CoverageExtension"]

updated_df = Final_Policy_Claim.alias("f").join(
    aslob_manual_update.select(*join_cols, col("Correct ASLOB").alias("ASLOB_new")).alias("m"),
    on=join_cols,
    how="left"
)

Final_Policy_Claim = updated_df.select(
    *[
        when(col("f.ASLOB").isNull(), col("m.ASLOB_new"))
        .otherwise(col("f.ASLOB"))
        .alias("ASLOB") if c == "ASLOB" else col(f"f.{c}")
        for c in Final_Policy_Claim.columns
    ]
)


# In[50]:


from pyspark.sql.functions import col, when

Final_Policy_Claim = Final_Policy_Claim.withColumn(
    "ASLOB",
    when((col("ASLOB").isNull()) & (col("PolicySource") == "Origami") & (col("CoverageCode") == "BODINJLIAB"), 194)
    .when((col("ASLOB").isNull()) & (col("PolicySource") == "Origami") & (col("CoverageCode") == "COLLISION"), 212)
    .when((col("ASLOB").isNull()) & (col("PolicySource") == "Origami") & (col("CoverageCode") == "UIM"), 194)
    .when((col("ASLOB").isNull()) & (col("PolicySource") == "Origami") & (col("CoverageCode") == "COMB-FPB"), 193)
    .when((col("ASLOB").isNull()) & (col("PolicySource") == "Origami") & (col("CoverageCode") == "MED EXP"), 193)
    .when((col("ASLOB").isNull()) & (col("PolicySource") == "Origami") & (col("CoverageCode") == "OTC"), 212)
    .when((col("ASLOB").isNull()) & (col("PolicySource") == "Origami") & (col("CoverageCode") == "UBI"), 194)
    .otherwise(col("ASLOB"))
)


# BA PLUS ASLOB Correction

# In[51]:


Updated_BA_PLUS_ASLOB = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/BA PLUS-UpdateASLOB.csv")

from pyspark.sql.functions import col, when, lit

# -------------------------
# 1. Prepare lookup (distinct to avoid duplicate explosion)
# -------------------------
lookup_df = Updated_BA_PLUS_ASLOB.select(
    col("PolicyNumber"),
    col("PolicyVersion"),
    col("CoverageCode")
).dropDuplicates()

# -------------------------
# 2. Left join (safe)
# -------------------------
joined_df = Final_Policy_Claim.join(
    lookup_df.withColumn("match_flag", lit(1)),
    on=[
        "PolicyNumber",
        "PolicyVersion",
        "CoverageCode"
    ],
    how="left"
)

# -------------------------
# 3. Update ASLOB based on match
# -------------------------
Final_Policy_Claim = joined_df.withColumn(
    "ASLOB",
    when((col("match_flag") == 1) & (col("ClaimNumber").isNull()), lit(194))
    .otherwise(col("ASLOB"))
).drop("match_flag")

# -------------------------
# 4. Validation (must match)
# -------------------------
print("Before:", Final_Policy_Claim.count())
print("After :", Final_Policy_Claim.count())


# In[52]:


# ZIP Manual Update
Zip_manual_update = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/Missing_ZIP_Code.csv")

from pyspark.sql.functions import col, when

# -------------------------------
# 1. Prepare lookup dataframe
# -------------------------------
zip_df = (
    Zip_manual_update
    .select(
        col("InTransactionId").alias("ZIP_InTransactionId"),
        col("ZIP Code").alias("ZIP_From_Manual")
    )
    # Prevent duplicate explosion
    .dropDuplicates(["ZIP_InTransactionId"])
)

# -------------------------------
# 2. LEFT JOIN (strict count preservation)
# -------------------------------
joined_df = Final_Policy_Claim.join(
    zip_df,
    Final_Policy_Claim["InTransactionId"] == zip_df["ZIP_InTransactionId"],
    "left"
)

# -------------------------------
# 3. Conditional update
# -------------------------------
final_df = joined_df.withColumn(
    "ZipCode",
    when(
        col("ZipCode").isNull(),
        col("ZIP_From_Manual")
    ).otherwise(col("ZipCode"))
)

# -------------------------------
# 4. Cleanup
# -------------------------------
final_df = final_df.drop("ZIP_InTransactionId", "ZIP_From_Manual")

# -------------------------------
#  Validation (as per your practice)
# -------------------------------
print("Base Count  :", Final_Policy_Claim.count())
print("Final Count :", final_df.count())


# In[53]:


from pyspark.sql.functions import when, col

final_df = final_df.withColumn(
    "ZipCode",
    when(col("InTransactionId") == 2830107, 45356)
    .otherwise(col("ZipCode"))
)


# In[54]:


# Incorrect ZIP fix

Zip_Further_fix = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/ZIP_Further_fix.csv")


from pyspark.sql.functions import col, when

# -------------------------------
# 1. Prepare Fix Lookup
# -------------------------------
fix_df = (
    Zip_Further_fix
    .select(
        col("InTransactionId").alias("FIX_InTransactionId"),
        col("CoverageId").alias("FIX_CoverageId"),
        col("ZIP Code").alias("ZIP_Fix_Value")
    )
    #  Prevent duplicate explosion (CRITICAL)
    .dropDuplicates(["FIX_InTransactionId", "FIX_CoverageId"])
)

# -------------------------------
# 2. LEFT JOIN on TWO KEYS
# -------------------------------
joined_df = final_df.join(
    fix_df,
    (final_df["InTransactionId"] == fix_df["FIX_InTransactionId"]) &
    (final_df["CoverageId"] == fix_df["FIX_CoverageId"]),
    "left"
)

# -------------------------------
# 3. Overwrite ZipCode (only if fix exists)
# -------------------------------
updated_df = joined_df.withColumn(
    "ZipCode",
    when(
        col("ZIP_Fix_Value").isNotNull(),
        col("ZIP_Fix_Value")   #  overwrite with correct
    ).otherwise(col("ZipCode"))  #  fallback to existing
)

# -------------------------------
# 4. Cleanup
# -------------------------------
updated_df = updated_df.drop(
    "FIX_InTransactionId",
    "FIX_CoverageId",
    "ZIP_Fix_Value"
)

# -------------------------------
# Validation
# -------------------------------
print("Before Count :", final_df.count())
print("After Count  :", updated_df.count())


# In[55]:


# check =updated_df[updated_df['InTransactionId']==2830107]
# display(check)


# Territory Code Mapping

# In[56]:


# Reading the csv files
Territory_code = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/Territory_code_2026.csv")

#display(Territory_code)
################################ 
# Segragating the required states
states = ['DE', 'IL', 'IN', 'KS', 'OH', 'OK', 'PA', 'VA']

filtered_df = Territory_code[
    Territory_code['StateAbbrev'].isin(states)
]
#display(filtered_df)
#########################################################

from pyspark.sql import functions as F

# Allowed states
allowed_states = ['DE', 'IL', 'IN', 'KS', 'OH', 'OK', 'PA', 'VA']

# LEFT JOIN to preserve row count
Final_Policy_Claim_final = (
    updated_df.alias("f")
    .join(
        filtered_df.select("ZIP", "StateAbbrev", "Territory").alias("t"),
        (F.col("f.ZipCode") == F.col("t.ZIP")) &
        (F.col("f.PrimaryRiskState") == F.col("t.StateAbbrev")),
        how="left"
    )
    # Populate TerritoryCode only for allowed states
    .withColumn(
        "TerritoryCode",
        F.when(
            F.col("PrimaryRiskState").isin(allowed_states),
            F.col("Territory")
        ).otherwise(F.lit(None))
    )
    # Drop lookup columns
    .drop("ZIP", "StateAbbrev", "Territory")
)


# In[57]:


# Final_Policy_Claim_final.count()


# In[58]:


# check =Final_Policy_Claim_final[Final_Policy_Claim_final['InTransactionId']==2830107]
# display(check)


# In[59]:


# display(Final_Policy_Claim_final[Final_Policy_Claim_final['TerritoryCode'].isNull()])


# Adding HasPIPVUWCCoveredInd

# In[60]:


# For IBMi Policies (Rating Basis Part)
from pyspark.sql import functions as F

# =========================================================
# STEP 1 — Valid Keys
# =========================================================
valid_keys_df = (
    Final_Policy_Claim_final
    .filter(
        (F.col("PolicySource") == "IBMi") &
        (F.col("PrimaryRiskState") == "PA") &
        (F.col("CoverageExtension").isin("NF", "FPB")) &
        (F.col("RiskItemId").isNotNull())
    )
    .select("InTransactionId", "RiskItemId")
    .distinct()
)

# =========================================================
# STEP 2 — RB Value
# =========================================================
rb_df = (
    spark.table("ODS_STATS_TBLS_LH.policy_question_answer")
    .filter(F.col("QuestionCode") == "RatingBasis")
    .select(
        F.col("InTransactionID").alias("InTransactionId"),
        F.col("RiskItemID").alias("RiskItemId"),
        F.col("TextValue").cast("int").alias("RB_TextValue")
    )
)

# Only valid combinations
valid_rb_df = (
    valid_keys_df
    .join(rb_df, ["InTransactionId", "RiskItemId"], "inner")
)

# =========================================================
# STEP 3 — JOIN BACK ( REMOVE DUPLICATES IMMEDIATELY)
# =========================================================
df = (
    Final_Policy_Claim_final
    .join(valid_rb_df, ["InTransactionId", "RiskItemId"], "left")
    .select(
        Final_Policy_Claim_final["*"],   #  TAKE ONLY ORIGINAL COLUMNS
        F.col("RB_TextValue")      #  ADD ONLY THIS
    )
)

# =========================================================
# STEP 4 — APPLY MAPPING (NO NULL, NO AMBIGUITY)
# =========================================================
from pyspark.sql import functions as F

df = df.withColumn(
    "HasPIPVUWCCoveredInd",
    F.when(
        (F.col("CoverageCode") == "MED EXP") & (F.col("RB_TextValue") == 3), 1
    ).when(
        (F.col("CoverageCode") == "INC LOSS") & (F.col("RB_TextValue") == 3), 1
    ).when(
        (F.col("CoverageCode") == "ACC DEATH") & (F.col("RB_TextValue") == 3), 1
    ).when(
        (F.col("CoverageCode") == "FUN EXP") & (F.col("RB_TextValue") == 3), 1
    ).when(
        (F.col("CoverageCode") == "MED EXP") & (F.col("RB_TextValue").isin(0, 1, 2, 4)), 0
    ).when(
        (F.col("CoverageCode") == "ACC DEATH") & (F.col("RB_TextValue").isin(0, 1, 2, 4)), 0
    ).when(
        (F.col("CoverageCode") == "INC LOSS") & (F.col("RB_TextValue").isin(0, 1, 2, 4)), 0
    ).when(
        (F.col("CoverageCode") == "FUN EXP") & (F.col("RB_TextValue").isin(0, 1, 2, 4)), 0
    ).when(
        (F.col("CoverageCode") == "COMB-FPB") & (F.col("RB_TextValue") == 3), 1
    ).when(
        (F.col("CoverageCode") == "COMB-FPB") & (F.col("RB_TextValue").isin(1, 2, 4)), 0
    ).when(
        (F.col("CoverageCode") == "EMB") & (F.col("RB_TextValue").isin(1, 3)), 1
    ).when(
        (F.col("CoverageCode") == "EMB") & (F.col("RB_TextValue").isin(2, 4)), 0
    ).when(
        (F.col("CoverageCode") == "BROAD PIP"), 0
    ).otherwise(F.lit(None)) 
)

# =========================================================
# STEP 5 — FINAL CLEANUP
# =========================================================
Final_Policy_Claim_updated = df.drop("RB_TextValue")


# In[61]:


HasPIPVUWC_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/HasPIPVUWC_New.csv")

#display(HasPIPVUWC_df)


# In[62]:


from pyspark.sql.functions import col, coalesce

# Step 1: Prepare lookup with renamed column
haspip_lookup_df = (
    HasPIPVUWC_df.select(
        "PolicyNumber",
        "PolicyVersion",
        "InTransactionId",
        "RiskItemId",
        "CoverageExtension",
        col("HasPIPVUWCCoveredInd").alias("HasPIPVUWCCoveredInd_new")
    )
)

# Step 2: Join
joined_df = (
    Final_Policy_Claim_updated.alias("f")
    .join(
        haspip_lookup_df.alias("h"),
        [
            col("f.PolicyNumber") == col("h.PolicyNumber"),
            col("f.PolicyVersion") == col("h.PolicyVersion"),
            col("f.InTransactionId") == col("h.InTransactionId"),
            col("f.RiskItemId") == col("h.RiskItemId"),
            col("f.CoverageExtension") == col("h.CoverageExtension")
        ],
        "left"
    )
    .select(
        col("f.*"),
        col("h.HaspipVUWCcoveredInd_new")
    )
)

# Step 3: THIS IS THE IMPORTANT FIX
final_policy_claim_updated_df = (
    joined_df
    .withColumn(
        "HasPIPVUWCCoveredInd",
        coalesce(col("HasPIPVUWCCoveredInd_new"), col("HasPIPVUWCCoveredInd"))
    )
    .drop("HasPIPVUWCCoveredInd_new")
)


# In[63]:


final_policy_claim_updated_df= final_policy_claim_updated_df.dropDuplicates()


# In[64]:


from pyspark.sql.functions import col, when

final_policy_claim_updated_df = final_policy_claim_updated_df.withColumn(
    "CoverageExtension",
    when(
        (col("CoverageCode") == "BA PLUS") &
        (col("CoverageExtension").isin("CBI", "BI")),
        "CM"
    ).when(
        (col("CoverageCode") == "MED EXP") &
        (col("CoverageExtension").isNull() | (col("CoverageExtension") == "")),
        "NF"
    ).otherwise(col("CoverageExtension"))
)


# In[65]:


from pyspark.sql.functions import col, when, concat_ws

final_policy_claim_updated_df = final_policy_claim_updated_df.withColumn(
    "ClaimCode",
    when(
        col("ParentCoverageId").isNull() & col("ClaimCode").isNull(),
        concat_ws("__", col("CoverageCode"), col("CoverageExtension"))
    ).otherwise(col("ClaimCode"))
)


# In[66]:


from pyspark.sql.functions import col, when

final_policy_claim_updated_df = final_policy_claim_updated_df.withColumn(
    "Option1",
    when(
        (col("ClaimCode").isin("UBI__UBI", "UIM__UBI")) &
        col("Option1").isNull() &
        (col("PrimaryRiskState") == "PA"),
        "NS"
    ).otherwise(col("Option1"))
)


# In[67]:


# check =final_policy_claim_updated_df[final_policy_claim_updated_df['ClaimCode']=='UIM__UBI']
# display(check)


# In[68]:


from pyspark.sql.functions import col, when, concat_ws

valid_pairs = [
    "MED EXP_NF",
    "MED EXP_FPB",
    "COMB-FPB_NF",
    "COMB-FPB_FPB",
    "EMB_NF",
    "EMB_FPB",
    "BROAD PIP_NF",
    "BROAD PIP_FPB",
    "ACC DEATH_NF",
    "ACC DEATH_FPB",
    "FUN EXP_NF",
    "FUN EXP_FPB",
    "INC LOSS_NF",
    "INC LOSS_FPB"
]

final_policy_claim_updated_df = final_policy_claim_updated_df.withColumn(
    "pair",
    concat_ws("_", col("CoverageCode"), col("CoverageExtension"))
)

final_policy_claim_updated_df = final_policy_claim_updated_df.withColumn(
    "HasPIPVUWCCoveredInd",
    when(
        col("pair").isin(valid_pairs) &
        col("HasPIPVUWCCoveredInd").isNull(),
        0
    ).otherwise(col("HasPIPVUWCCoveredInd"))
)


# Reading Mapping File

# In[69]:


Mapping_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/NISS_Column_Mapping-Updated.csv")

#display(Mapping_df)


# In[70]:


from pyspark.sql.functions import col, when

Mapping_df = Mapping_df.withColumn(
    "Deductible",
    when(col("Deductible") == 0, None).otherwise(col("Deductible"))
)


# NISS Coverage Code Mapping

# In[71]:


###### CURRENTLY WORKING ON THIS CODE FOR NISSS

from pyspark.sql.functions import col, when, lpad, row_number, lit, coalesce, count, concat_ws
from pyspark.sql.window import Window
from pyspark.sql.functions import trim, upper

# =====================================================
# STEP 0: PREPARE FINAL POLICY DATA (NORMALIZATION)
# =====================================================


fp_clean = (
    final_policy_claim_updated_df
    # Clean Option1
    .withColumn(
        "Option1_clean",
        when(col("Option1").isin("NI", "NI&R", "NS", "ST"), col("Option1"))
        .otherwise(None)
    )
    # Apply Deductible1 logic
    .withColumn(
    "Deductible1",
    when(
        ~(
            (upper(trim(col("ClaimCode"))) == "OTC__CM") |
            (upper(trim(col("ClaimCode"))) == "COLLISION__CL") |
            (
                (upper(trim(col("ClaimCode"))) == "PIP LIMIT__NF") &
                (upper(trim(col("PrimaryRiskState"))) == "DE")
            )
        ),
        None
    ).otherwise(col("Deductible1"))
)
    # Clean Deductible1
    .withColumn(
        "Deductible1_clean",
        when(col("Deductible1") == 0.0, None)
        .otherwise(col("Deductible1"))
    )
)


# =====================================================
# STEP 1: CLEAN MAPPING DATA


Mapping_df = Mapping_df.withColumn(
    "NISS Coverage Code",
    lpad(col("NISS Coverage Code").cast("string"), 3, "0")
)

# =====================================================
# STEP 2: EFFECTIVE COVERAGE CODE
# =====================================================
mp2 = Mapping_df.withColumn(
    "EffectiveCoverageCode",
    coalesce(
        col("ODS Child Coverage Code"),
        col("Coverage Code")
    )
)

# =====================================================
# STEP 3: DEDUPLICATION
# =====================================================
window_spec = Window.partitionBy(
    "EffectiveCoverageCode",
    "State",
    "Coverage Extension",
    "Deductible",
    "Deductible Applies to Option1",
    "Covered By Worker's Compensation",
    "ClaimCode"
).orderBy(lit(1))

mp_dedup = (
    mp2
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# =====================================================
# STEP 4: SPLIT DATA
# =====================================================
fp_parent = fp_clean.filter(col("ClaimCode").isNull())
fp_child  = fp_clean.filter(col("ClaimCode").isNotNull())

print("FP Parent count :", fp_parent.count())
print("FP Child count  :", fp_child.count())

# =====================================================
#  NULL-SAFE COMPARISON FUNCTIONS INLINE
# =====================================================

# Option1 NULL-safe
option1_match = (
    (col("fp.Option1_clean") == col("mp.Deductible Applies to Option1")) |
    (col("fp.Option1_clean").isNull() & col("mp.Deductible Applies to Option1").isNull())
)

# Deductible NULL-safe
deductible_match = (
    (col("fp.Deductible1_clean") == col("mp.Deductible")) |
    (col("fp.Deductible1_clean").isNull() & col("mp.Deductible").isNull())
)

# WorkerComp NULL-safe
worker_match = (
    (col("fp.HasPIPVUWCCoveredInd").cast("int") == col("mp.Covered By Worker's Compensation").cast("int")) |
    (col("fp.HasPIPVUWCCoveredInd").isNull() & col("mp.Covered By Worker's Compensation").isNull())
)

# =====================================================
# STEP 5: PARENT JOIN (Coverage-based)
# =====================================================
parent_join = (
    fp_parent.alias("fp")
    .join(
        mp_dedup.alias("mp"),
        (col("fp.CoverageCode") == col("mp.EffectiveCoverageCode")) &
        (col("fp.PrimaryRiskState") == col("mp.State")) &
        (col("fp.CoverageExtension") == col("mp.Coverage Extension"))
        & option1_match
        & deductible_match
        & worker_match,
        "left"
    )
    .select(
        "fp.*",
        col("mp.NISS Coverage Code").alias("NISS_Coverage_Code"),
        col("mp.NISS Subline Code (1)").alias("NISS_Subline_Code"),
        col("mp.NISS Type of Loss (1)").alias("NISS_Type_of_Loss"),
        col("mp.NISS Class Codes (6)").alias("NISS_Class_Codes")
    )
)

print("Parent join count :", parent_join.count())

# =====================================================
# STEP 6: CHILD JOIN (ClaimCode-based)
# =====================================================
child_join = (
    fp_child.alias("fp")
    .join(
        mp_dedup.alias("mp"),
        (col("fp.ClaimCode") == col("mp.ClaimCode")) &
        (col("fp.PrimaryRiskState") == col("mp.State"))
        & option1_match
        & deductible_match
        & worker_match,
        "left"
    )
    .select(
        "fp.*",
        col("mp.NISS Coverage Code").alias("NISS_Coverage_Code"),
        col("mp.NISS Subline Code (1)").alias("NISS_Subline_Code"),
        col("mp.NISS Type of Loss (1)").alias("NISS_Type_of_Loss"),
        col("mp.NISS Class Codes (6)").alias("NISS_Class_Codes")
    )
)

print("Child join count  :", child_join.count())


# In[72]:


# For ClaimCode

from pyspark.sql.functions import col, when

child_join = child_join \
.withColumn(
    "NISS_Coverage_Code",
    when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UBI HIRED"), 201)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UBI NONOWN"), 201)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UIM HIRED"), 202)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UIM NONOWN"), 202)
    .otherwise(col("NISS_Coverage_Code"))
) \
.withColumn(
    "NISS_Subline_Code",
    when((col("PolicySource") == "IBMi") & col("CoverageCode").isin("UBI HIRED", "UBI NONOWN", "UIM HIRED", "UIM NONOWN"), 0)
    .otherwise(col("NISS_Subline_Code"))
) \
.withColumn(
    "NISS_Type_of_Loss",
    when((col("PolicySource") == "IBMi") & col("CoverageCode").isin("UBI HIRED", "UBI NONOWN", "UIM HIRED", "UIM NONOWN"), 0)
    .otherwise(col("NISS_Type_of_Loss"))
) \
.withColumn(
    "NISS_Class_Codes",
    when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UBI HIRED"), 9404)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UBI NONOWN"), 9408)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UIM HIRED"), 9404)
    .when((col("PolicySource") == "IBMi") & (col("CoverageCode") == "UIM NONOWN"), 9408)
    .otherwise(col("NISS_Class_Codes"))
)


# In[73]:


# BA PLUS CoverageExtension mismatch ISSUE
from pyspark.sql.functions import col, when, lit

condition = col("ClaimCode").isin("BA PLUS__CBI", "BA PLUS__BI")

child_join = child_join \
    .withColumn(
        "NISS_Coverage_Code",
        when(condition, lit(19)).otherwise(col("NISS_Coverage_Code"))
    ) \
    .withColumn(
        "NISS_Subline_Code",
        when(condition, lit(0)).otherwise(col("NISS_Subline_Code"))
    ) \
    .withColumn(
        "NISS_Type_of_Loss",
        when(condition, lit(None)).otherwise(col("NISS_Type_of_Loss"))
    ) \
    .withColumn(
        "NISS_Class_Codes",
        when(condition, lit(9999)).otherwise(col("NISS_Class_Codes"))
    )


# In[74]:


check =child_join[child_join['NISS_Coverage_Code'].isNull()]
display(check)


# In[75]:


# check.count()


# In[76]:


# Updating "Vehicle Class Code" with Actual Class code
from pyspark.sql.functions import when, col
final_enriched_df = child_join
final_enriched_df = final_enriched_df.withColumn(
    "NISS_Class_Codes",
    when(col("NISS_Class_Codes") == "Vehicle Class Code", col("ClassCode"))
    .otherwise(col("NISS_Class_Codes"))
)


# In[77]:


from pyspark.sql.functions import col, substring

final_enriched_df = final_enriched_df.withColumn(
    "NISS_Class_Codes",
    substring(col("NISS_Class_Codes"), 1, 4)
)


# In[78]:


# Replacing Correct NISS COde
from pyspark.sql.functions import col, when

Corrected_Class_code = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/Corrected_Class_code.csv")

# Rename columns in lookup DF to avoid ambiguity
cc_df = Corrected_Class_code.select(
    col("Class").alias("cc_Class"),
    col("NISS Code").alias("cc_NISS_Code")
)

# LEFT JOIN (count-safe if lookup is unique)
updated_df = final_enriched_df.alias("f").join(
    cc_df.alias("c"),
    col("f.NISS_Class_Codes") == col("c.cc_Class"),
    "left"
)

# Replace logic
updated_df = updated_df.withColumn(
    "NISS_Class_Codes",
    when(col("c.cc_NISS_Code").isNotNull(), col("c.cc_NISS_Code"))
    .otherwise(col("f.NISS_Class_Codes"))
)

# Drop helper columns
final_enriched_df = updated_df.drop("cc_Class", "cc_NISS_Code")
final_enriched_df.count()


# In[79]:


NissCauseofloss_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/NissCauseofloss_df.csv")
#display(NissCauseofloss_df)


# In[80]:


from pyspark.sql.functions import col, when

# Step 1: Prepare mapping dataframe (only required columns, avoid duplicates)
mapping_df = NissCauseofloss_df.select(
    col("Numeric Code").alias("map_Numeric_Code"),
    col("Cause Of Loss Code3").alias("map_Cause_Of_Loss_Code")
).dropDuplicates(["map_Numeric_Code"])

# Step 2: Join with final dataframe 
joined_df = final_enriched_df.join(
    mapping_df,
    final_enriched_df["CauseOfLoss"] == mapping_df["map_Numeric_Code"],
    "left"
)

# Step 3: Apply conditional update
updated_df = joined_df.withColumn(
    "NISS_Type_of_Loss",
    when(
        (col("ClaimNumber").isNotNull()) & 
        (col("NISS_Type_of_Loss").isNull()) &
        (col("map_Cause_Of_Loss_Code").isNotNull()),
        col("map_Cause_Of_Loss_Code")
    ).otherwise(col("NISS_Type_of_Loss"))
)

# Step 4: Drop helper columns (to avoid affecting other columns)
final_enriched_df = updated_df.drop("map_Numeric_Code", "map_Cause_Of_Loss_Code")


# In[81]:


from pyspark.sql.functions import col, when, lit

final_enriched_df = final_enriched_df.withColumn(
    "NISS_Type_of_Loss",
    when(col("ClaimNumber").isNull(), lit(None))
    .otherwise(col("NISS_Type_of_Loss"))
)


# Exception_code Mapping

# In[82]:


from pyspark.sql import functions as F

# =====================================================================
# STEP 0: Set lookup coverage codes
# =====================================================================
cov_codes = ["MED EXP", "ACC DEATH", "INC LOSS", "FUN EXP"]

# =====================================================================
# LOAD CoverageInfo FROM THE CORRECT LAKEHOUSE
# =====================================================================
coverage_info_df = spark.table("ODS_STATS_TBLS_LH.policy_coverage_info")

# =====================================================================
# STEP 1: Filter Final_Policy_Claim for MED EXP + NF/FPB + PA
# =====================================================================
medexp_keys = (
    final_enriched_df
        .filter(
            (F.col("CoverageCode") == "MED EXP") &
            (F.col("CoverageExtension").isin("NF", "FPB")) &
            (F.col("PrimaryRiskState") == "PA")
        )
        .select("InTransactionId", "RiskItemId", "CoverageId")
        .dropDuplicates()
)

# =====================================================================
# STEP 2a: RiskItem level CoverageInfo (NO underscores)
# =====================================================================
riskitem_df = (
    coverage_info_df.alias("c")
        .join(
            medexp_keys.alias("m"),
            (F.col("c.InTransactionId") == F.col("m.InTransactionId")) &
            (F.col("c.RiskItemId") == F.col("m.RiskItemId")),
            "inner"
        )
        .filter(F.col("c.CoverageCode").isin(cov_codes))
        .select(
            F.col("c.InTransactionId"),
            F.col("c.RiskItemId"),
            F.col("c.CoverageCode"),
            F.col("c.CoverageExtension"),
            F.col("c.Limit1"),
            F.col("c.Limit2"),
            F.col("c.CoverageId"),
            F.col("c.ParentCoverageId")
        )
)

# =====================================================================
# STEP 2b: Hierarchy lookup using CoverageId OR ParentCoverageId
# =====================================================================
hier_df = (
    coverage_info_df.alias("c")
        .join(
            medexp_keys.select("InTransactionId", "CoverageId").alias("m"),
            (F.col("c.InTransactionId") == F.col("m.InTransactionId")) &
            (
                (F.col("c.CoverageId") == F.col("m.CoverageId")) |
                (F.col("c.ParentCoverageId") == F.col("m.CoverageId"))
            ),
            "inner"
        )
        .filter(F.col("c.CoverageCode").isin(cov_codes))
        .select(
            F.col("c.InTransactionId"),
            F.col("c.CoverageCode"),
            F.col("c.OnsetAmount"),
            F.col("c.OffsetAmount")
        )
)

# =====================================================================
# STEP 2c: Merge hierarchy values
# =====================================================================
full_limits = (
    riskitem_df.alias("r")
        .join(
            hier_df.alias("h"),
            (F.col("r.InTransactionId") == F.col("h.InTransactionId")) &
            (F.col("r.CoverageCode") == F.col("h.CoverageCode")),
            "left"
        )
        .select(
            # keep ONLY r side keys (remove ambiguity)
            F.col("r.InTransactionId").alias("InTransactionId"),
            F.col("r.RiskItemId").alias("RiskItemId"),
            F.col("r.CoverageCode"),
            F.col("r.CoverageExtension"),
            F.col("r.Limit1"),
            F.col("r.Limit2"),
            F.col("r.CoverageId"),
            F.col("r.ParentCoverageId"),

            # bring hierarchy amounts
            F.col("h.OnsetAmount"),
            F.col("h.OffsetAmount")
        )
)
# =====================================================================
# STEP 2d: Pivot limits to one row per transaction/risk pair
# =====================================================================
pivot_limits = (
    full_limits
        .groupBy("InTransactionId", "RiskItemId")
        .pivot("CoverageCode", cov_codes)
        .agg(
            F.first("Limit1").alias("Limit1"),
            F.first("Limit2").alias("Limit2")
        )
)

# =====================================================================
# STEP 2e: Rename pivot columns EXACTLY as CSV (NO UNDERSCORES)
# =====================================================================
pivot_renamed = (
    pivot_limits
        .withColumnRenamed("MED EXP_Limit1",  "MED EXP Limit1")
        .withColumnRenamed("ACC DEATH_Limit1","ACC DEATH Limit1")
        .withColumnRenamed("FUN EXP_Limit1",  "FUN EXP Limit1")
        .withColumnRenamed("INC LOSS_Limit1", "INC LOSS Limit1")
        .withColumnRenamed("INC LOSS_Limit2", "INC LOSS Limit2")
)

# =====================================================================
# STEP 3: Read exception mapping CSV from Fabric (NO underscores)
# =====================================================================
exception_code_mapping = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("Files/Exception_code_Mapping.csv")
)

# =====================================================================
# STEP 3b: Normalize Any / All Other (AO)
# =====================================================================
map_df = (
    exception_code_mapping
        .withColumn("MED EXP Limit1",
            F.when(F.col("MED EXP Limit1")=="Any",      "Any")
             .when(F.col("MED EXP Limit1")=="All Other","AO")
             .otherwise(F.col("MED EXP Limit1").cast("int"))
        )
        .withColumn("ACC DEATH Limit1",
            F.when(F.col("ACC DEATH Limit1")=="Any",      "Any")
             .when(F.col("ACC DEATH Limit1")=="All Other","AO")
             .otherwise(F.col("ACC DEATH Limit1").cast("int"))
        )
        .withColumn("FUN EXP Limit1",
            F.when(F.col("FUN EXP Limit1")=="Any",      "Any")
             .when(F.col("FUN EXP Limit1")=="All Other","AO")
             .otherwise(F.col("FUN EXP Limit1").cast("int"))
        )
        .withColumn("INC LOSS Limit1",
            F.when(F.col("INC LOSS Limit1")=="Any",      "Any")
             .when(F.col("INC LOSS Limit1")=="All Other","AO")
             .otherwise(F.col("INC LOSS Limit1").cast("int"))
        )
        .withColumn("INC LOSS Limit2",
            F.when(F.col("INC LOSS Limit2")=="Any",      "Any")
             .when(F.col("INC LOSS Limit2")=="All Other","AO")
             .otherwise(F.col("INC LOSS Limit2").cast("int"))
        )
)

# =====================================================================
# STEP 3c: Prepare pivot dataset for comparison (all ints)
# =====================================================================
src = (
    pivot_renamed
        .withColumn("MED EXP Limit1",  F.col("MED EXP Limit1").cast("int"))
        .withColumn("ACC DEATH Limit1",F.col("ACC DEATH Limit1").cast("int"))
        .withColumn("FUN EXP Limit1",  F.col("FUN EXP Limit1").cast("int"))
        .withColumn("INC LOSS Limit1", F.col("INC LOSS Limit1").cast("int"))
        .withColumn("INC LOSS Limit2", F.col("INC LOSS Limit2").cast("int"))
)

# =====================================================================
# STEP 4: Sequential narrowing match condition
# =====================================================================
def cond(col_src, col_map):
    return (
        (col_map == "Any") |
        (col_map == "AO")  |
        (col_map == col_src.cast("string")) |
        (col_map.cast("int") == col_src)
    )

# =====================================================================
# STEP 4a: Find best matching exception code
# =====================================================================
matched = (
    src.alias("s")
        .join(
            map_df.alias("m"),
            cond(F.col("s.`MED EXP Limit1`"),    F.col("m.`MED EXP Limit1`")) &
            cond(F.col("s.`ACC DEATH Limit1`"),  F.col("m.`ACC DEATH Limit1`")) &
            cond(F.col("s.`FUN EXP Limit1`"),    F.col("m.`FUN EXP Limit1`")) &
            (
                cond(F.col("s.`INC LOSS Limit1`"), F.col("m.`INC LOSS Limit1`")) |
                cond(F.col("s.`INC LOSS Limit2`"), F.col("m.`INC LOSS Limit2`"))
            ),
            "left"
        )
        .select(
            "s.InTransactionId",
            "s.RiskItemId",
            F.col("m.`NISS Exception Code`").alias("NISS_Exception_Code")
        )
        .dropDuplicates(["InTransactionId", "RiskItemId"])
)

# =====================================================================
# FINAL STRICT LEFT JOIN – Preserves ROW COUNT exactly
# =====================================================================
Final_with_Exception = (
    final_enriched_df.alias("f")
        .join(
            matched.alias("x"),
            ["InTransactionId", "RiskItemId"],
            "left"
        )
)

#Final_with_Exception


# In[83]:


from pyspark.sql.functions import col, when

mapping_expr = when(col("CoverageCode") == "EMB",
    when(col("Limit1") == 100000, "01")
    .when(col("Limit1") == 300000, "02")
    .when(col("Limit1") == 500000, "03")
    .when(col("Limit1") == 1000000, "04")
).when(col("CoverageCode") == "COMB-FPB",
    when(col("Limit1") == 12500, "06")
    .when(col("Limit1") == 17500, "01")
    .when(col("Limit1") == 50000, "02")
    .when(col("Limit1") == 100000, "03")
    .when(col("Limit1") == 177500, "07")
    .when(col("Limit1") == 200000, "04")
    .when(col("Limit1") == 277500, "05")
    .otherwise("09")   
)

Final_with_Exception = Final_with_Exception.withColumn(
    "NISS_Exception_Code",
    mapping_expr.otherwise(col("NISS_Exception_Code"))
)


# In[84]:


Final_with_Exception = Final_with_Exception.withColumn(
    "NISS_Exception_Code",
    F.lpad(F.col("NISS_Exception_Code").cast("string"), 2, "0")
)


# In[85]:


from pyspark.sql.functions import col, when

Final_with_Exception = Final_with_Exception.withColumn(
    "NISS_Exception_Code",
    when(
        (
            col("CoverageCode").isin(
                "COMB-FPB",
                "EMB",
                "MED EXP",
                "ACC Death",
                "FUN EXP"
            )
        )
        | (
            (col("CoverageCode") == "INC LOSS") & 
            (col("CoverageExtension") == "NF")
        ),
        col("NISS_Exception_Code")
    ).otherwise(None)
)


# # Exposure

# In[86]:


from pyspark.sql.functions import col, when, lit, trim

Final_with_Exception = Final_with_Exception.withColumn(
    "ProrataPercent",
    when(
        (col("PolicySource") == "Origami") &
        (col("ClaimNumber").isNull()) &
        (
            col("ProrataPercent").isNull() |
            (trim(col("ProrataPercent")) == "") |
            (col("ProrataPercent") == "null")
        ),
        lit(1)
    ).otherwise(col("ProrataPercent"))
)


# In[87]:


from pyspark.sql import functions as F

df = Final_with_Exception

Final_with_Exception = df.withColumn(
    "Premium",
    F.when(F.col("OnSetAmount") != 0, F.col("OnSetAmount"))
     .otherwise(F.col("OffsetAmount"))
).withColumn(
    "Raw_Exposure",
    (F.col("ProrataPercent")) * 12
).withColumn(
    "Rounded_Exposure",
    F.when(
        (F.col("Raw_Exposure") - F.floor(F.col("Raw_Exposure"))) >= 0.5,
        F.ceil(F.col("Raw_Exposure"))
    ).otherwise(F.floor(F.col("Raw_Exposure")))
).withColumn(
    "Final_Exposure",
    F.when(F.col("Premium") < 0, -F.col("Rounded_Exposure"))
     .otherwise(F.col("Rounded_Exposure"))
).drop(
    "Premium",
    "Raw_Exposure",
    "Rounded_Exposure"
)


# In[88]:


from pyspark.sql.functions import col, when

Final_with_Exception = Final_with_Exception.withColumn(
    "PrimaryRiskState",
    when(col("PrimaryRiskState") == "DE", "07")
    .when(col("PrimaryRiskState") == "IL", "12")
    .when(col("PrimaryRiskState") == "IN", "13")
    .when(col("PrimaryRiskState") == "KS", "15")
    .when(col("PrimaryRiskState") == "OH", "34")
    .when(col("PrimaryRiskState") == "OK", "35")
    .when(col("PrimaryRiskState") == "PA", "37")
    .when(col("PrimaryRiskState") == "VA", "45")
    .otherwise(col("PrimaryRiskState"))  # keep original if not listed
)


# In[89]:


from pyspark.sql import functions as F

Final_with_Exception = Final_with_Exception.withColumn(
    "CoverageCode",
    F.when(
        (F.col("ClaimNumber") == 633662) &
        (F.col("ClaimCoverageName") == "BA PLUS_RR"),
        "BA PLUS"
    ).otherwise(F.col("CoverageCode"))
)


# Data FIX

# In[90]:


#  Fix variable name (no hyphens)
DataFix_ForOffsetRecordsWithoutReferenceData = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("Files/DataFix-ForOffsetRecordsWithoutReferenceData.csv")
)

from pyspark.sql.functions import col, when

# Key columns for matching
join_cols = [
    "InTransactionId",
    "PolicyNumber",
    "PolicyVersion",
    "PrimaryRiskState",
    "CoverageCode",
    "OffsetAmount"
]

# Alias dataframes
f2_alias = Final_with_Exception.alias("f2")
f1_alias = DataFix_ForOffsetRecordsWithoutReferenceData.alias("f1")

# Join
joined_df = f2_alias.join(f1_alias, on=join_cols, how="left")

# Columns to update (only those present in f1 excluding join keys)
update_cols = [c for c in DataFix_ForOffsetRecordsWithoutReferenceData.columns if c not in join_cols]

# Rebuild dataframe
f2_updated = joined_df.select(
    *[
        when(col(f"f1.{c}").isNotNull(), col(f"f1.{c}"))
        .otherwise(col(f"f2.{c}"))
        .alias(c) if c in update_cols
        else col(f"f2.{c}")
        for c in Final_with_Exception.columns
    ]
)

# Final result
Final_with_Exception = f2_updated


# In[91]:


from pyspark.sql.functions import col, when
#  Fix variable name (no hyphens)
DataFix_ForOffsetRecordsWithoutReferenceData_claim = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("Files/DataFix-Claims.csv")
)
# -------------------------
# 1. Define claim-level join keys
# -------------------------
claim_join_cols = [
    "ClaimNumber",
    "InTransactionId",
    "CoverageCode"
]

# -------------------------
# 2. Alias dataframes
# -------------------------
f2_alias = Final_with_Exception.alias("f2")
f1_alias = DataFix_ForOffsetRecordsWithoutReferenceData_claim.alias("f1")

# -------------------------
# 3. Join ONLY on claim keys
# -------------------------
joined_df = f2_alias.join(
    f1_alias,
    on=claim_join_cols,
    how="left"
)

# -------------------------
# 4. Identify columns to update
# -------------------------
update_cols = [
    c for c in DataFix_ForOffsetRecordsWithoutReferenceData_claim.columns
    if c not in claim_join_cols
]

# -------------------------
# 5. Rebuild dataframe (ONLY update matching CLAIM rows)
# -------------------------
f2_updated = joined_df.select(
    *[
        when(
            (col("f2.ClaimNumber").isNotNull()) & 
            (col(f"f1.{c}").isNotNull()),
            col(f"f1.{c}")
        ).otherwise(col(f"f2.{c}")).alias(c)
        if c in update_cols
        else col(f"f2.{c}")
        for c in Final_with_Exception.columns
    ]
)

# -------------------------
# 6. Final assignment
# -------------------------
Final_with_Exception = f2_updated


# In[92]:


from pyspark.sql.functions import col, when, lit

Final_with_Exception = Final_with_Exception.withColumn(
    "NISS_Exception_Code",

    when(
        (col("InTransactionId") == 3203888) &
        (col("RiskItemId") == 2174291) &
        (col("PolicyNumber") == "GMCA100000183") &
        (col("PolicyVersion") == 1824) &
        (col("CoverageId") == 17000057) &
        (col("ClaimCode") == "INC LOSS__NF"),
        lit("53")
    )
    .when(
        (col("InTransactionId") == 3276779) &
        (col("RiskItemId") == 2216881) &
        (col("PolicyNumber") == "GMCA001138996") &
        (col("PolicyVersion") == 2215) &
        (col("CoverageId") == 17390445) &
        (col("ClaimCode") == "INC LOSS__NF"),
        lit("53")
    )
    .when(
        (col("InTransactionId") == 156722) &
        (col("RiskItemId") == 92935) &
        (col("PolicyNumber") == "135096") &
        (col("PolicyVersion") == 12) &
        (col("CoverageId") == 653539) &
        (col("ClaimCode") == "INC LOSS__NF"),
        lit("52")
    )

    # Preserve existing values
    .otherwise(col("NISS_Exception_Code"))
)


# In[93]:


# Final Table for Reporting logic
spark.sql("DROP TABLE IF EXISTS Final_Transformed_Data")
Final_with_Exception.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("Final_Transformed_Data")


# # Report Generation

# In[94]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# CREATE TABLE IF NOT EXISTS Constant (
#     ColumnName    STRING,
#     ConstantValue STRING
# )
# USING DELTA;


# In[95]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql
# INSERT INTO Constant (ColumnName, ConstantValue)
# VALUES
#     ('CompanyNumber', '526'),
#     ('LineCode', '1'),
#     ('CommercialIndicatorCode', '9'),
#     ('ClaimantLevelIndicator', 'C');


# In[96]:


# -------------------------------
# STEP 0: Load Constant Table
# -------------------------------
const_df = spark.table("Constant")

constant_dict = {
    row["ColumnName"]: row["ConstantValue"]
    for row in const_df.collect()
}

# -------------------------------
# STEP 1: Load Final Data
# -------------------------------
df = spark.table("Final_Transformed_Data")
from pyspark.sql.functions import lit, col, lpad, rpad, year, substring , right

###############################################################################
# INDEX 1 — Blank_1 (Length 1)
###############################################################################
df = df.withColumn("Blank_1", lit(None))
df = df.withColumn("Blank_1", rpad(col("Blank_1"), 1, " "))

###############################################################################
# INDEX 2 — CompanyNumber (Length 3) — From Constant Table
###############################################################################
df = df.withColumn("CompanyNumber", lit(constant_dict["CompanyNumber"]))
df = df.withColumn("CompanyNumber", rpad(col("CompanyNumber"), 3, " "))

###############################################################################
# INDEX 3 — Blank_5_6 (Length 2)
###############################################################################
df = df.withColumn("Blank_5_6", lit(None))
df = df.withColumn("Blank_5_6", rpad(col("Blank_5_6"), 2, " "))



###############################################################################
# INDEX 4 — CalendarYear (Length 2)
# Rule:
#   CalendarYear = last 2 digits of BookDate year (e.g., 2025 -> "25")
###############################################################################
#df = df.withColumn("CalendarYear",
#                   substring(year(col("BookDate")).cast("string"), 3, 2))
df = df.withColumn("CalendarYear",lit('25'))
# Right-pad if needed
df = df.withColumn("CalendarYear", lpad(col("CalendarYear"), 2, " "))


###############################################################################
# INDEX 5 — CallYear (Length 2)
# Rule:
#   CallYear = CalendarYear + 1  (e.g., 25 -> 26)
###############################################################################
#df = df.withColumn("CallYear",
#                   (substring(year(col("BookDate")).cast("string"), 3, 2).cast("int") + 1).cast("string"))
df = df.withColumn("CallYear",lit("26"))
# Ensure 2‑digit formatting (e.g., 09)
df = df.withColumn("CallYear", lpad(col("CallYear"), 2, "0"))
df = df.withColumn("CallYear", lpad(col("CallYear"), 2, " "))

###############################################################################
# INDEX 6 — StateCode (Length 2)
###############################################################################
df = df.withColumn("StateCode", col("PrimaryRiskState").cast("string"))
df = df.withColumn("StateCode", rpad(col("StateCode"), 2, " "))

###############################################################################
# INDEX 7 — LineCode (Length 1) — From Constant Table
###############################################################################
df = df.withColumn("LineCode", lit(constant_dict["LineCode"]))
df = df.withColumn("LineCode", rpad(col("LineCode"), 1, " "))

###############################################################################
# INDEX 8 — Blank_14_15 (Length 2)
###############################################################################
df = df.withColumn("Blank_14_15", lit(None))
df = df.withColumn("Blank_14_15", rpad(col("Blank_14_15"), 2, " "))

###############################################################################
# INDEX 9 — AccidentYear (Length 2)
###############################################################################
df = df.withColumn("AccidentYear", substring(col("AccidentYear").cast("string"),3,2))
# df = df.withColumn("AccidentYear", rpad(col("AccidentYear"), 2, " "))

###############################################################################
# INDEX 10 — Blank_18_22 (Length 5)
###############################################################################
df = df.withColumn("Blank_18_22", lit(None))
df = df.withColumn("Blank_18_22", rpad(col("Blank_18_22"), 5, " "))

###############################################################################
# INDEX 11 — CoverageCode (Length 3)
###############################################################################
df = df.withColumn("NISSCoverageCode", col("NISS_Coverage_Code").cast("string"))
df = df.withColumn("NISSCoverageCode", lpad(col("NISSCoverageCode"), 3, "0"))

###############################################################################
# INDEX 12 — Blank_26_30 (Length 5)
###############################################################################
df = df.withColumn("Blank_26_30", lit(None))
df = df.withColumn("Blank_26_30", rpad(col("Blank_26_30"), 5, " "))

###############################################################################
# INDEX 13 — TerritoryCode (Length 3)
###############################################################################
df = df.withColumn("TerritoryCode", col("TerritoryCode").cast("string"))
df = df.withColumn("TerritoryCode", lpad(col("TerritoryCode"), 3, "0"))

###############################################################################
# INDEX 14 — RatingZoneCode (Length 1) — Blank
###############################################################################
df = df.withColumn("RatingZoneCode", lit(None))
df = df.withColumn("RatingZoneCode", rpad(col("RatingZoneCode"), 1, " "))

###############################################################################
# INDEX 15 — TerminalZoneCode (Length 2) — Blank
###############################################################################
df = df.withColumn("TerminalZoneCode", lit(None))
df = df.withColumn("TerminalZoneCode", rpad(col("TerminalZoneCode"), 2, " "))
from pyspark.sql.functions import lit, col, rpad

###############################################################################
# INDEX 16 — ZIPCode (Length 5)
# Always blank but field must exist with correct length 5
###############################################################################
#df = df.withColumn("NSSZIPCode", lit(None))
#df = df.withColumn("ZIPCode", rpad(col("ZIPCode"), 5, " "))
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType

df = df.withColumn("NSSZIPCode", lit(None).cast(StringType()))

###############################################################################
# INDEX 17 — Blank_42_45 (Length 4)
###############################################################################
df = df.withColumn("Blank_42_45", lit(None))
df = df.withColumn("Blank_42_45", rpad(col("Blank_42_45"), 4, " "))

###############################################################################
# INDEX 18 — ClassificationCode (Length 6)
# Pull from Final_Transformed_Data.ClassificationCode
###############################################################################
from pyspark.sql.functions import col, lpad

df = df.withColumn(
    "ClassificationCode",
    rpad(col("NISS_Class_Codes").cast("string"), 6, "0")
)


###############################################################################
# INDEX 19 — EligibilityPointsCode (Length 2)
# Always blank
###############################################################################
df = df.withColumn("EligibilityPointsCode", lit(None))
df = df.withColumn("EligibilityPointsCode", rpad(col("EligibilityPointsCode"), 2, " "))

###############################################################################
# INDEX 20 — Blank_54_55 (Length 2)
###############################################################################
df = df.withColumn("Blank_54_55", lit(None))
df = df.withColumn("Blank_54_55", rpad(col("Blank_54_55"), 2, " "))

###############################################################################
# INDEX 21 — AgeGroupCode (Length 1)
# Always blank
###############################################################################
df = df.withColumn("AgeGroupCode", lit(None))
df = df.withColumn("AgeGroupCode", rpad(col("AgeGroupCode"), 1, " "))

###############################################################################
# INDEX 22 — ManufacturersModelYear (Length 2)
# Always blank
###############################################################################
df = df.withColumn("ManufacturersModelYear", lit(None))
df = df.withColumn("ManufacturersModelYear", rpad(col("ManufacturersModelYear"), 2, " "))

###############################################################################
# INDEX 23 — Blank_59_60 (Length 2)
###############################################################################
df = df.withColumn("Blank_59_60", lit(None))
df = df.withColumn("Blank_59_60", rpad(col("Blank_59_60"), 2, " "))

###############################################################################
# INDEX 24 — CommercialIndicatorCode (Length 1)
# Pull from ConstantTable["CommercialIndicatorCode"]
###############################################################################
df = df.withColumn("CommercialIndicatorCode",
                   lit(constant_dict["CommercialIndicatorCode"]))
df = df.withColumn("CommercialIndicatorCode",
                   rpad(col("CommercialIndicatorCode"), 1, " "))

###############################################################################
# INDEX 25 — ExceptionCode (Length 2)
# From Final_Transformed_Data.ExceptionCode
###############################################################################
df = df.withColumn("NISS_Exception_Code", col("NISS_Exception_Code").cast("string"))
df = df.withColumn("NISS_Exception_Code", lpad(col("NISS_Exception_Code"), 2, "0"))

###############################################################################
# INDEX 26 — ForgivenessCode (Length 1)
# Always blank
###############################################################################
df = df.withColumn("ForgivenessCode", lit(None))
df = df.withColumn("ForgivenessCode", rpad(col("ForgivenessCode"), 1, " "))

###############################################################################
# INDEX 27 — ClaimantLevelIndicator (Length 1)
# Pull from ConstantTable["ClaimantLevelIndicator"]
###############################################################################
df = df.withColumn("ClaimantLevelIndicator",
                   lit(constant_dict["ClaimantLevelIndicator"]))
df = df.withColumn("ClaimantLevelIndicator",
                   rpad(col("ClaimantLevelIndicator"), 1, " "))

###############################################################################
# INDEX 28 — PassiveRestraintCode (Length 2)
# Always blank
###############################################################################
df = df.withColumn("PassiveRestraintCode", lit(None))
df = df.withColumn("PassiveRestraintCode", rpad(col("PassiveRestraintCode"), 2, " "))

###############################################################################
# INDEX 29 — DefensiveDriverCreditCode (Length 1)
# Always blank
###############################################################################
df = df.withColumn("DefensiveDriverCreditCode", lit(None))
df = df.withColumn("DefensiveDriverCreditCode", rpad(col("DefensiveDriverCreditCode"), 1, " "))

###############################################################################
# INDEX 30 — Blank_69 (Length 1)
###############################################################################
df = df.withColumn("Blank_69", lit(None))
df = df.withColumn("Blank_69", rpad(col("Blank_69"), 1, " "))


# In[97]:


from pyspark.sql.functions import lit, col, rpad, lpad

###############################################################################
# INDEX 31 — AntiTheftDeviceCode (Length 1)
# Always Blank
###############################################################################
df = df.withColumn("AntiTheftDeviceCode", lit(None))
df = df.withColumn("AntiTheftDeviceCode", rpad(col("AntiTheftDeviceCode"), 1, " "))

###############################################################################
# INDEX 32 — DaytimeRunningLampsDiscountCode (Length 1)
# Always Blank
###############################################################################
df = df.withColumn("DaytimeRunningLampsDiscountCode", lit(None))
df = df.withColumn("DaytimeRunningLampsDiscountCode", rpad(col("DaytimeRunningLampsDiscountCode"), 1, " "))

###############################################################################
# INDEX 33 — Blank_72_74 (Length 3)
###############################################################################
df = df.withColumn("Blank_72_74", lit(None))
df = df.withColumn("Blank_72_74", rpad(col("Blank_72_74"), 3, " "))

###############################################################################
# INDEX 34 — PolicyLimitsCode (Length 2)
# Always Blank
###############################################################################
df = df.withColumn("PolicyLimitsCode", lit(None))
df = df.withColumn("PolicyLimitsCode", rpad(col("PolicyLimitsCode"), 2, " "))

###############################################################################
# INDEX 35 — DeductibleCode (Length 2)
#  Always Blank
###############################################################################
df = df.withColumn("DeductibleCode", lit(None))
df = df.withColumn("DeductibleCode", rpad(col("DeductibleCode"), 2, " "))

###############################################################################
# INDEX 36 — Blank_79 (Length 1)
###############################################################################
df = df.withColumn("Blank_79", lit(None))
df = df.withColumn("Blank_79", rpad(col("Blank_79"), 1, " "))

###############################################################################
# INDEX 37 — SupplementalSpousalLiabilityCode (Length 1)
#  Always Blank
###############################################################################
df = df.withColumn("SupplementalSpousalLiabilityCode", lit(None))
df = df.withColumn("SupplementalSpousalLiabilityCode", rpad(col("SupplementalSpousalLiabilityCode"), 1, " "))

###############################################################################
# INDEX 38 — Blank_81 (Length 1)
###############################################################################
df = df.withColumn("Blank_81", lit(None))
df = df.withColumn("Blank_81", rpad(col("Blank_81"), 1, " "))

###############################################################################
# INDEX 39 — SublineOfBusinessCode (Length 1)
# From Final_Transformed_Data.NissSublimeCode
###############################################################################
df = df.withColumn("SublineOfBusinessCode", col("NISS_Subline_Code").cast("string"))
df = df.withColumn("SublineOfBusinessCode", rpad(col("SublineOfBusinessCode"), 1, " "))

###############################################################################
# INDEX 40 — Blank_83 (Length 1)
###############################################################################
df = df.withColumn("Blank_83", lit(None))
df = df.withColumn("Blank_83", rpad(col("Blank_83"), 1, " "))

###############################################################################
# INDEX 41 — TypeOfLossCode (Length 1)
# From Final_Transformed_Data.NissTypeOfLoss
###############################################################################
df = df.withColumn("TypeOfLossCode", col("NISS_Type_of_Loss").cast("string"))
df = df.withColumn("TypeOfLossCode", rpad(col("TypeOfLossCode"), 1, " "))

###############################################################################
# INDEX 42 — LiabilityNoFaultCode (Length 1)
#  Always Blank
###############################################################################
df = df.withColumn("LiabilityNoFaultCode", lit(None))
df = df.withColumn("LiabilityNoFaultCode", rpad(col("LiabilityNoFaultCode"), 1, " "))

###############################################################################
# INDEX 43 — Blank_86_98 (Length 13)
###############################################################################
df = df.withColumn("Blank_86_98", lit(None))
df = df.withColumn("Blank_86_98", rpad(col("Blank_86_98"), 13, " "))

###############################################################################
# INDEX 44 — AnnualStatementLineOfBusinessCode (Length 3)
###############################################################################
df = df.withColumn("AnnualStatementLineOfBusinessCode", col("ASLOB").cast("string"))
df = df.withColumn("AnnualStatementLineOfBusinessCode", rpad(col("AnnualStatementLineOfBusinessCode"), 3, " "))

###############################################################################
# INDEX 45 — Blank_102_103 (Length 2)
###############################################################################
df = df.withColumn("Blank_102_103", lit(None))
df = df.withColumn("Blank_102_103", rpad(col("Blank_102_103"), 2, " "))

from pyspark.sql.functions import when, col, lpad, regexp_replace, abs
# NOW NUMERIC STRICT PADDING (46–52)
###############################################################################
# INDEX 46 — WrittenExposure (Length 8)
###############################################################################
df = df.withColumn("WrittenExposure", col("Final_Exposure"))

from pyspark.sql.functions import when, col, round, regexp_replace

###############################################################################
# INDEX 47 — WrittenPremium (Length 8)
# Final Optimized Logic:
#   1. Pick OnsetAmount if non-null & non-zero
#   2. Else pick OffsetAmount if non-null & non-zero
#   3. Else NULL (no default needed)
#   4. Remove commas
#   5. Apply round()
###############################################################################

df = df.withColumn(
    "WrittenPremium",
    when((col("OnsetAmount").isNotNull()) & (col("OnsetAmount") != 0), col("OnsetAmount"))
    .when((col("OffsetAmount").isNotNull()) & (col("OffsetAmount") != 0), col("OffsetAmount"))
)

# Remove commas (safety)
df = df.withColumn(
    "WrittenPremium",
    regexp_replace(col("WrittenPremium").cast("string"), ",", "").cast("double")
)

# Apply rounding
df = df.withColumn(
    "WrittenPremium",
    round(col("WrittenPremium"),0).cast('int')
)





from pyspark.sql.functions import col, round

###############################################################################
# INDEX 48 — PaidLosses
# Rule:
#   - Take value directly from PaidLossAmount
#   - Apply round() (numeric field)
###############################################################################
df = df.withColumn(
    "PaidLosses",
    round(col("PaidLossAmount").cast("double"),0).cast('int')
)

###############################################################################
# INDEX 49 — PaidAllocatedLossAdjustmentExpenses
# Rule:
#   - Take value from AllocatedLossAdjExp
#   - Apply round()
###############################################################################
df = df.withColumn(
    "PaidAllocatedLossAdjustmentExpenses",
    round(col("AllocatedLossAdjExp").cast("double"),0).cast('int')
)

###############################################################################
# INDEX 50 — OutstandingLossesIncludingALAE
# Rule:
#   - Take value from ReserveAmount
#   - Apply round()
###############################################################################
df = df.withColumn(
    "OutstandingLossesIncludingALAE",
    round(col("ReserveAmount").cast("double"),0).cast('int')
)

###############################################################################
# INDEX 51 — NumberOfPaidClaims
# Rule:
#   - Direct copy (count already integer)
###############################################################################
df = df.withColumn(
    "NumberOfPaidClaims",
    col("PaidCount").cast('int')
)

###############################################################################
# INDEX 52 — NumberOfOutstandingClaims
# Rule:
#   - Direct copy (count already integer)
###############################################################################
df = df.withColumn(
    "NumberOfOutstandingClaims",
    col("ReserveCount").cast('int')
)

###############################################################################
#  INDEX 53 — Blank 154–161 (Length 8)
###############################################################################
df = df.withColumn("ReservedForNISSUse", lit(None))
df = df.withColumn("ReservedForNISSUse", rpad(col("ReservedForNISSUse"), 8, " "))

###############################################################################
# INDEX 54 — ClaimNumberPremiumID (Length 16)
# Source: Final_Transformed_Data.ClaimNumber
# Rule:
#   1. Convert to string
#   2. If value > 16 chars → truncate to 16
#   3. If value < 16 chars → right-pad with spaces to 16
###############################################################################

from pyspark.sql.functions import col, when, concat_ws, rpad

###############################################################################
# INDEX 54 — ClaimNumberPremiumID (Length 16)
# Rule:
#   IF ClaimNumber is NOT NULL →
#       ClaimNumber_ClaimantNumber_CoverageCode
#   ELSE →
#       InTransactionId_CoverageId
#   Then:
#       1. Truncate to 16 characters
#       2. Right-pad with spaces to length 16
###############################################################################

# Step 1 — Build base value
df = df.withColumn(
    "ClaimNumberPremiumID",
    when(
        col("ClaimNumber").isNotNull(),
        concat_ws("_",
                  col("ClaimNumber").cast("string"),
                  col("ClaimantNumber").cast("string"),
                  col("CoverageCode").cast("string"))
    ).otherwise(
        concat_ws("_",
                  col("InTransactionId").cast("string"),
                  col("CoverageId").cast("string"))
    )
)

# Step 2 — Truncate to first 16 characters
df = df.withColumn(
    "ClaimNumberPremiumID",
    col("ClaimNumberPremiumID").substr(1, 16)
)

# Step 3 — Right-pad to ensure fixed length = 16
df = df.withColumn(
    "ClaimNumberPremiumID",
    rpad(col("ClaimNumberPremiumID"), 16, " ")
)


###############################################################################
# INDEX 55 — ClaimantNumber (Length 3)
# Source: Final_Transformed_Data.ClaimantNumber
# Rule:
#   1. Convert to string
#   2. Truncate to 3 chars if >3
#   3. Left-pad with zeros if <3
###############################################################################

# Step 1: Cast to string
df = df.withColumn("ClaimantNumber", col("ClaimantNumber").cast("string"))

# Step 2: Truncate to 3 characters if longer
df = df.withColumn("ClaimantNumber", col("ClaimantNumber").substr(1, 3))

# Step 3: Left-pad with zeros if shorter
df = df.withColumn("ClaimantNumber", lpad(col("ClaimantNumber"), 3, "0"))
from pyspark.sql import functions as F
df = df.withColumn(
    "NISS_Exception_Code",
    F.lpad(F.col("NISS_Exception_Code").cast("string"), 2, "0")
)


# In[98]:


from pyspark.sql.functions import col, substring, when

# -------------------------
# Step 1: Get distinct 3-char values from lookup DF
ClassesWithClaimInd_9 = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("Files/ClassesWithClaimInd_9.csv")
)
ClassesWithClaimInd_9 = ClassesWithClaimInd_9.withColumn(
    "Class Codes - first 3 characters",
    F.lpad(F.col("Class Codes - first 3 characters").cast("string"), 3, "0")
)
# -------------------------
valid_codes = [row[0] for row in ClassesWithClaimInd_9.select("Class Codes - first 3 characters").distinct().collect()]

# -------------------------
# Step 2: Apply logic
# -------------------------
df = df.withColumn(
    "CommercialIndicatorCode",
    
    when(
        substring(col("ClassificationCode"), 1, 3).isin(valid_codes),
        col("CommercialIndicatorCode")   # keep original
    ).otherwise(None)                   # set NULL
)
from pyspark.sql.functions import col, when

# List of ClassificationCodes
codes_to_null = [
    "608000","604000","580000","600100","600300","600500","600900",
    "648000","649000","780000","790000","708000","940900","949500",
    "940600","940500","940400","999900","940800","941100","941400",
    "941200","946400"
]

# Apply logic
df = df.withColumn(
    "WrittenExposure",
    when(
        col("ClassificationCode").isin(codes_to_null),
        None
    ).otherwise(col("WrittenExposure"))
)
from pyspark.sql.functions import col, when, lit

df = df.withColumn(
    "NISS_Exception_Code",
    
    when(
        (col("PrimaryRiskState") == "37") & (col("CoverageCode") == "BROAD PIP"),
        lit("49")
    ).otherwise(col("NISS_Exception_Code"))
)
from pyspark.sql import functions as F
df = df.withColumn(
    "NISS_Exception_Code",
    F.lpad(F.col("NISS_Exception_Code").cast("string"), 2, "0")
)
from pyspark.sql.functions import col, lit, sum as _sum, count, substring, year, current_date

# -------------------------
# 1. Derive Call Year (last 2 digits)
# -------------------------
call_year = str(2026)[-2:]   

# -------------------------
# 2. Aggregate per State
# -------------------------
transmittal_df = df.groupBy("PrimaryRiskState").agg(
    
    # Numeric aggregations
    _sum("WrittenExposure").alias("WrittenExposure"),
    _sum("WrittenPremium").alias("WrittenPremium"),
    _sum("PaidLosses").alias("PaidLosses"),
    _sum("PaidAllocatedLossAdjustmentExpenses").alias("PaidALAE"),
    _sum("OutstandingLossesIncludingALAE").alias("OutstandingLosses"),
    
    _sum("NumberOfPaidClaims").alias("NumPaidClaims"),
    _sum("NumberOfOutstandingClaims").alias("NumOutstandingClaims")
)

# -------------------------
# 3. Add Constant Columns
# -------------------------
transmittal_df = transmittal_df.select(
    
    # Field 1
    lit("T").alias("TransmittalIdentifierCode"),
    
    # Field 2-4
    lit("526").alias("CompanyNumber"),
    
    # Field 5-8
    lit(None).alias("Blank_5_8").cast('String'),

    # Field 9-10
    lit(call_year).alias("CallYear"),
    
    # Field 11-12
    col("PrimaryRiskState").alias("StateCode"),
    
    # Field 13
    lit("1").alias("LineCode"),
    # Field 14-17
    lit(None).alias("Blank_14_17").cast('String'),
    
    
    # Field 18
    lit(None).alias("RefileIndicator").cast('String'),
    # Field 19
    lit(None).alias("Blank_19").cast('String'),
    
    # Field 20
    lit(None).alias("NTRIndicator").cast('String'),
    # Field 21-103
    lit(None).alias("Blank_21_103").cast('String'),
    
    # Aggregated fields
    col("WrittenExposure").cast('int'),
    col("WrittenPremium"),
    col("PaidLosses").cast('int'),
    col("PaidALAE").cast('int'),
    col("OutstandingLosses").cast('int'),
    col("NumPaidClaims").cast('int'),
    col("NumOutstandingClaims").cast('int')
)

# -------------------------
# 4. Final Output
# -------------------------
transmittal_df

output_path = "Files/TransmittalFILE"

transmittal_df \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "false") \
    .csv(output_path)
######################################################
# Summary File
from pyspark.sql.functions import lit, count

# -------------------------
# 1. Get Record Count (ONLY DATA RECORDS)
# -------------------------
record_count = df.count()

# -------------------------
# 2. Call Year
# -------------------------
call_year = str(2026)[-2:]

# -------------------------
# 3. Create Summary DF (Single Row)
# -------------------------
summary_df = spark.createDataFrame([(1,)], ["dummy"]).select(
    
    # Field 1
    lit("S").alias("TransmittalIdentifierCode"),
    
    # Field 2-4
    lit("526").alias("CompanyNumber"),

    # Field 5-8
    lit(None).alias("Blank_1").cast('String'),
    
    # Field 9-10
    lit(call_year).alias("CallYear"),
    # Field 5-8
    lit(None).alias("Blank_2").cast('String'),
    
    # Field 13
    lit("1").alias("LineCode"),
    # Field 5-8
    lit(None).alias("Blank_3").cast('String'),
    # Field 5-8
    
    
    
    # Field 18
    lit(None).cast("string").alias("RefileIndicator"),
    # Field 5-8
    lit(None).alias("Blank_5").cast('String'),
    
    # Field 154-165
    lit(record_count).alias("RecordCount"),
    lit(None).alias("Blank_4").cast('String')
)

# -------------------------
# 4. Final Output
# -------------------------
display(summary_df)

output_path = "Files/SUMMARYFILE"

summary_df \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "false") \
    .csv(output_path)
##############################################################
# Updating on 12th may After merging onset and Offset Reference records
output_path = "Files/Pre_Final_Report_File"

df \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

##################################################
###############################################################################
# FINAL SELECT ORDER — All 55 Columns in Exact Sequence Required by NISS
###############################################################################

final_order = [
    "Blank_1","CompanyNumber","Blank_5_6","CalendarYear","CallYear","StateCode",
    "LineCode","Blank_14_15","AccidentYear","Blank_18_22","NISSCoverageCode",
    "Blank_26_30","TerritoryCode","RatingZoneCode","TerminalZoneCode","NSSZIPCode",
    "Blank_42_45","ClassificationCode","EligibilityPointsCode","Blank_54_55",
    "AgeGroupCode","ManufacturersModelYear","Blank_59_60","CommercialIndicatorCode",
    "NISS_Exception_Code","ForgivenessCode","ClaimantLevelIndicator","PassiveRestraintCode",
    "DefensiveDriverCreditCode","Blank_69","AntiTheftDeviceCode",
    "DaytimeRunningLampsDiscountCode","Blank_72_74","PolicyLimitsCode","DeductibleCode",
    "Blank_79","SupplementalSpousalLiabilityCode","Blank_81","SublineOfBusinessCode",
    "Blank_83","TypeOfLossCode","LiabilityNoFaultCode","Blank_86_98",
    "AnnualStatementLineOfBusinessCode","Blank_102_103","WrittenExposure",
    "WrittenPremium","PaidLosses","PaidAllocatedLossAdjustmentExpenses",
    "OutstandingLossesIncludingALAE","NumberOfPaidClaims",
    "NumberOfOutstandingClaims","ReservedForNISSUse","ClaimNumberPremiumID",
    "ClaimantNumber"
]

df_final = df.select(final_order)
# remove header after converting them in the above values

###############################################################################
# WRITE DELTA TABLE
spark.sql("DROP TABLE IF EXISTS NISS_Final_Report")
###############################################################################
df_final.write.mode("overwrite").format("delta").saveAsTable("NISS_Final_Report")

###############################################################################
# WRITE CSV — NO HEADER, NO QUOTES, FIXED WIDTH OUTPUT
###############################################################################

output_path = "Files/First_NISS_FORMAT_FILE"

df_final \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "false") \
    .csv(output_path)


# In[99]:


end_time =time.time()
total_time=end_time-strat_time
print(f"Total Execution Time: {total_time} Seconds")
print(f"Total Execution Time: {total_time/60: .2f} Minutes")

