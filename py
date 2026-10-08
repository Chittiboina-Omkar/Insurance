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


