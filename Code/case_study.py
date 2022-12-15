from pyspark.sql.functions import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
import sys

class Case_Study():


    def __init__(self):
        # Initiating SparkSession
        spark = SparkSession.builder.appName("Case Study").master("local[*]").getOrCreate()
        self.spark=spark

        """

        Defining the schema manually for all the required datasets

        """

        person_schema = StructType([ \
            StructField("CRASH_ID", IntegerType()), \
            StructField("units_NBR", IntegerType()), \
            StructField("PRSN_NBR", IntegerType()), \
            StructField("PRSN_TYPE_ID", StringType()), \
            StructField("PRSN_OCCPNT_POS_ID", StringType()), \
            StructField("PRSN_INJRY_SEV_ID", StringType()), \
            StructField("PRSN_AGE", IntegerType()), \
            StructField("PRSN_ETHNICITY_ID", StringType()), \
            StructField("PRSN_GNDR_ID", StringType()), \
            StructField("PRSN_EJCT_ID", StringType()), \
            StructField("PRSN_REST_ID", StringType()), \
            StructField("PRSN_AIRBAG_ID", StringType()), \
            StructField("PRSN_HELMET_ID", StringType()), \
            StructField("PRSN_SOL_FL", StringType()), \
            StructField("PRSN_ALC_SPEC_TYPE_ID", StringType()), \
            StructField("PRSN_ALC_RSLT_ID", StringType()), \
            StructField("PRSN_BAC_TEST_RSLT", StringType()), \
            StructField("PRSN_DRG_SPEC_TYPE_ID", StringType()), \
            StructField("PRSN_DRG_RSLT_ID", StringType()), \
            StructField("DRVR_DRG_CAT_1_ID", StringType()), \
            StructField("PRSN_DEATH_TIME", StringType()), \
            StructField("INCAP_INJRY_CNT", IntegerType()), \
            StructField("NONINCAP_INJRY_CNT", IntegerType()), \
            StructField("POSS_INJRY_CNT", IntegerType()), \
            StructField("NON_INJRY_CNT", IntegerType()), \
            StructField("UNKN_INJRY_CNT", IntegerType()), \
            StructField("TOT_INJRY_CNT", IntegerType()), \
            StructField("DEATH_CNT", IntegerType()), \
            StructField("DRVR_LIC_TYPE_ID", StringType()), \
            StructField("DRVR_LIC_STATE_ID", StringType()), \
            StructField("DRVR_LIC_CLS_ID", StringType()), \
            StructField("DRVR_ZIP", StringType()) \
            ])

        units_schema = StructType([ \
            StructField("CRASH_ID", IntegerType()), \
            StructField("units_NBR", StringType()), \
            StructField("units_DESC_ID", StringType()), \
            StructField("VEH_PARKED_FL", StringType()), \
            StructField("VEH_HNR_FL", StringType()), \
            StructField("VEH_LIC_STATE_ID", StringType()), \
            StructField("VIN", StringType()), \
            StructField("VEH_MOD_YEAR", StringType()), \
            StructField("VEH_COLOR_ID", StringType()), \
            StructField("VEH_MAKE_ID", StringType()), \
            StructField("VEH_MOD_ID", StringType()), \
            StructField("VEH_BODY_STYL_ID", StringType()), \
            StructField("EMER_RESPNDR_FL", StringType()), \
            StructField("OWNR_ZIP", StringType()), \
            StructField("FIN_RESP_PROOF_ID", StringType()), \
            StructField("FIN_RESP_TYPE_ID", StringType()), \
            StructField("VEH_DMAG_AREA_1_ID", StringType()), \
            StructField("VEH_DMAG_SCL_1_ID", StringType()), \
            StructField("FORCE_DIR_1_ID", StringType()), \
            StructField("VEH_DMAG_AREA_2_ID", StringType()), \
            StructField("VEH_DMAG_SCL_2_ID", StringType()), \
            StructField("FORCE_DIR_2_ID", StringType()), \
            StructField("VEH_INVENTORIED_FL", StringType()), \
            StructField("VEH_TRANSP_NAME", StringType()), \
            StructField("VEH_TRANSP_DEST", StringType()), \
            StructField("CONTRIB_FACTR_1_ID", StringType()), \
            StructField("CONTRIB_FACTR_2_ID", StringType()), \
            StructField("CONTRIB_FACTR_P1_ID", StringType()), \
            StructField("VEH_TRVL_DIR_ID", StringType()), \
            StructField("FIRST_HARM_EVT_INV_ID", StringType()), \
            StructField("INCAP_INJRY_CNT", IntegerType()), \
            StructField("NONINCAP_INJRY_CNT", IntegerType()), \
            StructField("POSS_INJRY_CNT", IntegerType()), \
            StructField("NON_INJRY_CNT", IntegerType()), \
            StructField("UNKN_INJRY_CNT", IntegerType()), \
            StructField("TOT_INJRY_CNT", IntegerType()), \
            StructField("DEATH_CNT", IntegerType()), \
            ])

        charges_schema = StructType([ \
            StructField("CRASH_ID", IntegerType()), \
            StructField("units_NBR", IntegerType()), \
            StructField("PRSN_NBR", StringType()), \
            StructField("CHARGE", StringType()), \
            StructField("CITATION_NBR", StringType()) \
            ])

        damages_schema = StructType([ \
            StructField("CRASH_ID", IntegerType()), \
            StructField("DAMAGED_PROPERTY", StringType()) \
            ])

        self.person_schema = person_schema
        self.units_schema = units_schema
        self.charges_schema = charges_schema
        self.damages_schema = damages_schema

    def parse_config_file(self,path):

        # Loading and Parsing the config.json file to fetch input and output path

        f = open(path)

        config = json.load(f)

        f.close()

        return config



    def read_dataset(self,config):

        """

        Using Spark READ API to load datasets and creating dataframes

        """

        persons = self.spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(self.person_schema) \
        .option("path", str(config["persons"])) \
        .load()

        units = self.spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(self.units_schema) \
        .option("path", str(config["units"])) \
        .load()

        charges = self.spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(self.charges_schema) \
        .option("path", str(config["charges"])) \
        .load()

        damages = self.spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(self.damages_schema) \
        .option("path", str(config["damages"])) \
        .load()

        return persons,units,charges,damages

    def start_analysis(self,persons,units,charges,damages):

        """

        Analysis 1
        Finding the number of crashes (accidents) in which number of persons killed are male

        """

        analysis1 = persons.select(col("DEATH_CNT")) \
        .where("PRSN_GNDR_ID='MALE' and DEATH_CNT>0") \
        .agg(sum("DEATH_CNT").alias("DEATH_COUNT_MALE"))

        """

        Analysis 2
        Finding the number of two wheelers which are booked for crashes

        """

        u_join_c = units.join(charges, units.CRASH_ID == charges.CRASH_ID,"inner").cache()  # using cache() to store the result in memory

        analysis2 = u_join_c.select(units["CRASH_ID"], units["VIN"]) \
        .where("VEH_BODY_STYL_ID IN ('MOTORCYCLE','POLICE MOTORCYCLE')") \
        .distinct() \
        .agg(count("VIN").alias("TWO_WHEELER_COUNT"))

        """

        Analysis 3
        Which state has highest number of accidents in which females are involved

        """

        analysis3 = persons.select(col("CRASH_ID"), col("DRVR_LIC_STATE_ID")) \
        .where("PRSN_GNDR_ID='FEMALE'") \
        .distinct() \
        .groupBy("DRVR_LIC_STATE_ID") \
        .agg(count("CRASH_ID").alias("COUNT_CRASHES")) \
        .sort(col("COUNT_CRASHES").desc()).limit(1) \
        .select(col("DRVR_LIC_STATE_ID"))


        """

        Analysis 4
        Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death

        """

        windowSpec1 = Window \
        .orderBy(col("TOTAL_INJURY_COUNT").desc())

        analysis4 = units.select("VEH_MAKE_ID", (col("TOT_INJRY_CNT") + col("DEATH_CNT")).alias("TOTAL_INJURY_COUNT")) \
        .groupBy("VEH_MAKE_ID") \
        .agg(sum("TOTAL_INJURY_COUNT").alias("TOTAL_INJURY_COUNT")) \
        .withColumn("ROW_NUM", row_number().over(windowSpec1)) \
        .where("ROW_NUM>=5 and ROW_NUM<=15") \
        .select("VEH_MAKE_ID")



        """

        Analysis 5
        For all the body styles involved in crashes, mention the top ethnic user group of each unique body style

        """

        windowSpec2 = Window \
        .partitionBy("VEH_BODY_STYL_ID") \
        .orderBy(col("COUNT_ETHNICITY").desc())

        p_join_u = persons.join(units, units.CRASH_ID == persons.CRASH_ID,"inner").cache()  # using cache() to store the result in memory

        analysis5 = p_join_u.select(units["VEH_BODY_STYL_ID"], persons["PRSN_ETHNICITY_ID"]) \
        .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID") \
        .agg(count("PRSN_ETHNICITY_ID").alias("COUNT_ETHNICITY")) \
        .withColumn("ROW_NUM", row_number().over(windowSpec2)) \
        .select(col("VEH_BODY_STYL_ID"), col("PRSN_ETHNICITY_ID"), col("ROW_NUM")) \
        .where("ROW_NUM=1") \
        .select(col("VEH_BODY_STYL_ID"), col("PRSN_ETHNICITY_ID"))


        """

        Analysis 6
        Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash

        """

        analysis6 = p_join_u.select(persons["DRVR_ZIP"], persons["CRASH_ID"]) \
        .where("CONTRIB_FACTR_1_ID='UNDER INFLUENCE - ALCOHOL' or CONTRIB_FACTR_2_ID='UNDER INFLUENCE - ALCOHOL' or CONTRIB_FACTR_P1_ID='UNDER INFLUENCE - ALCOHOL'") \
        .filter(col("DRVR_ZIP").isNotNull()) \
        .distinct() \
        .groupBy("DRVR_ZIP") \
        .agg(count("CRASH_ID").alias("CRASH_COUNT")) \
        .sort(col("CRASH_COUNT").desc()).limit(5)


        """

        Analysis 7
        Count of Distinct Crash IDs where No Damaged Property was observed 
        and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

        """

        analysis7 = units.join(damages, units.CRASH_ID == damages.CRASH_ID, "leftanti") \
        .select(units["CRASH_ID"], substring("VEH_DMAG_SCL_1_ID", 9, 1).cast(IntegerType()).alias("DAMAGAE_LVL1"), substring("VEH_DMAG_SCL_2_ID", 9, 1).cast(IntegerType()).alias("DAMAGAE_LVL2")) \
        .where("DAMAGAE_LVL1>4 or DAMAGAE_LVL2>4 and FIN_RESP_TYPE_ID IN ('LIABILITY INSURANCE POLICY','INSURANCE BINDER','PROOF OF LIABILITY INSURANCE') ") \
        .select("CRASH_ID") \
        .distinct() \
        .agg(count("CRASH_ID").alias("COUNT_CRASH_ID"))


        """

        Analysis 8
        Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, 
        has licensed Drivers, uses top 10 used vehicle colours 
        and has car licensed with the Top 25 states with highest number of offences

        """

        top_25_state = u_join_c.where("CHARGE like '%SPEED%' ") \
        .select("VEH_LIC_STATE_ID", units["CRASH_ID"]).distinct() \
        .groupBy("VEH_LIC_STATE_ID") \
        .count().sort(col("count").desc()).limit(25) \
        .select("VEH_LIC_STATE_ID")

        top_10_colour = units.join(top_25_state, top_25_state.VEH_LIC_STATE_ID == units.VEH_LIC_STATE_ID, "inner") \
        .select("VEH_COLOR_ID", "CRASH_ID") \
        .distinct() \
        .groupBy("VEH_COLOR_ID") \
        .agg(count("CRASH_ID").alias("count")).sort(col("count").desc()).limit(10) \
        .select("VEH_COLOR_ID")

        licensed_drivers = p_join_u.select(units["CRASH_ID"], "VEH_MAKE_ID", "VEH_COLOR_ID") \
        .where("DRVR_LIC_CLS_ID not in ('UNLICENSED','UNKNOWN','NA')") \
        .distinct()

        analysis8 = licensed_drivers.join(top_10_colour, ["VEH_COLOR_ID"], "inner") \
        .select("VEH_MAKE_ID", "CRASH_ID") \
        .distinct() \
        .groupBy("VEH_MAKE_ID").agg(count("CRASH_ID").alias("count")) \
        .sort(col("count").desc()).limit(5) \
        .select("VEH_MAKE_ID")



        return analysis1,analysis2,analysis3,analysis4,analysis5,analysis6,analysis7,analysis8


    def write_output(self,analysis1,analysis2,analysis3,analysis4,analysis5,analysis6,analysis7,analysis8,config):

        # Storing the result of analysis 1

        analysis1.write.format("csv").mode("overwrite").save(str(config["analysis1"]))

        # Storing the result of analysis 2

        analysis2.write.format("csv").mode("overwrite").save(str(config["analysis2"]))

        # Storing the result of analysis 3

        analysis3.write.format("csv").mode("overwrite").save(str(config["analysis3"]))

        # Storing the result of analysis 4

        analysis4.write.format("csv").mode("overwrite").save(str(config["analysis4"]))

        # Storing the result of analysis 5

        analysis5.write.format("csv").mode("overwrite").save(str(config["analysis5"]))

        # Storing the result of analysis 6

        analysis6.write.format("csv").mode("overwrite").save(str(config["analysis6"]))

        # Storing the result of analysis 7

        analysis7.write.format("csv").mode("overwrite").save(str(config["analysis7"]))

        # Storing the result of analysis 8

        analysis8.write.format("csv").mode("overwrite").save(str(config["analysis8"]))


if __name__ == "__main__":
    case_study = Case_Study()
    config = case_study.parse_config_file(sys.argv[1])
    persons,units,charges,damages = case_study.read_dataset(config)
    analysis1,analysis2,analysis3,analysis4,analysis5,analysis6,analysis7,analysis8 = case_study.start_analysis(persons,units,charges,damages)
    case_study.write_output(analysis1,analysis2,analysis3,analysis4,analysis5,analysis6,analysis7,analysis8,config)
    print("")
    print("Application completed successfully!")
