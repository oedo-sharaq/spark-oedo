from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from sparkOEDOModules.procModules import tref, manipulation, mapper, tot, calibrator, constants, monotoneTableConverter
from sparkOEDOModules.detectorProcs.srppac import srppacPosDqdx

#DETECTOR_NAMES = ["sr91","sr92","src1","src2","sr11","sr12"]
#DETECTOR_NAMES = ["sr0"]
DETECTOR_NAMES = ["sr91"]

mapping_df = None
tref_mapping_df = None
udf_dict = {}

def LoadCSVFiles(spark: SparkSession):
    """
    Load CSV Files to DataFrames. This function should be called once before calling Process()

    Parameters
    ----------
    spark: SparkSession
    """
    global mapping_df, tref_mapping_df, udf_dict
    if mapping_df is None:
        # 全ファイルの読み込みに成功してから設定を反映する。
        new_mapping = mapper.ReadMapCSV(spark, 'srppac.csv', DETECTOR_NAMES)
        new_tref = tref.ReadCSV(spark)
        new_udfs = {}
        for det in DETECTOR_NAMES:
            new_udfs[det + '_xc0'] = monotoneTableConverter.getConverterUDF(
                spark, 'srppac/' + det + '_xc0.csv')
            new_udfs[det + '_yc0'] = monotoneTableConverter.getConverterUDF(
                spark, 'srppac/' + det + '_yc0.csv')
        mapping_df, tref_mapping_df, udf_dict = new_mapping, new_tref, new_udfs



def Process(rawDF: DataFrame, full: bool, require: str) -> DataFrame:
    """
    Main processor function for SRPPAC detectors

    Parameters
    ----------
    rawDF: Input rawdata DataFrame
    full: Flag for full outputs
    require: Only include rows with column "require" is not null

    Returns
    -------
    Output DataFrame
    """

    if mapping_df == None:
        print("Call LoadCSVFiles() before calling Process()")
        return

    # Map tref channels first
    tref_df = tref.Tref(rawDF, tref_mapping_df)

    # Generate timecharge dataframes
    df = mapper.Map(rawDF, mapping_df, [constants.ID_COLNAME,"cat","value","id","edge","dev","fp","det","geo"])
    df = tref.SubtractTref(df,tref_df) # Tref subtraction
    df = manipulation.Validate(df,[-100000,100000])
         # Flip edge for cathode signals (trailingComesFirst=True)
    df = df.withColumn("edge", F.when(F.col("cat").endswith("a"), F.col("edge")).otherwise(1-F.col("edge")))
    df = tot.Tot(df)
    df = calibrator.ToFloat(df,"charge", 0, 0.09765627)
    df = calibrator.ToFloat(df,"timing", 0, 0.09765627)
    srppac_df = rawDF.select(constants.ID_COLNAME).dropDuplicates([constants.ID_COLNAME])

    for ppac in DETECTOR_NAMES:
        df_a = df.filter(F.col("cat") == ppac+"a")
        df_x = df.filter(F.col("cat") == ppac+"x")
        df_y = df.filter(F.col("cat") == ppac+"y")
        pos_x = srppacPosDqdx.srppacPosDqdx(df_x,udf_dict[ppac+"_xc0"],46.5,2.55,0,True)
        pos_y = srppacPosDqdx.srppacPosDqdx(df_y,udf_dict[ppac+"_yc0"],28.5,2.58,0,True)
        pos_x = pos_x.select(constants.ID_COLNAME,F.col("pos").alias(ppac+"x_pos"))
        pos_y = pos_y.select(constants.ID_COLNAME,F.col("pos").alias(ppac+"y_pos"))

        # Fill event data into an array per event and add the ppac name to the column name
        if full:
            # Full output
            df_a = df_a.groupBy(constants.ID_COLNAME).agg(
                F.collect_list("timing").alias(ppac+"a_timing"),
                F.collect_list("charge").alias(ppac +"a_charge")
            )
            df_x = df_x.groupBy(constants.ID_COLNAME).agg(
                F.collect_list("id").alias(ppac+"x_id"),
                F.collect_list("timing").alias(ppac+"x_timing"),
                F.collect_list("charge").alias(ppac +"x_charge")
            )
            df_x = df_x.join(pos_x, [constants.ID_COLNAME], how="left")
            df_y = df_y.groupBy(constants.ID_COLNAME).agg(
                F.collect_list("id").alias(ppac+"y_id"),
                F.collect_list("timing").alias(ppac+"y_timing"),
                F.collect_list("charge").alias(ppac +"y_charge")
            )
            df_y = df_y.join(pos_y, [constants.ID_COLNAME], how="left")
            # join to the final output data frame
            srppac_df = srppac_df.join(df_a, on=[constants.ID_COLNAME], how="left")
            srppac_df = srppac_df.join(df_x, on=[constants.ID_COLNAME], how="left")
            srppac_df = srppac_df.join(df_y, on=[constants.ID_COLNAME], how="left")
        else:
            # Short output
            df_a = df_a.groupBy(constants.ID_COLNAME).agg(
                F.collect_list("timing").alias(ppac+"a_timing"),
                F.collect_list("charge").alias(ppac +"a_charge")
            )
            pos_x = pos_x.filter(F.col(ppac+"x_pos").isNotNull())
            result_df = pos_x.join(pos_y, [constants.ID_COLNAME], how="left")
            result_df = result_df.join(df_a, [constants.ID_COLNAME], how="left")
            if ppac == require:
                srppac_df = srppac_df.join(result_df, [constants.ID_COLNAME], how="inner")
            else:
                srppac_df = srppac_df.join(result_df, [constants.ID_COLNAME], how="left")

    return srppac_df
 
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help='input file name', required=True)
    parser.add_argument('--partitions', help='number of partitions', nargs='?', const=1, type=int)
    parser.add_argument('--full', help='output full time charge data', action='store_true')
    parser.add_argument('--require', help='required ppac name. Rows without this ppac column will be deleted')
    args = parser.parse_args()
    if constants.SERVER_TYPE == 'aws':
        spark = SparkSession.builder.getOrCreate()
    else:
        spark = (SparkSession.builder.master(constants.MASTER).appName('SRPPAC')
                 .config('spark.driver.memory', '20g').config('spark.executor.memory', '20g').getOrCreate())
    raw_df = spark.read.parquet(constants.DATA_PATH + '/' + args.input + '.parquet')
    if args.partitions is not None:
        raw_df = raw_df.repartition(args.partitions)
    exploded_df = mapper.ExplodeRawData(raw_df)
    LoadCSVFiles(spark)
    srppac_df = Process(exploded_df, args.full, args.require)
    srppac_df.write.mode('overwrite').parquet(constants.DATA_PATH + '/' + args.input + '_srppac.parquet')


if __name__ == '__main__':
    main()