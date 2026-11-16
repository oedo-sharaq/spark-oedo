from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from sparkOEDOModules.procModules import mapper, constants
from functools import reduce

def track_4ppacs(posi_dfs: dict, geometry: list[list[float]], event_col: str) -> DataFrame:
    """
    Event-by-event PPAC tracking

    Parameters
    ----------
    posi_dfs : dict
        {
            "1A": SparkDF,
            "1B": SparkDF,
            "2A": SparkDF,
            "2B": SparkDF
        }

        each DataFrame must contain:
            event_col
            GetX
            GetY

    geometry : list
        [
            [zx, zy, z],   # 1A
            [zx, zy, z],   # 1B
            [zx, zy, z],   # 2A
            [zx, zy, z]    # 2B
        ]

    event_col : str
        event id column

    Returns
    -------
    Spark DataFrame

        columns:
            event_col
            x0
            y0
            a0
            b0
    """

    det_order = ["1A", "1B", "2A", "2B"]

    dfs = []

    # ----------------------------------------
    # prepare detector tables
    # ----------------------------------------
    for det, geo in zip(det_order, geometry):

        zx, zy, z = geo

        z_x = z + zx
        z_y = z + zy

        df = (
            posi_dfs[det]
            .select(
                event_col,

                F.col("GetX").alias(f"{det}_x"),
                F.col("GetY").alias(f"{det}_y")
            )

            .withColumn(f"{det}_zx", F.lit(z_x))
            .withColumn(f"{det}_zy", F.lit(z_y))
        )

        dfs.append(df)

    # ----------------------------------------
    # join all detectors
    # ----------------------------------------
    merged_df = reduce(
        lambda left, right:
            left.join(right, on=event_col, how="outer"),
        dfs
    )

    # ----------------------------------------
    # helper expressions
    # ----------------------------------------
    def fit_expr(val_cols, z_cols):

        n_expr = None
        sz_expr = None
        szz_expr = None
        sx_expr = None
        sxz_expr = None

        for vcol, zcol in zip(val_cols, z_cols):

            valid = F.when(F.col(vcol).isNotNull(), 1).otherwise(0)

            zval = F.when(F.col(vcol).isNotNull(), F.col(zcol)).otherwise(0.0)

            xval = F.when(F.col(vcol).isNotNull(), F.col(vcol)).otherwise(0.0)

            n_expr   = valid if n_expr is None else n_expr + valid
            sz_expr  = zval if sz_expr is None else sz_expr + zval
            szz_expr = zval*zval if szz_expr is None else szz_expr + zval*zval

            sx_expr  = xval if sx_expr is None else sx_expr + xval
            sxz_expr = xval*zval if sxz_expr is None else sxz_expr + xval*zval

        delta = 1.0 / (n_expr * szz_expr - sz_expr * sz_expr)

        slope = (n_expr * sxz_expr - sx_expr * sz_expr) * delta*1000  # slope in mrad

        intercept = (-sxz_expr * sz_expr + szz_expr * sx_expr) * delta

        return intercept, slope, n_expr

    # ----------------------------------------
    # x fit
    # ----------------------------------------
    x_cols = [f"{det}_x" for det in det_order]
    zx_cols = [f"{det}_zx" for det in det_order]

    x0_expr, a0_expr, nx_expr = fit_expr(x_cols, zx_cols)

    # ----------------------------------------
    # y fit
    # ----------------------------------------
    y_cols = [f"{det}_y" for det in det_order]
    zy_cols = [f"{det}_zy" for det in det_order]

    y0_expr, b0_expr, ny_expr = fit_expr(y_cols, zy_cols)

    # ----------------------------------------
    # final dataframe
    # ----------------------------------------
    result_df = (
        merged_df
        .withColumn("nx", nx_expr)
        .withColumn("ny", ny_expr)

        # 少なくとも3 detector
        .filter(
            (F.col("nx") >= 3) &
            (F.col("ny") >= 3)
        )
        .withColumn("x0", x0_expr)
        .withColumn("a0", a0_expr)

        .withColumn("y0", y0_expr)
        .withColumn("b0", b0_expr)

        .select(
            event_col,
            "x0",
            "y0",
            "a0",
            "b0")
            )
    return result_df