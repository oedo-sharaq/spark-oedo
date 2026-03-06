from pyspark.sql import DataFrame
from pyspark.sql.functions import posexplode
from pyspark.ml.feature import Bucketizer
import matplotlib.pyplot as plt
import numpy as np

def Hist2D(dataFrame: DataFrame, colName: tuple[str, str], nbins: tuple[int, int], range: tuple[tuple[float, float], tuple[float, float]]) -> plt:
    """
    Plot 2D histogram of the column named colName["x", "y"]. Assuming the column stores a value per row

    Parameters
    ----------
    dataFrame: Input DataFrame
    colName: Column names to plot [x, y]
    nbis: Number of bins [nbinsx, nbinsy]
    range: Histogram range as [x[min, max], y[min, max]]

    Returns
    -------
    2D histogram as matplotlib.pyplot.plt
    """

    df = dataFrame.select(colName[0],colName[1]).toPandas()

    # Step 2: Filter the DataFrame to the range of interest (range[0][0] to 100) for both pos_x and pos_y
    filtered_df = df[(df[colName[0]] >= range[0][0]) & (df[colName[0]] <= range[0][1]) & (df[colName[1]] >= range[1][0]) & (df[colName[1]] <= range[1][1])]

    # Step 3: Plot the 2D histogram using hist2d
    plt.hist2d(filtered_df[colName[0]], filtered_df[colName[1]], bins=nbins, range=[[range[0][0], range[0][1]], [range[1][0], range[1][1]]], cmap='viridis')

    # Add labels and title
    plt.xlabel(colName[0])
    plt.ylabel(colName[1])
    plt.title("2D Histogram of " + colName[1] + " vs " + colName[0])

    # Show the color bar
    plt.colorbar(label='Counts')

    return plt

def Hist2DArrays(dataFrame: DataFrame, colName: tuple[str, str], nbins: tuple[int, int], range: tuple[tuple[float, float], tuple[float, float]]) -> plt:
    """
    Plot 2D histogram of the column named colName["x", "y"]. Assuming the column stores an array that is aligned with each other

    Parameters
    ----------
    dataFrame: Input DataFrame
    colName: Column names to plot [x, y]
    nbis: Number of bins [nbinsx, nbinsy]
    range: Histogram range as [x[min, max], y[min, max]]

    Returns
    -------
    2D histogram as matplotlib.pyplot.plt
    """
    dataFrame = dataFrame.select(colName[0],colName[1])
    exploded_pos_x_df = dataFrame.select(posexplode(colName[0]).alias("idx", colName[0]))
    exploded_pos_y_df = dataFrame.select(posexplode(colName[1]).alias("idx", colName[1]))
    exploded_df = exploded_pos_x_df.join(exploded_pos_y_df, "idx")

    return Hist2D(exploded_df, colName, nbins, range)
