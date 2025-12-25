from pyspark.sql.functions import posexplode, row_number, lit
from pyspark.sql import DataFrame
from pyspark.ml.feature import Bucketizer
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import numpy as np

def Hist2D(dataFrame: DataFrame, colName: tuple[str, str], nbins: tuple[int, int], range: tuple[tuple[float, float], tuple[float, float]], **kwargs) -> np.ndarray:
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
    # Define bin edges
    bin_edges_x = np.linspace(range[0][0], range[0][1], num=nbins[0]).tolist()
    bin_edges_y = np.linspace(range[1][0], range[1][1], num=nbins[1]).tolist()
    ex_bin_edges_x = bin_edges_x.copy()
    ex_bin_edges_y = bin_edges_y.copy()
    ex_bin_edges_x.append(np.inf)
    ex_bin_edges_y.append(np.inf)
    ex_bin_edges_x.insert(0,-np.inf)
    ex_bin_edges_y.insert(0,-np.inf)

    # Create a Bucketizer
    bucketizer_x = Bucketizer(splits=ex_bin_edges_x, inputCol=colName[0], outputCol="bin_x")
    bucketizer_y = Bucketizer(splits=ex_bin_edges_y, inputCol=colName[1], outputCol="bin_y")

    # Apply the Bucketizer to the DataFrame
    binned_df = bucketizer_x.transform(dataFrame.select(colName[0], colName[1]))
    binned_df = bucketizer_y.transform(binned_df)

    # Group by bin and count the occurrences in each bin and remove Null bins
    binned_df = binned_df.filter(binned_df["bin_x"].isNotNull() & binned_df["bin_y"].isNotNull())
    histogram_2d_df = binned_df.groupBy("bin_x","bin_y").count()

    # Collect the histogram data from Spark to local
    histogram_data = histogram_2d_df.collect()

    # Extract bin indices and counts from the collected data
    counts = np.zeros((len(bin_edges_x) - 1, len(bin_edges_y) - 1))

    # Populate the 2D counts array
    # statArray is an 3 x 3 numpy.ndarray with underflow, inrange, overflow statistics for x and y
    statArray = np.zeros(shape=(3,3))
    for row in histogram_data:
        bin_x = int(row['bin_x'])
        bin_y = int(row['bin_y'])
        idx = 1
        idy = 1
        if bin_x == 0:
            idx = 0
        elif bin_x == nbins[0]:
            idx = 2
        if bin_y == 0:
            idy = 0
        elif bin_y == nbins[1]:
            idy = 2
        if idx == 1 and idy == 1:
            counts[bin_x-1, bin_y-1] = row['count']
        statArray[idx,idy] = statArray[idx,idy] + row['count']

    # Plot the 2D histogram using a heatmap
    plt.imshow(counts.T, extent=[range[0][0], range[0][1], range[1][0], range[1][1]], origin='lower', aspect='auto', **kwargs)

    # Add labels and title
    plt.xlabel(colName[0])
    plt.ylabel(colName[1])
    plt.title("2D Histogram of " + colName[1] + " vs " + colName[0])

    # Show the color bar
    plt.colorbar(label='Counts')

    # Print statistics
    print("Statistics:")
    print(np.flip(np.swapaxes(statArray, 0, 1),0))
    return counts

def Hist2DArrays(dataFrame: DataFrame, colName: tuple[str, str], nbins: tuple[int, int], range: tuple[tuple[float, float], tuple[float, float]], filter: str = None, **kwargs) -> np.ndarray:
    """
    Plot 2D histogram of the column named colName["x", "y"]. Assuming the column stores an array that is aligned with each other

    Parameters
    ----------
    dataFrame: Input DataFrame
    colName: Column names to plot [x, y]
    nbis: Number of bins [nbinsx, nbinsy]
    range: Histogram range as [x[min, max], y[min, max]]
    filter: Optional filter string to apply after exploding before histogramming

    Returns
    -------
    2D histogram as matplotlib.pyplot.plt
    """
    dataFrame = dataFrame.select(colName[0],colName[1])
    windowSpec = Window.orderBy(lit(0))
    dataFrame = dataFrame.withColumn("row", row_number().over(windowSpec))
    exploded_pos_x_df = dataFrame.select(posexplode(colName[0]).alias("idx", colName[0]),"row")
    exploded_pos_y_df = dataFrame.select(posexplode(colName[1]).alias("idx", colName[1]),"row")
    exploded_df = exploded_pos_x_df.join(exploded_pos_y_df, ["row", "idx"])
    if filter is not None:
        exploded_df = exploded_df.filter(filter)
    return Hist2D(exploded_df, colName, nbins, range, **kwargs)

def Hist2DArrayVsPos(dataFrame: DataFrame, colName: str, nbins: tuple[int, int], range: tuple[tuple[float, float], tuple[float, float]], **kwargs) -> np.ndarray:
    """
    Plot 2D histogram of the array elements vs these array pos.

    Parameters
    ----------
    dataFrame: Input DataFrame
    colName: Name of the column with a 1D array
    nbis: Number of bins [nbinsx, nbinsy]
    range: Histogram range as [x[min, max], y[min, max]]

    Returns
    -------
    2D histogram as matplotlib.pyplot.plt
    """
    dataFrame = dataFrame.select(colName)
    exploded_df = dataFrame.select(posexplode(colName))
    colNames = ["pos", "col"]
    return Hist2D(exploded_df, colNames, nbins, range, **kwargs)
