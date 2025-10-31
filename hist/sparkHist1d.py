from pyspark.sql.functions import explode
from pyspark.sql import DataFrame
from pyspark.ml.feature import Bucketizer
import matplotlib.pyplot as plt
import numpy as np

def Hist1D(dataFrame: DataFrame, colName: str, nbins: int, range: tuple[float, float], **kwargs) -> tuple[np.ndarray, np.ndarray] :
    """
    Plot 1D histogram of the column named "colName". Assuming the column stores a value per row.
    Uses spark to count bin contents. pandasHist1d module could be faster for a short DataFrame

    Parameters
    ----------
    dataFrame: Input DataFrame that contains a column named colName
    colName: Column name to plot
    nbis: Number of bins
    range: Histogram range as [min, max]

    Other parameters
    ----------------
    **kwargs: It will be passed to matplotlb bar() function

    Returns
    -------
    1D histogram as matplotlib.pyplot.plt
    """
    # Define bin edges (n bins between -range[0] and range[1])
    bin_edges = np.linspace(range[0], range[1], num=nbins).tolist()

    # Add underflow and overflow bins to an extended list
    ex_bin_edges = bin_edges.copy()
    ex_bin_edges.append(np.inf)
    ex_bin_edges.insert(0, -np.inf)

    # Create a Bucketizer with the extended list of bins
    bucketizer = Bucketizer(splits=ex_bin_edges, inputCol=colName, outputCol="bin")

    # Apply the Bucketizer to the DataFrame
    binned_df = bucketizer.transform(dataFrame.select(colName))

    # Group by bin and count the occurrences in each bin and remove Null bins
    histogram_df = binned_df.groupBy("bin").count().orderBy("bin")
    histogram_df = histogram_df.filter(histogram_df["bin"].isNotNull())

    # Collect the histogram data from Spark to local
    histogram_data = histogram_df.collect()

    # Calculate bin centers
    bin_edges = np.linspace(range[0], range[1], num=nbins)
    counts = np.zeros(len(bin_edges)-1)

    # Populate the counts array based on the histogram data
    # Also, counts statistics for under/overflowed bins
    underflow = 0
    overflow = 0
    inrange = 0
    for row in histogram_data:
        bin_index = int(row['bin'])  # Get the bin index
        if bin_index == 0: # The underflow bin
            underflow = row['count']
        if bin_index == nbins: # The overflow bin
            overflow = row['count']
        else:
            counts[bin_index-1] = row['count']  # Populate the count for the bin
            inrange = inrange + row['count']

    # Calculate bin centers
    bin_centers = 0.5 * (bin_edges[:-1] + bin_edges[1:])
    bin_width = bin_centers[1] - bin_centers[0]

    # Plot the histogram
    plt.bar(bin_centers, counts, width=bin_width, **kwargs)

    # Add labels and title
    plt.xlabel(colName)
    plt.ylabel("Frequency")
    plt.title("Hist1D " + colName + " values")

    # Print statistics
    print("Total entries: {}, Underflow: {}, Inside: {}, Overflow: {}".format(inrange+underflow+overflow, underflow, inrange, overflow))

    return bin_centers, counts

def Hist1DArrays(dataFrame: DataFrame, colName: str, nbins: int, range: tuple[float, float], **kwargs) -> tuple[np.ndarray,np.ndarray]:
    """
    Plot 1D histogram of the column named colName.
    The column stores an array of values.

    Parameters
    ----------
    dataFrame: Input DataFrame
    colName: Column name to plot
    nbis: Number of bins
    range: Histogram range as [min, max]

    Returns
    -------
    1D histogram as matplotlib.pyplot.plt
    """
    dataFrame = dataFrame.select(colName)
    exploded_df = dataFrame.select(explode(colName).alias(colName))

    return Hist1D(exploded_df, colName, nbins, range, **kwargs)