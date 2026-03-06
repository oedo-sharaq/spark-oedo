from pyspark.sql import DataFrame
from pyspark.sql.functions import explode
import matplotlib.pyplot as plt

def Hist1D(dataFrame: DataFrame, colName: str, nbins: int, range: tuple[float, float], **kwargs) -> plt:
    """
    Plot 1D histogram of the column named "colName". Assuming the column stores a value per row

    Parameters
    ----------
    dataFrame: Input DataFrame that contains a column named colName
    colName: Column name to plot
    nbis: Number of bins
    range: Histogram range as [min, max]

    Returns
    -------
    1D histogram as matplotlib.pyplot.plt
    """
    pandas_df = dataFrame.select(colName).toPandas()

    # Step 1: Filter the DataFrame to the range of interest (-100 to 100)
    filtered_df = pandas_df[(pandas_df[colName] >= range[0]) & (pandas_df[colName] <= range[1])]

    # Step 3: Plot the histogram
    plt.hist(filtered_df[colName], bins=nbins, range=(range[0], range[1]),**kwargs)

    # Add labels and title
    plt.xlabel(colName)
    plt.ylabel("Frequency")
    plt.title("Histogram of " + colName + " values")

    return plt

def Hist1DArray(dataFrame: DataFrame, colName: str, nbins: int, range: tuple[float, float]) -> plt:
    dataFrame = dataFrame.select(colName)
    exploded_df = dataFrame.select(explode(colName).alias(colName))

    return Hist1D(exploded_df, colName, nbins, range)
