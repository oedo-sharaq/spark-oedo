# spark-oedo
Apache spark libraries for OEDO/SHARAQ data analysis

## Installation
### with Anaconda
1. Install Anaconda (miniconda3)
  ```
  wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
  sh Miniconda3-latest-Linux-x86_64.sh
  ```
Answer "yes" to auto setup to setup the environment on login. `source .bashrc` to launch the environment.
Disable it by `conda config --set auto_activate false`.

2. Create environment for pyspark.
Requirements are listed in environment.yaml.
  ```
  conda env create -f environment.yaml
  conda activate spark-examples
  ```
Try `pyspark` to check if the environment is ready. <br>

environment.yaml has a channel option `nodefaults` so it should install packages from conda-forge, not from defaults.
You can run `conda config --remove channels defaults --system` to avoid using the defaults channel in future `conda install`.
Anaconda is not free for the commercial use. Please check their ToS when you use the defaults channel.

3. Setup conda environment to load setting for spark-oedo at activation.
  ```
  echo source $PWD/setup.sh >> $CONDA_PREFIX/etc/conda/activate.d/env_vars.sh
  ```
This will source setup.sh at `conda activate`

4. Install and setup Scala to the conda environment.
  ```
  curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs install scala:2.13.16 scalac:2.13.16 --install-dir ./temp_bin && ./cs setup --install-dir ./temp_bin && mv ./cs $CONDA_PREFIX/bin/ && mv ./temp_bin/* $CONDA_PREFIX/bin/ && rm -r ./temp_bin
  ```
5. Compile scala_package
  ```
  cd scala_package
  sbt package
  ```
### without Anaconda
1. Install python packages by pip
  ```
  pip install pyspark=4.0.1 pandas pyarrow numpy jupyter notebook matplotlib plotly scipy ipympl
  ```
2. Install JDK
`apt install openjdk-21-jdk` or download and extract the package from Eclipse Temurin site. <br>
Set `JAVA_HOME` and `PATH=$JAVA_HOME/bin:$PATH` to your environment
3. Setup environment
  ```
  source setup.sh
  ```
4. Follow the step 4. and 5. of installation guide with Anaconda

## Usages
### RIDF decoder
- `ridf_to_parquet.py [input_ridf_file] [output_parquet_file]`: It will create a parquet file with ridf blocks as a byte array.
- `ridf_parquet_processor.py [input_ridf_file] [output_parquet_file]`: It will create a parquet file with ridf segdata as a byte array. `run`, `event_number`, `timestamp`, `fp`, `dev`, `det`, `mod` will be decoded.

### Module decoders

### hist
- sparkHist1d.py: Functions for 1D histogramming using spark
  - Hist1D(): Generate a 1D histogram of a column
  - Hist1DArray(): Generate a 1D histogram of a column which stores an array of values
- sparkHist2d.py: Functions for 2D histogramming using spark
  - Hist2D(): Generate a 2D histogram of the correlation between two columns
  - Hist2DArray(): Generate a 2D histogram of the correlation between two columns that stores arrays with the same size in a same row.
  - Hist2DArrayVsPos(): Generate a 2D histogram of the array value vs array pos of the column that stores an array.
- fitHist1d.py: Funcitions for curve fitting of a 1D histogram
  - FitHist1DGauss(): Initiate an iteractive curve fit widget on previously plotted 1D histogram with Gaussian + linear function in a Jupyter notebook.
    ```
    %matplotlib widget
    Hist1D(df, "colname", 1000, [0, 1000])
    FitHist1DGauss()
    ```
    
   
