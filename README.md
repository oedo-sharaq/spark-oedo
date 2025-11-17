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
