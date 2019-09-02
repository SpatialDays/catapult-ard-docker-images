FROM continuumio/miniconda3:4.7.10

LABEL maintainer="Luigi Di Fraia"

RUN conda install --quiet --yes \
    boto3 \
    geopandas \
    pyyaml \
    rasterio \
    && conda clean --all -f -y

RUN pip install --no-cache-dir \
    google-cloud-storage \
    redis \
    sentinelsat

RUN wget --quiet http://step.esa.int/thirdparties/sen2cor/2.8.0/Sen2Cor-02.08.00-Linux64.run && \
    /bin/sh ./Sen2Cor-02.08.00-Linux64.run && \
    rm ./Sen2Cor-02.08.00-Linux64.run

RUN apt-get install -y --no-install-recommends xmlstarlet && \
    rm -rf /var/lib/apt/lists/*

RUN xmlstarlet edit -L -u "//Downsample_20_to_60" -v "FALSE" $HOME/sen2cor/2.8/cfg/L2A_GIPP.xml

#CMD [ "/bin/bash" ]

RUN conda install --quiet --yes \
    jupyter \
    && conda clean --all -f -y && \
    mkdir /opt/notebooks

COPY utils /opt/notebooks/utils

COPY rediswq.py /opt/notebooks

COPY worker.ipynb /opt/notebooks

CMD jupyter notebook \
    --allow-root \
    --notebook-dir=/opt/notebooks \
    --NotebookApp.ip='0.0.0.0' \
    --NotebookApp.port='8888' \
    --NotebookApp.token='secretpassword' \
    --NotebookApp.open_browser='False'
