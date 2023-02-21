FROM continuumio/miniconda3:4.8.2

LABEL maintainer="Luigi Di Fraia"

RUN conda update conda --quiet --yes \
    && conda clean --all -f -y \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete

RUN conda install --quiet --yes \
    boto3 \
    geopandas \
    pip \
    pyyaml \
    rasterio \
    && conda clean --all -f -y \
    && find /opt/conda/ -follow -type f -name '*.a' -delete \
    && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    && find /opt/conda/ -follow -type f -name '*.js.map' -delete

RUN pip install --no-cache-dir \
    asynchronousfilereader \
    redis \
    google-api-python-client \
    google-cloud-storage \
    sentinelsat \
    xmltodict \
    beautifulsoup4 \
    pyproj \
    lxml

RUN wget http://step.esa.int/thirdparties/sen2cor/2.8.0/Sen2Cor-02.08.00-Linux64.run
RUN chmod +x Sen2Cor-02.08.00-Linux64.run
RUN ./Sen2Cor-02.08.00-Linux64.run 
RUN pip install lxml

COPY . /app
WORKDIR /app