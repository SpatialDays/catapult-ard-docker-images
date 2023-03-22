# FROM debian:latest
# RUN sed -i '/updates/d' /etc/apt/sources.list
# RUN apt-get update
# CMD /bin/sh
# RUN apt-get install fontconfig --yes

# FROM continuumio/miniconda3:4.8.2
FROM continuumio/miniconda3:22.11.1

RUN apt-get update -y
RUN apt-get install fonts-dejavu fontconfig -y

# ENV DISPLAY=host_name:0.0

# RUN wget --quiet http://step.esa.int/downloads/9.0/installers/esa-snap_sentinel_unix_9_0_0.sh \
#     && /bin/sh ./esa-snap_sentinel_unix_9_0_0.sh -q \
#     && rm ./esa-snap_sentinel_unix_9_0_0.sh \
#     && /opt/snap/bin/snap --nosplash --nogui --modules --update-all

RUN wget --quiet http://step.esa.int/downloads/9.0/installers/esa-snap_sentinel_unix_9_0_0.sh
RUN /bin/sh ./esa-snap_sentinel_unix_9_0_0.sh -q
RUN rm ./esa-snap_sentinel_unix_9_0_0.sh
# CMD 2>&1 | while read -r line; do echo "$line" [ "$line" = "updates=0" ] && sleep 2 && pkill -TERM -f "snap/jre/bin/java" done
# RUN /opt/snap/bin/snap --nosplash --nogui --modules --update-all

RUN conda install --quiet --yes \
    # boto3 \
    # geopandas \
    pip 
    # pyyaml \
    # rasterio \
    # && conda clean --all -f -y \
    # && find /opt/conda/ -follow -type f -name '*.a' -delete \
    # && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
    # && find /opt/conda/ -follow -type f -name '*.js.map' -delete

RUN pip install --no-cache-dir \
    asynchronousfilereader==0.2.1 \
    redis==4.5.1 \
    google-api-python-client==2.80.0 \
    google-cloud-storage==2.7.0 \
    xmltodict==0.13.0 \
    beautifulsoup4==4.11.2 \
    lxml==4.9.2 \
    boto3 \
    pyyaml \
    rasterio 

# RUN conda update conda --quiet --yes \
#     && conda clean --all -f -y \
#     && find /opt/conda/ -follow -type f -name '*.a' -delete \
#     && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
#     && find /opt/conda/ -follow -type f -name '*.js.map' -delete

# RUN conda install --quiet --yes \
#     boto3 \
#     geopandas \
#     hdmedians \
#     matplotlib \
#     pandas \
#     pip \
#     pyyaml \
#     rasterio \
#     requests \
#     scikit-learn \
#     xarray \
#     # && conda clean --all -f -y \
#     # && find /opt/conda/ -follow -type f -name '*.a' -delete \
#     # && find /opt/conda/ -follow -type f -name '*.pyc' -delete \
#     # && find /opt/conda/ -follow -type f -name '*.js.map' -delete

RUN conda install boto3
RUN conda install hdmedians
RUN conda install matplotlib
RUN conda install pandas=1.3.5 python=3.8
# RUN conda install pandas=1.3.5 python=3.8
# RUN conda install geopandas
RUN conda install requests
RUN conda install scikit-learn
RUN conda install xarray
RUN pip install geopandas
RUN pip install pyproj==2.6.1.post1
RUN pip install sentinelsat
RUN conda install -c conda-forge gdal=3.0.2
RUN conda install -c conda-forge libiconv

# RUN pip install --no-cache-dir \
#     asynchronousfilereader \
#     redis 

RUN wget --quiet http://step.esa.int/thirdparties/sen2cor/2.8.0/Sen2Cor-02.08.00-Linux64.run
RUN chmod +x Sen2Cor-02.08.00-Linux64.run
RUN ./Sen2Cor-02.08.00-Linux64.run 
# RUN pip install lxml

# RUN wget --quiet http://step.esa.int/downloads/7.0/installers/esa-snap_sentinel_unix_7_0.sh \
#     && /bin/sh ./esa-snap_sentinel_unix_7_0.sh -q \
#     && rm ./esa-snap_sentinel_unix_7_0.sh \
#     && /opt/snap/bin/snap --nosplash --nogui --modules --update-all

COPY workflows/utils/s1am/snap/bin/gpt.vmoptions /opt/snap/bin
COPY workflows/utils/s1am/snap/etc/snap.auxdata.properties /opt/snap/etc

COPY . /app
WORKDIR /app