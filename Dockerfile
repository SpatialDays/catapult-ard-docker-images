FROM python:3.7-buster

RUN apt-get update -y
RUN apt-get install fonts-dejavu fontconfig -y

RUN wget --quiet http://step.esa.int/downloads/9.0/installers/esa-snap_sentinel_unix_9_0_0.sh
RUN /bin/sh ./esa-snap_sentinel_unix_9_0_0.sh -q
RUN rm ./esa-snap_sentinel_unix_9_0_0.sh



RUN pip install   asynchronousfilereader==0.2.1 
RUN pip install  redis==4.5.1 
RUN pip install  google-api-python-client==2.80.0 
RUN pip install google-cloud-storage==2.7.0 
RUN pip install xmltodict==0.13.0 
RUN pip install beautifulsoup4==4.11.2 
RUN pip install lxml==4.9.2 
RUN pip install  boto3 
RUN pip install  pyyaml 
RUN pip install  rasterio 
RUN pip install hdmedians
RUN pip install matplotlib
RUN pip install pandas==1.3.5 
RUN pip install requests
RUN pip install scikit-learn
RUN pip install xarray
RUN pip install geopandas
RUN pip install pyproj==2.6.1.post1
RUN pip install sentinelsat
RUN apt-get install libgdal-dev -y #install just to get the dependencies
RUN apt-get install cmake -y
RUN apt-get install libsqlite3-dev -y
RUN apt-get install sqlite -y

RUN wget https://github.com/OSGeo/PROJ/releases/download/6.2.0/proj-6.2.0.tar.gz 
RUN tar -xvf proj-6.2.0.tar.gz
# change to proj directory
WORKDIR /proj-6.2.0
RUN mkdir build
WORKDIR /proj-6.2.0/build
RUN cmake ..
RUN make install
RUN ldconfig
# go back to root directory
WORKDIR /

RUN wget https://download.osgeo.org/gdal/3.0.2/gdal-3.0.2.tar.gz
RUN ldconfig
RUN tar -xvf gdal-3.0.2.tar.gz
# change to gdal directory
WORKDIR /gdal-3.0.2
# configure
ENV LD_LIBRARY_PATH=/usr/local/lib 
RUN CPPFLAGS=-I/usr/local/include LDFLAGS=-L/usr/local/lib ./configure --with-proj=/usr/local --disable-shared --without-libtool
RUN make -j$(nproc)
RUN make install
RUN ldconfig
WORKDIR /
RUN pip install GDAL==$(gdal-config --version | awk -F'[.]' '{print $1"."$2}')

RUN wget --quiet http://step.esa.int/thirdparties/sen2cor/2.8.0/Sen2Cor-02.08.00-Linux64.run
RUN chmod +x Sen2Cor-02.08.00-Linux64.run
RUN ./Sen2Cor-02.08.00-Linux64.run 

COPY update_snap.sh .
RUN chmod +x update_snap.sh
RUN ./update_snap.sh

RUN mkdir -p /opt/snap/bin && ln -s /usr/local/bin/snap /opt/snap/bin/snap
RUN mkdir -p /opt/snap/bin && ln -s /usr/local/snap/bin/gpt /opt/snap/bin/gpt

COPY workflows/utils/s1am/snap/bin/gpt.vmoptions /opt/snap/bin
COPY workflows/utils/s1am/snap/etc/snap.auxdata.properties /opt/snap/etc

COPY . /app
WORKDIR /app