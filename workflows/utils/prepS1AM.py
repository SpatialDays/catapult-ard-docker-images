from dateutil.parser import parse
import glob
import uuid
from sentinelsat import SentinelAPI
from zipfile import ZipFile
from pyproj import CRS

# TODO ADD TO REQUIREMENTS
from bs4 import BeautifulSoup
from workflows.utils.prep_utils import *

from workflows.utils.s1am.raw2ard import Raw2Ard


root = setup_logging()

def find_s1_uuid(s1_filename):
    """
    Returns S1 uuid required for download via sentinelsat, based upon an input S1 file/scene name. 
    I.e. S1A_MSIL1C_20180820T223011_N0206_R072_T60KWE_20180821T013410
    Assumes esa hub creds stored as env variables.
    
    :param S1_file_name: Sentinel-2 scene name
    :return S1_uuid: download id
    """
    copernicus_username = os.getenv("COPERNICUS_USERNAME")
    copernicus_pwd = os.getenv("COPERNICUS_PWD")
    logging.debug(f"ESA username: {copernicus_username}")
    esa_api = SentinelAPI(copernicus_username, copernicus_pwd)

    if s1_filename[-5:] == '.SAFE':
        res = esa_api.query(filename=s1_filename)
        res = esa_api.to_geodataframe(res)

        return res.uuid.values[0]


def download_extract_s1_esa(scene_uuid, down_dir, original_scene_dir):
    """
    Download a single S1 scene from ESA via sentinelsat 
    based upon uuid. 
    Assumes esa hub creds stored as env variables.
    
    :param scene_uuid: S1 download uuid from sentinelsat query
    :param down_dir: directory in which to create a downloaded product dir
    :param original_scene_dir: 
    :return: 
    """
    # if unzipped .SAFE file doesn't exist then we must do something
    if not os.path.exists(original_scene_dir):

        # if downloaded .zip file doesn't exist then download it
        zip_file_path = original_scene_dir.replace('.SAFE/', '.zip')
        if not os.path.exists(zip_file_path):
            logging.info('Downloading ESA scene zip: {}'.format(os.path.basename(original_scene_dir)))

            copernicus_username = os.getenv("COPERNICUS_USERNAME")
            copernicus_pwd = os.getenv("COPERNICUS_PWD")
            logging.debug(f"ESA username: {copernicus_username}")

            try:
                esa_api = SentinelAPI(copernicus_username, copernicus_pwd)
                esa_api.download(scene_uuid, down_dir, checksum=True)
            except Exception as e:
                raise DownloadError(f"Error downloading {scene_uuid} from ESA hub: {e}")

            # Seemingly we don't need this
            # logging.info('Unzipping ESA scene zip: {}'.format(os.path.basename(original_scene_dir)))
            # with ZipFile(zip_file_path, 'r') as zip_file:
            #     zip_file.extractall(os.path.dirname(down_dir))

    else:
        logging.warning('ESA scene already extracted: {}'.format(original_scene_dir))

    # # remove zipped scene but onliy if unzipped 
    # if os.path.exists(original_scene_dir) & os.path.exists(original_scene_dir.replace('.SAFE/', '.zip')):
    #     logging.info('Deleting ESA scene zip: {}'.format(original_scene_dir.replace('.SAFE/', '.zip')))
    #     os.remove(original_scene_dir.replace('.SAFE/', '.zip'))


def band_name_s1(prod_path):
    """
    Determine polarisation of individual product from product name
    from path to specific product file
    """

    prod_name = str(prod_path.split('/')[-1])

    if '-vh-' in str(prod_name):
        return 'vh'
    elif '-vv-' in str(prod_name):
        return 'vv'
    # TODO: Work needed to find new name 
    if 'LayoverShadow_MASK' in str(prod_name):
        return 'layovershadow_mask'

    logging.error(f"could not find band name for {prod_path}")

    return 'unknown layer'


def conv_s1scene_cogs(noncog_scene_dir, cog_scene_dir, scene_name, overwrite=False):
    """
    Convert S1 scene products to cogs [+ validate].
    """

    if not os.path.exists(noncog_scene_dir):
        logging.warning('Cannot find non-cog scene directory: {}'.format(noncog_scene_dir))

    # create cog scene directory - replace with one lined os.makedirs(exists_ok=True)
    if not os.path.exists(cog_scene_dir):
        logging.warning('Creating scene cog directory: {}'.format(cog_scene_dir))
        os.mkdir(cog_scene_dir)

    prod_paths = discover_tiffs(noncog_scene_dir)

    logging.info(f"found {len(prod_paths)} products to convert to cog from {noncog_scene_dir}")

    # iterate over prods to create parellel processing list
    for prod in prod_paths:
        out_filename = os.path.join(cog_scene_dir, scene_name + '_' + os.path.basename(prod)[:-4] + '.tif')  # - TO DO*****
        logging.info(f"converting {prod} to cog at {out_filename}")
        # ensure input file exists
        to_cog(prod, out_filename, nodata=-9999)


def read_manifest(path: str):
    manifest_path: str = os.path.join(path, 'manifest.safe')
    try:
        with open(manifest_path, 'r') as f:
            manifest: str = f.read()
    except FileNotFoundError as e:
        print(f"Manifest file {manifest_path} not found.")
        raise e
    return manifest


def extract_wkt_and_coordinates(manifest):
    soup = BeautifulSoup(manifest, 'xml')

    crs_name = soup.find('safe:footPrint')['srsName']
    coordinates_str = soup.find('gml:coordinates').text
    epsg_code = crs_name.split('#')[-1]
    spatial_ref = CRS.from_epsg(int(epsg_code))
    wkt = spatial_ref.to_wkt()

    coordinates = [tuple(map(float, coord.split(','))) for coord in coordinates_str.split()]
    return wkt, coordinates


def get_s1_geometry(path):
    manifest = read_manifest(path)
    wkt, coordinates = extract_wkt_and_coordinates(manifest)

    top = coordinates[2][0]
    left = coordinates[1][1]
    right = coordinates[3][1]
    bottom = coordinates[0][0]

    projection = {
        'geo_ref_points': {
            'ul': {
                'x': left,
                'y': top
            },
            'ur': {
                'x': right,
                'y': top
            },
            'll': {
                'x': left,
                'y': bottom
            },
            'lr': {
                'x': right,
                'y': bottom
            }
        },
        'spatial_reference': wkt
    }

    extent = {
        'll' : {
            'lat': bottom,
            'lon': left
        },
        'lr' : {
            'lat' : bottom,
            'lon' : right
        },
        'ul' : {
            'lat' : top,
            'lon' : left
        },
        'ur' : {
            'lat' : top,
            'lon' : right
        }
    }
    return projection, extent


def yaml_prep_s1(scene_dir, down_dir):
    """
    Prepare individual S1 scene directory containing S1 products
    note: doesn't inc. additional ancillary products such as incidence
    angle or layover/foreshortening masks
    """
    scene_name = scene_dir.split('/')[-2]

    # Get rid of the last part of the path
    scene_dir = scene_dir.split('/')[0:-1]
    scene_dir = '/'.join(scene_dir)
    logging.info("Scene path {}".format(scene_dir)) # /tmp/data/intermediate/S1A_IW_GRDH_1SDV_20230118T174108_tmp

    prod_paths = discover_tiffs(scene_dir)
    logging.debug(f"Found {len(prod_paths)} products")

    t0 = parse(str(datetime.strptime(scene_name.split("_")[-2], '%Y%m%dT%H%M%S')))

    # get polorisation from each image product (S1 band)
    # should be replaced with a more concise, generalisable parsing
    images = {
        band_name_s1(prod_path): {
            'path': prod_path
        } for prod_path in prod_paths
    }

    # read from manifest
    projection, extent = get_s1_geometry(down_dir)


    # format metadata (i.e. construct hashtable tree for syntax of file interface)
    return {
        'id': str(uuid.uuid5(uuid.NAMESPACE_URL, scene_name)),
        'processing_level': "sac_snap_ard",
        'product_type': "gamma0",
        'creation_dt': str(datetime.today().strftime('%Y-%m-%d %H:%M:%S')),
        'platform': {
            'code': 'SENTINEL_1'
        },
        'instrument': {
            'name': 'SAR'
        },
        'extent': create_metadata_extent(extent, t0, t0),
        'format': {
            'name': 'GeoTiff'
        },
        'grid_spatial': {
            'projection': projection
        },
        'image': {
            'bands': images
        },
        'lineage': {
            'source_datasets': {},
        }
    }


def prepare_S1AM(title, chunks=24, s3_bucket='public-eo-data', s3_dir='common_sensing/sentinel_1/', inter_dir='/tmp/data/intermediate/', **kwargs):
    in_scene = title
    """
    Prepare a Sentinel-1 scene (L1C or L2A) for indexing in ODC by converting it to COGs.

    :param in_scene: the name of the input Sentinel-1 scene, e.g. "S1A_IW_GRDH_1SDV_20211004T165352_20211004T165417_039025_049FA6_E5F6"
    :param chunks: the number of chunks to use when creating COGs (default is 24)
    :param s3_bucket: the S3 bucket where the scene is located (default is 'public-eo-data')
    :param s3_dir: the directory path within the S3 bucket where the scene is located (default is 'common_sensing/sentinel_1/')
    :param inter_dir: an optional intermediate directory to be used for processing (default is '/tmp/data/intermediate/')
    :return: None
    """

    tmp_inter_dir = inter_dir

    if not in_scene.endswith('.SAFE'):
        in_scene += '.SAFE'

    scene_name = in_scene[:32]
    inter_dir = f'{inter_dir}{scene_name}_tmp/'

    cog_dir = os.path.join(inter_dir, scene_name)
    os.makedirs(cog_dir, exist_ok=True)

    down_zip = inter_dir + in_scene.replace('.SAFE','.zip')
    am_dir = down_zip.replace('.zip', 'Orb_Cal_Deb_ML_TF_TC_dB/')
    down_dir = inter_dir + in_scene + '/'

    root.info(f'download dir: {down_dir}')
    try:

        # Download scene from ESA
        try:
            s1id = find_s1_uuid(in_scene)
            logging.debug(s1id)
            root.info(f"{in_scene} {scene_name}: Available for download from ESA")
            # download_extract_s1_esa(s1id, inter_dir, down_dir)
            root.info(f"{in_scene} {scene_name}: Downloaded from ESA")
        except Exception as e:
            root.exception(f"{in_scene} {scene_name}: Failed to download from ESA")
            raise DownloadError(f"Failed to download {in_scene} from ESA") from e


        # Download external DEMs
        ext_dem_path_list = download_external_dems(in_scene, scene_name, tmp_inter_dir, s3_bucket, root)

        # Process AM
        try:
            root.info(f"{in_scene} {scene_name} Starting AM SNAP processing")
            obj = Raw2Ard( chunks=chunks, gpt='/opt/snap/bin/gpt' )
            obj.process(down_zip, am_dir, ext_dem_path_list[0], ext_dem_path_list[1])
        except Exception as e:
            root.exception(e)

        # Convert scene to COGs in a temporary directory
        try:
            root.info(f"Converting {in_scene} to COGs")
            conv_s1scene_cogs(inter_dir, cog_dir, scene_name)
            root.info(f"Finished converting {in_scene} to COGs")
        except Exception as e:
            root.exception(f"Failed to convert {in_scene} to COGs")
            raise Exception(f"COG conversion error: {e}")


        # Create YAML metadata for the COGs
        try:
            root.info(f"Creating dataset YAML for {in_scene}")
            metadata = yaml_prep_s1(cog_dir, down_dir)
            create_yaml(cog_dir, metadata)
            root.info(f"Finished creating dataset YAML for {in_scene}")
        except Exception as e:
            root.exception(f"Failed to create dataset YAML for {in_scene}")
            raise Exception(f"YAML creation error: {e}")


        # Upload COGs to S3 bucket
        try:
            root.info(f"Uploading {in_scene} COGs to S3 bucket")
            cogs_to_upload = glob.glob(os.path.join(cog_dir, '*'))
            s3_upload_cogs(cogs_to_upload, s3_bucket, s3_dir)
            root.info(f"Finished uploading {in_scene} COGs to S3 bucket")
        except Exception as e:
            root.exception(f"Failed to upload {in_scene} COGs to S3 bucket")
            raise Exception(f"S3 upload error: {e}")


        # Delete temporary files
        # clean_up(inter_dir)

    except Exception as e:
        logging.error(f"could not process {scene_name} {e}")
        # clean_up(inter_dir)


if __name__ == '__main__':
    prepare_S1AM('S1A_IW_GRDH_1SDV_20230118T174108_20230118T174131_046841_059DD6_8B3D')
