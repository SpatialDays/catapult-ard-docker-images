""" Being used to process both S1 and S1AM data - the seperating statement is in raw2ard.py """

from dateutil.parser import parse
import glob
import uuid
from sentinelsat import SentinelAPI
from zipfile import ZipFile
from pyproj import CRS
from osgeo import gdal

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

    else:
        logging.warning('ESA scene already extracted: {}'.format(original_scene_dir))


def band_name_s1(prod_path):
    """
    Determine polarisation of individual product from product name
    from path to specific product file
    """

    prod_name = str(prod_path.split('/')[-1])

    print('PROD NAME:', prod_name)

    if 'LayoverShadow_MASK' in str(prod_name):
        print('layovershadow_mask found')
        return 'layovershadow_mask'
    if 'VH' in str(prod_name):
        print('vh found')
        return 'vh'
    if 'VV' in str(prod_name):
        print('vv found')
        return 'vv'

    logging.error(f"could not find band name for {prod_path}")

    return 'unknown layer'


##### from AM code
# def conv_s1scene_cogs(noncog_scene_dir, cog_scene_dir, scene_name, overwrite=False):
#     """
#     # Convert S1 scene products to cogs [+ validate].
#     """

#     if not os.path.exists(noncog_scene_dir):
#         logging.warning('Cannot find non-cog scene directory: {}'.format(noncog_scene_dir))

#     # create cog scene directory - replace with one lined os.makedirs(exists_ok=True)
#     if not os.path.exists(cog_scene_dir):
#         logging.warning('Creating scene cog directory: {}'.format(cog_scene_dir))
#         os.mkdir(cog_scene_dir)

#     prod_paths = discover_tiffs(noncog_scene_dir)

#     logging.info(f"found {len(prod_paths)} products to convert to cog from {noncog_scene_dir}")

#     # iterate over prods to create parellel processing list
#     for prod in prod_paths:
#         out_filename = os.path.join(cog_scene_dir, scene_name + '_' + os.path.basename(prod)[:-4] + 'tif')  # - TO DO*****
#         logging.info(f"converting {prod} to cog at {out_filename}")
#         # ensure input file existsscene_cogs
#         to_cog(prod, out_filename, nodata=-9999)

def conv_s1scene_cogs(noncog_scene_dir, cog_scene_dir, scene_name, fiji_AM=False, overwrite=False): # REMOVE fiji_AM=False IF THIS WORKS
    """
    Convert S1 scene products to cogs [+ validate].
    """

    if not os.path.exists(noncog_scene_dir):
        logging.warning(f'Cannot find non-cog scene directory: {noncog_scene_dir}')

    # create cog scene directory - replace with one lined os.makedirs(exists_ok=True)
    if not os.path.exists(cog_scene_dir):
        logging.warning(f'Creating scene cog directory: {cog_scene_dir}')
        os.mkdir(cog_scene_dir)

        # if fiji_AM:
        #     # cog_scene_dir_east = f'{cog_scene_dir}_E'
        #     # cog_scene_dir_west = f'{cog_scene_dir}_W'
        #     if '_E' in cog_scene_dir:
        #         logging.warning(f'Creating scene cog directories for am fiji: {cog_scene_dir_east} and {cog_scene_dir_west}')
        #         os.mkdir(cog_scene_dir)
        #         os.mkdir(cog_scene_dir_west)
        # else:
        #     logging.warning(f'Creating scene cog directory: {cog_scene_dir}')
        #     os.mkdir(cog_scene_dir)



    des_prods = ["Gamma0_VV_db",
                 "Gamma0_VH_db",
                 "LayoverShadow_MASK_VH"]  # to ammend once outputs finalised - TO DO*****

    # find all individual prods to convert to cog (ignore true colour images (TCI))
    prod_paths = glob.glob(noncog_scene_dir + '*TF_TC*/*.img')  # - TO DO*****
    prod_paths = [x for x in prod_paths if os.path.basename(x)[:-4] in des_prods]

    root.info(f"ALL PROD_PATHS: {prod_paths}")

    # CHECK IF east or west in the prod_path
    # if fiji_AM:
    #     for prod in prod_paths:
    #         root.info(f'CHECKING IF EAST OR WEST IS IN THE PROD NAME: {prod}')
    #         if 'east' in prod:
    #             out_filename = os.path.join(cog_scene_dir,
    #                                 scene_name + '_' + os.path.basename(prod)[:-4] + '.tif')  # - TO DO*****
    #             logging.info(f"converting {prod} to cog at {out_filename}")
    #             # ensure input file exists
    #             to_cog(prod, out_filename, nodata=-9999)
    #         elif 'west' in prod:
    #             out_filename = os.path.join(cog_scene_dir,
    #                                 scene_name + '_' + os.path.basename(prod)[:-4] + '.tif')  # - TO DO*****
    #             logging.info(f"converting {prod} to cog at {out_filename}")
    #             # ensure input file exists
    #             to_cog(prod, out_filename, nodata=-9999)    
    #         else:
    #             root.info(f'ERROR: the prod {prod} crosses the AM but contains neither "east" or "west"')           
    # else:
    #     for prod in prod_paths:
    #         out_filename = os.path.join(cog_scene_dir,
    #                                 scene_name + '_' + os.path.basename(prod)[:-4] + '.tif')  # - TO DO*****
    #         logging.info(f"converting {prod} to cog at {out_filename}")
    #         # ensure input file exists
    #         to_cog(prod, out_filename, nodata=-9999)

    # REFINING THE PROD PATHS BY EAST, WEST AND NONE
    # prod_paths_east = []
    # prod_paths_west = []
    # for prod in prod_paths:
    #     if 'east' in prod:



    # iterate over prods to create parellel processing list
    for prod in prod_paths:
        # NEED TO ONLY GET THE PRODS WITH THE EAST FOR EAST DIR ETC
        root.info(f'the prod is: {prod}')
        if '_E' in cog_scene_dir and 'east' in prod: 
            out_filename = os.path.join(cog_scene_dir,
                                    scene_name + '_' + os.path.basename(prod)[:-4] + '.tif')  # - TO DO*****
            logging.info(f"converting {prod} to cog at {out_filename}")
            # ensure input file exists
            to_cog(prod, out_filename, nodata=-9999)
        elif '_W' in cog_scene_dir and 'west' in prod:
            out_filename = os.path.join(cog_scene_dir,
                                    scene_name + '_' + os.path.basename(prod)[:-4] + '.tif')  # - TO DO*****
            logging.info(f"converting {prod} to cog at {out_filename}")
            # ensure input file exists
            to_cog(prod, out_filename, nodata=-9999)
        else:
            root.info(f'prod: {prod} doesnt cross AM')
            out_filename = os.path.join(cog_scene_dir,
                                    scene_name + '_' + os.path.basename(prod)[:-4] + '.tif')  # - TO DO*****
            logging.info(f"converting {prod} to cog at {out_filename}")
            # ensure input file exists
            to_cog(prod, out_filename, nodata=-9999)


    # # iterate over prods to create parellel processing list
    # for prod in prod_paths:
    #     out_filename = os.path.join(cog_scene_dir,
    #                                 scene_name + '_' + os.path.basename(prod)[:-4] + '.tif')  # - TO DO*****
    #     logging.info(f"converting {prod} to cog at {out_filename}")
    #     # ensure input file exists
    #     to_cog(prod, out_filename, nodata=-9999)

def copy_s1_metadata(out_s1_prod, cog_scene_dir, scene_name):
    """
    Parse through S2 metadtaa .xml for either l1c or l2a S2 scenes.
    """

    if os.path.exists(out_s1_prod):

        meta_base = os.path.basename(out_s1_prod)  # THIS IS TOO LONG FOR AM CORSSING TIFFS
        # n_meta = os.path.join(cog_scene_dir + '/' + scene_name + '_' + meta_base)

        if 'east' in out_s1_prod or 'west' in out_s1_prod:
            n_meta = os.path.join(cog_scene_dir + '/' + meta_base)
        else:
            n_meta = os.path.join(cog_scene_dir + '/' + scene_name + '_' + meta_base)

        logging.info("Copying original metadata file to cog dir: {}".format(n_meta))
        if not os.path.exists(n_meta):
            shutil.copyfile(out_s1_prod, n_meta)
        else:
            logging.info("Original metadata file already copied to cog_dir: {}".format(n_meta))
    else:
        logging.warning("Cannot find orignial metadata file: {}".format(out_s1_prod))


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


def get_s1_geometry(path, hemisphere=None):

    # needs to get the east and west coors for fiji-AM - GET coordinates and wkt
    if hemisphere:
        # find east vv or vh tiff to get the coors
        # get the first tiff from the dir
        print('S1AM')
        print('PATH IS:', path)
        for root, dirs, files in os.walk(path):
            print('FILES ARE: ', files)
            tiff_list = []
            file_list = []
            for file in files:
                print('WHAT ', os.path.join(root, file))
                file_list.append(os.path.join(root, file))
                # print('FILE IS: ', file)
                for file in file_list:
                    if 'tif' in file:
                        tiff_list.append(file)
        print(f'TIFF LIST: {tiff_list}')
        
        print(f'OPENING: {tiff_list[0]}')
        data = gdal.Open(f'{tiff_list[0]}')
        # data = tiff_list[0]
        print(f'DATA IS: {data}')
        geoTransform = data.GetGeoTransform()
        left = geoTransform[0]  # minx 
        top = geoTransform[3]  # maxy
        right = left + geoTransform[1] * data.RasterXSize  # maxx
        bottom = top + geoTransform[5] * data.RasterYSize  # miny

        wkt =  gdal.Dataset.GetProjection(data)# how to get wkt from east/west?
    else:
        # original method
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


def yaml_prep_s1(scene_dir, down_dir, hemisphere=None): # hemisphere=None
    """
    Prepare individual S1 scene directory containing S1 products
    note: doesn't inc. additional ancillary products such as incidence
    angle or layover/foreshortening masks
    """
    scene_name = scene_dir.split('/')[-2]

    logging.info("Scene path {}".format(scene_dir))

    # need to only discover east and west tiffs if hemisphere exists
    prod_paths = discover_tiffs(scene_dir, hemisphere)  # gets all the tiffs
    logging.debug(f"Found {len(prod_paths)} products")

    # original method
    # prod_paths = discover_tiffs(scene_dir)  # gets all the tiffs
    # logging.debug(f"Found {len(prod_paths)} products")

    t0 = parse(str(datetime.strptime(scene_name.split("_")[-2], '%Y%m%dT%H%M%S')))

    # get polorisation from each image product (S1 band)
    # should be replaced with a more concise, generalisable parsing
    images = {
        band_name_s1(prod_path): {
            'path': prod_path.split("/")[-1]
        } for prod_path in prod_paths
    }

    # read from manifest
    if hemisphere:
        # path is to the specific east/west tiff
        projection, extent = get_s1_geometry(scene_dir, hemisphere=hemisphere)
        # create id
        short_scene_name = scene_name[:-4]
        if hemisphere == 'east':
            hemisphere_scene_name = short_scene_name + 'east'
        else:
            hemisphere_scene_name = short_scene_name + 'west'
        
        scene_id = str(uuid.uuid5(uuid.NAMESPACE_URL, hemisphere_scene_name))

    else:
        # oringial method
        projection, extent = get_s1_geometry(down_dir)
        # creating id(uuid format)
        scene_id = str(uuid.uuid5(uuid.NAMESPACE_URL, scene_name))


    # format metadata (i.e. construct hashtable tree for syntax of file interface)
    return {
        'id': str(scene_id),  # original: 'id': str(uuid.uuid5(uuid.NAMESPACE_URL, scene_name))
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



def prepare_S1AM(title, region, chunks=24,s3_bucket='public-eo-data', s3_dir='common_sensing/sentinel_1/', inter_dir='/tmp/data/intermediate/', **kwargs):
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

    ############ COMMENTING OUT DURING YAML TESTING
    # print('YAML TESTING')
    # process_start_time = datetime.now().strftime("%H:%M:%S")  
    # print(f'prepS1AM code starting at: {process_start_time}')

    print(f'STARTING S1 PREP FOR {title} in {region}')
    process_start_time = datetime.now().strftime("%H:%M:%S")  
    print(f'prepS1AM code starting at: {process_start_time}')

    tmp_inter_dir = inter_dir

    if not in_scene.endswith('.SAFE'):
        in_scene += '.SAFE'

    scene_name = in_scene[:32]
    inter_dir = f'{inter_dir}{scene_name}_tmp/'

    cog_dir = os.path.join(inter_dir, scene_name)
    os.makedirs(cog_dir, exist_ok=True)

    down_zip = inter_dir + in_scene.replace('.SAFE','.zip')
    down_dir = inter_dir + in_scene + '/'

    # print('DOWN ZIP:', down_zip)
    # print('IN SCENE:', in_scene)
    # print('SCENE NAME:', scene_name)


    # root.info(f'download dir: {down_dir}')
    ########## UNCOMMENT AFTER YAML TESTING
    try:
        # Download scene from ESA
        try:
            s1id = find_s1_uuid(in_scene)
            logging.debug(s1id)
            root.info(f"{in_scene} {scene_name}: Available for download from ESA")
            download_extract_s1_esa(s1id, inter_dir, down_dir)
            root.info(f"{in_scene} {scene_name}: Downloaded from ESA")
        except Exception as e:
            root.exception(f"{in_scene} {scene_name}: Failed to download from ESA")
            raise DownloadError(f"Failed to download {in_scene} from ESA") from e


        print('DOWNLOADED SCENE')
        ####### MOVE THIS INSIDE THE S1 AM CODE (NOT USED FOR NON-AM)
        # Download external DEMs

        ext_dem_path_list_local = download_external_dems(region, in_scene, scene_name, tmp_inter_dir, s3_bucket, root)
        print('EXT DEM PATH LIST:', ext_dem_path_list_local)  # PASS INTO PROCESS AND INSIDE PROCESS SPLIT INTO E AND W

        # defining the tmp paths for the DEMs (since the external urls cannot be found)

        # Process for AM and non-AM (if statement in raw2ard.py)
        try:
            root.info(f"{in_scene} {scene_name} Starting SNAP processing")
            snap_gpt = os.getenv("GPT_PATH", '/opt/snap/bin/gpt')  # TRY '/esa-snap_sentinel_unix_6_0/bin/gpt' INSTEAD OF THE OPT PATH  # '/opt/snap/bin/gpt' 
            root.info(f'----------PATH TO SNAP GPT: {snap_gpt}')
            obj = Raw2Ard( chunks=chunks, gpt=snap_gpt )  # TEST PATH: gpt='/home/spatialdaysubuntu/esa_snap/bin/gpt')
            print('BEGINNING PREPROCESSING')
            print('inter_dir: ', inter_dir)
            out_prods = obj.process(s3_bucket, in_scene, down_zip, inter_dir, ext_dem_path_list_local, region)
        except Exception as e:
            root.exception(e)


        ##############################

        # checking if the process was for s1 or s1AM scene
        if out_prods[0] == 'S1':
            product_type = 'S1'
            print('S1 OUT PRODUCTS')
            print(f'prepS1 code started at: {process_start_time}')
            print(f'prepS1 preprocessing for scene finished at: {datetime.now().strftime("%H:%M:%S")}')
            out_prod1 = out_prods[1]
            out_prod2 = out_prods[2]
        else:
            product_type = 'S1AM'
            print('S1AM MOSAICS')
            print(f'prepS1AM code started at: {process_start_time}')
            print(f'prepS1AM preprocessing for AM scene finished at: {datetime.now().strftime("%H:%M:%S")}')
            out_prod1 = out_prods[1]  # vv EAST mosaic
            out_prod2 = out_prods[2]  # vh EAST mosaic
            out_prod3 = out_prods[3]  # vv WEST mosaic
            out_prod4 = out_prods[4]  # vh WEST mosaic
            print(f'out_prod1: {out_prod1}')
            print(f'out_prod2: {out_prod2}')
            print(f'out_prod3: {out_prod3}')
            print(f'out_prod4: {out_prod4}')
            # creating alternate cog dir paths
            cog_dir_east = f'{cog_dir}_E'
            cog_dir_west = f'{cog_dir}_W'


        ######## FOR YAML TESTING, TRY TO RUN JUST FROM HERE - COMMENTING OUT THE ABOVE
        # product_type = 'S1AM'
        # cog_dir_east = f'{cog_dir}_E'
        # cog_dir_west = f'{cog_dir}_W'
        #############

        root.info('COG CONVERTING')
        try:
            # root.info(f"{in_scene} {scene_name} Converting COGs")
            # conv_s1scene_cogs(inter_dir, cog_dir, scene_name)
            # root.info(f"{in_scene} {scene_name} COGGED")

            if product_type == 'S1AM':
                # cog_scene_dir_east, cog_scene_dir_west
                root.info(f"Fiji-AM Converting COGs - with cog dir: {cog_dir}")
                conv_s1scene_cogs(inter_dir, cog_dir_east, scene_name, fiji_AM=True)  # need to split into E and W if fiji-AM
                conv_s1scene_cogs(inter_dir, cog_dir_west, scene_name, fiji_AM=True)
                root.info("Fiji-AM scene COGGED")
            else:
                root.info(f"{in_scene} {scene_name} Converting COGs")
                conv_s1scene_cogs(inter_dir, cog_dir, scene_name)
                root.info(f"{in_scene} {scene_name} COGGED")


        except Exception as e:
            root.exception(f"{in_scene} {scene_name} COG conversion FAILED")
            raise Exception('COG Error', e)

            # PARSE METADATA TO TEMP COG DIRECTORY**
        root.info('PARSE METADATA TO TEMP COG DIRECTORY')
        try:
            root.info(f"{in_scene} {scene_name} Copying original METADATA")
            if product_type == 'S1AM':
                root.info('COPYING METADATA FOR S1AM')
                copy_s1_metadata(out_prod1, cog_dir_east, scene_name)
                copy_s1_metadata(out_prod2, cog_dir_east, scene_name)
                copy_s1_metadata(out_prod3, cog_dir_west, scene_name)
                copy_s1_metadata(out_prod4, cog_dir_west, scene_name)
            else:
                root.info('COPYING METADATA FOR S1')
                copy_s1_metadata(out_prod1, cog_dir, scene_name)
                copy_s1_metadata(out_prod2, cog_dir, scene_name)

            # ORIGINAL METHOD
            # copy_s1_metadata(out_prod1, cog_dir, scene_name)
            # copy_s1_metadata(out_prod2, cog_dir, scene_name)
            # # four products in totoal for Am crossing imagery
            # if product_type == 'S1AM':
            #     copy_s1_metadata(out_prod3, cog_dir, scene_name)
            #     copy_s1_metadata(out_prod4, cog_dir, scene_name)

            root.info(f"{in_scene} {scene_name} COPIED original METADATA")
        except Exception as e:
            root.exception(f"{in_scene} {scene_name} MTD not coppied")
            raise e
        
        ######## UNCOMMENT AFTER YAML TESTING

        # GENERATE YAML WITHIN TEMP COG DIRECTORY**
        print('GENERATE YAML WITHIN TEMP COG DIRECTORY')
        try:
            # original method
            # root.info(f"{in_scene} {scene_name} Creating dataset YAML")
            # create_yaml(cog_dir, yaml_prep_s1(cog_dir, down_dir))
            # root.info(f"{in_scene} {scene_name} Created original METADATA")

            if product_type == 'S1AM':
                root.info('Creating yamls for fiji East and West')
                create_yaml(cog_dir_east, yaml_prep_s1(cog_dir_east, down_dir, hemisphere='east'))  # creating yaml for east 
                create_yaml(cog_dir_west, yaml_prep_s1(cog_dir_west, down_dir, hemisphere='west'))  # creating yaml for west
                root.info('Yamls for fiji East and West have been created')
            else:
                root.info(f"{in_scene} {scene_name} Creating dataset YAML")
                create_yaml(cog_dir, yaml_prep_s1(cog_dir, down_dir))  # for all scenes that arent AM fiji 
                root.info(f"{in_scene} {scene_name} Created original METADATA")

        except Exception as e:
            root.exception(f"{in_scene} {scene_name} Dataset YAML not created")
            raise Exception('YAML creation error', e)

            # MOVE COG DIRECTORY TO OUTPUT DIRECTORY
        print('MOVE COG DIRECTORY TO OUTPUT DIRECTORY')
        try:
            root.info(f"{in_scene} {scene_name} Uploading to S3 Bucket")
            if product_type == 'S1AM':
                root.info('Uploading fiji AM EAST scene to S3 Bucket')
                s3_upload_cogs(glob.glob(os.path.join(cog_dir_east, '*')), s3_bucket, s3_dir)
                root.info('Uploading fiji AM WEST scene to S3 Bucket')
                s3_upload_cogs(glob.glob(os.path.join(cog_dir_west, '*')), s3_bucket, s3_dir)
            else:
                s3_upload_cogs(glob.glob(os.path.join(cog_dir, '*')), s3_bucket, s3_dir)

            # original method
            # s3_upload_cogs(glob.glob(os.path.join(cog_dir, '*')), s3_bucket, s3_dir)
            root.info(f"{in_scene} {scene_name} Uploaded to S3 Bucket")
        except Exception as e:
            root.exception(f"{in_scene} {scene_name} Upload to S3 Failed")
            raise Exception('S3  upload error', e)

    except Exception as e:
        logging.error(f"could not process {scene_name} {e}")
    finally:
        test_env = os.getenv('TEST_ENV', False)
        if test_env:
            logging.info(f"finished without clean up")  # FOR TESTING
        else:
            logging.info(f"cleaning up {inter_dir}")
            clean_up(inter_dir)


if __name__ == '__main__':
    prepare_S1AM('S1A_IW_GRDH_1SDV_20170724T174037_20170724T174100_017616_01D7A7_F0DA', 'fiji',s3_bucket = "ard-bucket")

    # region vars: 'solomon', 'fiji', 'vanuatu' ('default' or any other value for snap default dem)

    # non-AM test: S1A_IW_GRDH_1SDV_20230118T174108_20230118T174131_046841_059DD6_8B3D
    # AM test: S1A_IW_GRDH_1SDV_20230218T173255_20230218T173312_047293_05AD04_C398

    # Catapult comparision test AM: S1A_IW_GRDH_1SDV_20150502T063207_20150502T063219_005738_0075E6_7273
    # Catapult comparision test AM: S1A_IW_GRDH_1SDV_20170328T063214_20170328T063226_015888_01A309_0E7F  # USE FOR TESTING
    # Catapult comparision test non-AM: S1A_IW_GRDH_1SDV_20170724T174037_20170724T174100_017616_01D7A7_F0DA  # USE FOR TESTING

    # Catapult comparision for VANUATU: S1A_IW_GRDH_1SDV_20150321T181252_20150321T181321_005133_006778_E3CC  # USE FOR TESTING
    # Catapult comparision for VANUATU: S1A_IW_GRDH_1SDV_20150508T072136_20150508T072201_005826_0077EE_7582
    # Catapult comparision for SOLOMON ISLANDS: S1B_IW_GRDH_1SDV_20170920T075631_20170920T075656_007472_00D314_F2FA  # USE FOR TESTING
