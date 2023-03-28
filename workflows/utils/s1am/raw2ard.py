import os
import re
import sys
import math
import copy
from copy import deepcopy
import shutil
import xmltodict
import logging

from . import metadata
from . import utility

from workflows.utils.prep_utils import *

from osgeo import gdal
from pathlib import Path
from datetime import datetime
from collections import OrderedDict
from . densifygrid import DensifyGrid

import pdb

class Raw2Ard:

    def __init__( self, chunks=6, gpt='/opt/snap/bin/gpt' ):

        """
        constructor function
        """

        with open ( 'workflows/utils/s1am/recipes/cs_base.xml' ) as fd:
            self._pt1 = xmltodict.parse( fd.read() )

        self._densify = DensifyGrid()
        self._fat_swath = 10.0

        self._gpt = gpt
        self._chunks = chunks

        return


    def create_source_bands(self, bands):
        result = ""
        for x in bands:
            result = result + "Gamma0_" + x.upper() + ","

        print(f'CREATED SOURCE BANDS: {result[:-1]}')
        return result[:-1]  # takes the last comma off the end


    def create_selected_polarisations(self, bands):
        result = ""
        for x in bands:
            result = result + x.upper() + ", "
        
        print('CREATED SELECTED POLS:', result[:-2])

        return result[:-2]  # takes the last comma off the end


    def available_bands(self, source):
        """
        Finds which bands are available
        """

        if '1SSV' in source:
            print('found vv')
            return ['vv']
        if '1SDV' in source:
            print('found vv and vh')
            return ['vh', 'vv']
        raise Exception("unknown source type")


    def process ( self, s3_bucket, in_scene, scene, out_path, ext_dem_path_list_local, region, args=None  ):

        """
        entry point to class functionality
        """

        ext_dem = ext_dem_path_list_local
        scene_name = in_scene[:32]

        # update arguments
        self.getArguments( args )
        tmp_path = out_path
        dataset_files = utility.unpackFiles( scene, '(.*?)', tmp_path )

        # load metadata into dictionary from manifest.safe file and annotation xml files
        meta = metadata.getManifest( utility.matchFile( dataset_files, '.*\/manifest.safe' ) )
        meta.update( metadata.getAnnotation( utility.matchFile( dataset_files, '.*\/annotation\/s1.*vv.*\.xml' ) ) )
        
        # overall output product scene name (final str describes the applied SNAP operators from cs_base.xml)
        product = meta[ 'product' ]
        nm = 'S1{}_{}_{}_{}'.format(  product[ 'satellite' ], 
                                        product[ 'mode' ],
                                        meta[ 'acquisition' ][ 'start' ].strftime( '%y%m%dT%H%M%S' ),
                                        'bnr_orb_cal_ml_tf_tc_db' )
        outname = os.path.join( tmp_path, nm )
        
        # determining if scene crosses antemeridian
        extent = self.getSceneExtent( meta )
        if extent[ 'lon' ][ 'max' ] - extent[ 'lon' ][ 'min' ] > self._fat_swath:

            logging.info('THE CURRENT IMAGE CROSSES THE AM')

            E_DEM = ext_dem_path_list_local[0]
            W_DEM = ext_dem_path_list_local[1]

            # densify annotated geolocation grid - CURRENT STUCK POINT FOR AM IMAGERY
            self._densify.process( utility.matchFiles( dataset_files, '.*\/annotation\/s1.*\.xml' ), grid_pts=250 )  # grid_pts=250
            meta.update( metadata.getGeolocationGrid( utility.matchFile( dataset_files, '.*\/annotation\/s1.*vv.*\.xml' ) ) )

            # split gcps into east / west sub-groups
            logging.info('SPLITTING EAST AND WEST GCPS')
            gcps = self.splitGcps( meta[ 'gcps' ] )  # gcps = the ground points on the scene
            chunk_size = int ( math.ceil ( float ( meta[ 'image' ][ 'lines' ] ) / float ( self._chunks ) ))
    
            # process subset blocks either side of antemeridian
            logging.info('PROCESSING EITHER SIDE OF THE AM')
            for hemisphere in [ 'east', 'west' ]:

                results = []  # redefined per hemisphere
                logging.info(f'PROCESSING: {hemisphere}')
                logging.info(f'TIME NOW: {datetime.now().strftime("%H:%M:%S")}')

                # for each row block, ensuring subsets overlap
                start_row = 0; offset = 10
                while start_row < meta[ 'image' ][ 'lines' ]:

                    # derive subset parameters
                    block = {   'start' : max( start_row - offset, 0 ),
                                'end' : min ( start_row + chunk_size + offset, meta[ 'image' ][ 'lines' ] - 1 ),
                                'samples' : meta[ 'image' ][ 'samples' ],
                                'lines' : meta[ 'image' ][ 'lines' ] }

                    subset = self.getSubset( gcps[ hemisphere ], block )

                    logging.info(f'SUBSET: {subset}')  # x,y coors of the subset in gcps

                    # setting unq subset name
                    subset_name = '_'.join( str ( int( x ) ) for x in subset )

                    # schema = cs_base.xml processing graph
                    schema = deepcopy( self._pt1 ) 
                    
                    # set parameters of reader task
                    logging.info('SET PARAMETERS OF READER TASK')
                    param = self.getParameterSet( schema, 'Read' )
                    param[ 'file' ] = dataset_files[ 0 ] + 'manifest.safe'
                    param[ 'formatName' ] = 'SENTINEL-1'

                    # insert subset task
                    logging.info('INSERT SUBSET TASK')
                    schema = self.insertNewTask( schema, 'Subset', after='Read' )
                    param = self.getParameterSet ( schema, 'Subset' )
                    param[ 'geoRegion' ] = ''
                    
                    # copy subset values into schema dictionary
                    logging.info('COPYING SUBSET VALUES INTO SCEMA DICT')
                    param = self.getParameterSet ( schema, 'Subset' )
                    param[ 'region' ] = ','.join( str ( int( x ) ) for x in subset )

                    # ext dem input file
                    logging.info('SORTING OUT THE EXT DEMS FOR TF PROCESS')
                    param = self.getParameterSet ( schema, 'Terrain-Flattening' )
                    if hemisphere == 'west':
                        param[ 'externalDEMFile' ] = W_DEM
                    elif hemisphere == 'east':
                        param[ 'externalDEMFile' ] = E_DEM

                    # ext dem input file
                    logging.info('SORTING OUT THE EXT DEMS FOR TC PROCESS')
                    param = self.getParameterSet ( schema, 'Terrain-Correction' )            
                    if hemisphere == 'west':
                        param[ 'externalDEMFile' ] = W_DEM
                    elif hemisphere == 'east':
                        param[ 'externalDEMFile' ] = E_DEM
                        
                    # create subset-specific output path            
                    param = self.getParameterSet ( schema, 'Write' )   
                    param['formatName'] = 'ENVI'
                    outname_pt1 = os.path.join( outname, 'subset_'+ subset_name + '_Orb_Cal_Deb_ML_TF_TC_dB')
                    param[ 'file' ] = outname_pt1
                    results.append( param['file'] )

                    logging.info(f'PARAMS: {param}')

                    # transform dict back to xml schema & save serialised xml schema to file
                    logging.info('TRANSFORMING DICT BACK TO XML SCHEMA AND SAVE SERIALISED XML SCHEMA')
                    out = xmltodict.unparse( schema, pretty=True )
                    # path to the xml file to be executed for preprocessing
                    cfg_pathname = os.path.join ( tmp_path, '{}.xml'.format( os.path.basename(outname_pt1) ) )
                    with open( cfg_pathname, 'w+') as file:
                        print(f'xml file: ', file)
                        file.write(out)

                    # executing PT1 processing for subset
                    logging.info( f'PROCESSING PT1 {hemisphere} SUBSET: {subset_name}' )
                    out, err, code = utility.execute( self._gpt, [ cfg_pathname ] ) # status code of 0 => successful run
                    
                    logging.info('----------------------------------------------')
                    err_str = err.decode("utf-8")
                    err_msg = err_str.split('\n')
                    # log of any errors/warnings from snap (feedback during subset processing)
                    logging.info ( f'SNAP ERR MSG:' )
                    for line in err_msg:
                        logging.info(line)
                    # snap output messages
                    logging.info ( f'SNAP OUTPUT: { out }' )
                    logging.info('----------------------------------------------')
                    logging.info('PROCESSED SUBSET, MOVING ONTO NEXT')

                    # move onto next block
                    start_row += chunk_size

                # getting each hemisphere's mosaic in each polarisation and their paths
                if hemisphere == 'east':
                    vv_east_mosaic_path = self.generateImage( out_path, results, 'VV', scene_name, hemisphere )
                    vh_east_mosaic_path = self.generateImage( out_path, results, 'VH', scene_name, hemisphere )
                else:
                    vv_west_mosaic_path = self.generateImage( out_path, results, 'VV', scene_name, hemisphere )
                    vh_west_mosaic_path = self.generateImage( out_path, results, 'VH', scene_name, hemisphere )

            return ['S1AM', vv_east_mosaic_path, vh_east_mosaic_path, vv_west_mosaic_path, vh_west_mosaic_path]

        else:

            # normal S1 preprocessing with snap gpt
            logging.info('THE CURRENT IMAGE DOES NOT CROSSES THE AM')
            logging.info('STARTING SNAP GPT PROCESSING')

            # setting up s1-specific relative inputs/paths
            scene_name = in_scene[:32]
            input_mani = tmp_path + in_scene + '/manifest.safe'
            inter_prod1 = tmp_path + scene_name + '_Orb_Cal_Deb_ML.dim'
            inter_prod1_dir = inter_prod1[:-4] + '.data/'
            inter_prod2 = tmp_path + scene_name + '_Orb_Cal_Deb_ML_TF.dim'
            inter_prod2_dir = inter_prod2[:-4] + '.data/'
            out_prod1 = tmp_path + scene_name + '_Orb_Cal_Deb_ML_TF_TC_dB.dim'
            out_dir1 = out_prod1[:-4] + '.data/'
            out_prod2 = tmp_path + scene_name + '_Orb_Cal_Deb_ML_TF_TC_lsm.dim'
            out_dir2 = out_prod2[:-4] + '.data/'
            down_dir = tmp_path + in_scene + '/'
    
            snap_gpt = os.getenv("GPT_PATH", '/opt/snap/bin/gpt')  # set env var for testing or use default
            int_graph_1 = 'workflows/utils/cs_s1_pt1_bnr_Orb_Cal_ML.xml'

            # find out which region we're looking at, for discovering external dems (if any)
            avaliable_regions = ['fiji', 'vanuatu', 'solomon']  # TO DO - MAKE THIS AN ENV VAR (see also prep_utils.download_external_dems())

            if region.lower() in avaliable_regions:
                if region.lower() == 'fiji':
                    # find out if the image is in east or west and use that dem
                    logging.info('THE META AOI - TO FIND LONGITUDE')
                    logging.info(meta[ 'aoi' ][1][1])
                    # if any longitude in aoi is -ve => west hemisphere => need west dem
                    if meta[ 'aoi' ][1][1] < 0:
                        logging.info(meta[ 'aoi' ][1][1], ' is negative therefore the image is in the western hemisphere')
                        ext_dem = ext_dem_path_list_local[1] 
                    else:
                        logging.info(meta[ 'aoi' ][1][1], ' is positive therefore the image is in the eastern hemisphere')
                        ext_dem = ext_dem_path_list_local[0]
                        logging.info(f'E_DEM: {ext_dem}')
                else:
                    # any of the non-fiji regions
                    ext_dem = ext_dem_path_list_local[0]
                    logging.info(f'USING EXT DEM: {ext_dem}')
            else:
                ext_dem = None
                logging.info('SETTING THE EXT_DEM TO NONE. SNAP DEFAULT DEM WILL BE USED IN PREPROCESSING')

            if ext_dem:
                logging.info('EXTERNAL DEMS BEING USED')
                ext_dem_path = ext_dem
                int_graph_2 = 'workflows/utils/cs_s1_pt2A_TF.xml'
                int_graph_3 = 'workflows/utils/cs_s1_pt3A_TC_db.xml'
                int_graph_4 = 'workflows/utils/cs_s1_pt4A_Sm_Bm_TC_lsm.xml'
            else:  # when ext_dem is None or not found
                logging.info('SNAP DEFAULT DEMS BEING USED')
                int_graph_2 = 'workflows/utils/without_external_dems/cs_s1_pt2A_TF.xml'
                int_graph_3 = 'workflows/utils/without_external_dems/cs_s1_pt3A_TC_db.xml'
                int_graph_4 = 'workflows/utils/without_external_dems/cs_s1_pt4A_Sm_Bm_TC_lsm.xml'
            
            logging.info('{} {} Starting'.format(in_scene, scene_name))
            
            # the S1 preprocessing steps with snap gpt
            try:
                logging.info('FINDING AVALIBLE BANDS')

                # Figure out what bands are available.
                bands = self.available_bands(in_scene)

                # cmd contains the path to gpt, the graph xml file to use, and the params to pass into the file
                cmd = [
                    snap_gpt,
                    int_graph_1,
                    f"-Pinput_grd={input_mani}",
                    f"-Poutput_ml={inter_prod1}",
                    f"-Psource_bands={self.create_selected_polarisations(bands)}"
                ]
                # optional params: "-c" and "4" - set the number of cores to use for snap multiprocessing (doesnt seem to speed up the process though)

                logging.info(cmd)
                run_snap_command(cmd)
                logging.info(f"{in_scene} {scene_name} PROCESSED to MULTILOOK starting PT2")
                logging.info(f'external dem: {ext_dem}')

                # processing steps if an external dem was found
                if ext_dem:

                    logging.info('PREPROCESSING WITH EXTERNAL DEM')

                    cmd = [
                        snap_gpt,
                        int_graph_2,
                        f"-Pinput_ml={inter_prod1}",
                        f"-Pext_dem={ext_dem_path}",
                        f"-Poutput_tf={inter_prod2}"
                    ]

                    logging.info(cmd)
                    run_snap_command(cmd)
                    logging.info(f"{in_scene} {scene_name} PROCESSED to TERRAIN FLATTEN starting PT3")

                    # processes the TF image with TC and db
                    cmd = [
                        snap_gpt,
                        int_graph_3,
                        f"-Pinput_tf={inter_prod2}",
                        f"-Pext_dem={ext_dem_path}",
                        f"-Poutput_db={out_prod1}",
                        f"-Psource_bands={self.create_source_bands(bands)}"
                    ]

                    logging.info(cmd)
                    run_snap_command(cmd)
                    logging.info(f"{in_scene} {scene_name} PROCESSED to dB starting PT4")

                    cmd = [
                        snap_gpt,
                        int_graph_4,
                        f"-Pinput_tf={inter_prod2}",
                        f"-Pext_dem={ext_dem_path}",
                        f"-Poutput_ls={out_prod2}"
                    ]
                
                    logging.info(cmd)
                    run_snap_command(cmd)
                    logging.info(f"{in_scene} {scene_name} PROCESSED to lsm starting COG conversion")

                else:  
                    # processing with snap's default dems

                    logging.info('USING SNAP DEFAULT DEMS')
                    
                    cmd = [
                        snap_gpt,
                        int_graph_2,
                        f"-Pinput_ml={inter_prod1}",
                        f"-Poutput_tf={inter_prod2}"
                    ]

                    logging.info(cmd)
                    run_snap_command(cmd)
                    logging.info(f"{in_scene} {scene_name} PROCESSED to TERRAIN FLATTEN starting PT3")

                    # processes the TF image with TC and db
                    cmd = [
                        snap_gpt,
                        int_graph_3,
                        f"-Pinput_tf={inter_prod2}",
                        f"-Poutput_db={out_prod1}",
                        f"-Psource_bands={self.create_source_bands(bands)}"
                    ] 

                    logging.info(cmd)
                    run_snap_command(cmd)
                    logging.info(f"{in_scene} {scene_name} PROCESSED to dB starting PT4")

                    cmd = [
                        snap_gpt,
                        int_graph_4,
                        f"-Pinput_tf={inter_prod2}",
                        f"-Poutput_ls={out_prod2}"
                    ]
                
                    logging.info(cmd)
                    run_snap_command(cmd)
                    logging.info(f"{in_scene} {scene_name} PROCESSED to lsm starting COG conversion")


            except Exception as e:
                logging.critical(e, exc_info=True) 
                logging.info('SNAP GPT PREPROCESSING FAILED')
        
            return ['S1', out_prod1, out_prod2]
    

    def getArguments( self, args ):

        """
        parse supplied arguments or setup defaults
        """

        if args:
            # copy args if passed to constructor
            self._remove_border_noise = args.remove_border_noise
            self._remove_thermal_noise = args.remove_thermal_noise
            self._terrain_flattening = args.terrain_flattening
            self._geocoding = args.geocoding
            self._polarizations = args.polarizations
            self._target_resolution = args.target_resolution
            self._external_dem = args.external_dem
            self._scaling = args.scaling

        else:
            # default values
            self._remove_border_noise = True
            self._remove_thermal_noise = True
            self._terrain_flattening = True
            self._geocoding = 'Range-Doppler'
            self._polarizations = ['VV', 'VH' ]
            self._target_resolution = 20.0
            self._external_dem = None
            self._scaling = 'db'

        return


    def getTask ( self, schema, name ):

        """
        get task sub-schema
        """

        # locate node schema corresponding to name
        node = None

        for obj in schema[ 'graph' ][ 'node' ]:
            if obj [ '@id' ] == name: 
                node = obj.copy()

        return node


    def getParameterSet ( self, schema, name ):

        """
        get parameter-set within task schema
        """

        # locate parameter schema corresponding to task node
        node = None

        obj = self.getTask( schema, name )
        if obj is not None:
            node = obj[ 'parameters' ]

        return node


    def insertNewTask ( self, schema, name, after=None ):

        """
        insert new task into pipeline schema
        """

        # get xml schema for new task
        with open ( os.path.join( 'workflows/utils/s1am/recipes/nodes', name + '.xml'  )) as fd:
            new_task = xmltodict.parse( fd.read() )[ 'node' ]

        # create new ordered dict
        update = deepcopy( schema )
        update[ 'graph' ][ 'node' ].clear()
 
        # copy nodes into deep copy 
        nodes = []; last_task = None
        for obj in schema[ 'graph' ][ 'node' ]:

            # insert new task
            if last_task is not None:
                if after is not None and last_task[ 'operator' ] == after: 
                    nodes.append( new_task )

            nodes.append( obj ); last_task = obj

        # add to list end
        if after is None:
            nodes.append( new_task[ 'node' ] )

        # update source product values 
        prev_task = None
        for obj in nodes:

            if prev_task is not None:
                obj[ 'sources' ] = OrderedDict ( [ ( 'sourceProduct', OrderedDict( [ ( '@refid', prev_task[ 'operator' ] ) ] ) ) ] )
                
            prev_task = obj
        
        # add nodes to updated ordered dict
        update[ 'graph' ][ 'node' ] = nodes
        return update


#     def buildSchema ( self, schema, meta ):

#         """
#         initialise pipeline configuration schema
#         """

#         #### optionally insert border noise removal #####
#         if self._remove_border_noise:

#             insert task and update parameters
#             schema = self.insertNewTask( schema, 'Remove-GRD-Border-Noise', after='Read' )
#             param = self.getParameterSet ( schema, 'Remove-GRD-Border-Noise' )
#             param['selectedPolarisations'] = ','.join( self._polarizations )

#         #### optionally insert thermal noise removal #####
#         if self._remove_thermal_noise:

#             schema = self.insertNewTask( schema, 'ThermalNoiseRemoval', after='Read' )
#             param = self.getParameterSet ( schema, 'ThermalNoiseRemoval' )
#             param['selectedPolarisations'] = ','.join( self._polarizations )

#         #### update arguments for calibration task #####
#         param = self.getParameterSet ( schema, 'Calibration' )
#         param['selectedPolarisations'] = ','.join( self._polarizations )
#         param['sourceBands'] = ','.join ( ['Intensity_' + x for x in self._polarizations ] )

#         #### optionally insert terrain flattening #####
#         if self._terrain_flattening:

#             schema = self.insertNewTask( schema, 'Terrain-Flattening', after='Calibration' )
#             param = self.getParameterSet ( schema, 'Terrain-Flattening' )

# #             param['sourceBands'] = ','.join ( ['Beta0_' + x for x in self._polarizations ] )
# #             param['reGridMethod'] = True if self._external_dem is None else False
#             pred_tc = 'Terrain-Flattening'

#         else:

#             # update calibration output bands
#             param['outputBetaBand'] = False
#             param['outputGammaBand'] = True
#             pred_tc = 'Calibration'

#         #### insert terrain correction task #####
#         if self._geocoding == 'Range-Doppler':

#             # range doppler
#             schema = self.insertNewTask( schema, 'Terrain-Correction', after=pred_tc )
#             param = self.getParameterSet ( schema, 'Terrain-Correction' )
# #             param['sourceBands'] = ','.join( ['Gamma0_' + x for x in self._polarizations ] )

#         elif self._geocoding == 'Simulation-Cross-Correlation':

#             # simulation cross correlation
#             schema = self.insertNewTask( schema, 'SAR-Simulation', after=pred_tc )
#             schema = self.insertNewTask( schema, 'Cross-Correlation', after='SAR-Simulation' )
#             schema = self.insertNewTask( schema, 'SARSim-Terrain-Correction', after='Cross-Correlation' )

#             param = self.getParameterSet ( schema, 'SAR-Simulation' )
# #             param['sourceBands'] = ','.join( [ 'Gamma0_' + x for x in self._polarizations ] )

#         else:

#             # invalid geocoding configuration
#             raise ValueError ( 'Invalid geocoding configuration {}'.format( geocoding ) )

#         ##### insert multilooking task #####
#         schema = self.insertNewTask( schema, 'Multilook', after='Calibration' )
#         looks = self.getMultiLookParameters( meta )

#         param = self.getParameterSet ( schema, 'Multilook' )
#         param['nRgLooks'] = looks[ 'range' ]
#         param['nAzLooks'] = looks[ 'azimuth' ]
        
#         # set up source bands
#         cal_param = self.getParameterSet ( schema, 'Calibration' )
# #         if cal_param['outputBetaBand'] == 'true':
# #             param['sourceBands'] = ','.join( ['Beta0_' + x for x in self._polarizations ] )

# #         elif cal_param['outputGammaBand'] == 'true':
# #             param['sourceBands'] = ','.join( ['Gamma0_' + x for x in self._polarizations ] )

#         ##### insert unit conversion task #####
#         if self._scaling in ['dB', 'db']:
#             source = 'Terrain-Correction' if self._geocoding == 'Range-Doppler' else 'SARSim-Terrain-Correction'

#             schema = self.insertNewTask( schema, 'LinearToFromdB', after=source )            
#             param = self.getParameterSet ( schema, 'LinearToFromdB' )
# #             param['sourceBands'] = ','.join( ['Gamma0_' + x for x in self._polarizations ] )

#         ##### write task #####
#         param = self.getParameterSet ( schema, 'Write' )
#         param['formatName'] = 'BEAM-DIMAP'

#         ##### TODO - intermediate products #####
#         ##### TODO - dem configuration #####
#         ##### TODO - interpolation methods #####


#         return schema


#     def getMultiLookParameters ( self, meta ):

#         """
#         convert target resolution into looks
#         """

#         looks = {}

#         # pixel spacing and target spatial resolution
#         sp_range = meta[ 'pixel_spacing' ][ 'range' ]
#         sp_azimuth  = meta[ 'pixel_spacing' ][ 'azimuth' ]

#         tr_range = self._target_resolution
#         tr_azimuth = self._target_resolution

#         # handle slant range
#         if meta[ 'projection' ] == 'Slant Range':

#             # compute ground range resolution and range looks
#             gr_ps = sp_range / ( math.sin( math.radians( meta[ 'incidence_mid_swath' ] ) ) )
#             looks[ 'range' ] = int(math.floor( float( tr_range ) / gr_ps ) )

#         elif meta[ 'projection' ] == 'Ground Range':

#             # compute range looks
#             looks[ 'range' ] = int(math.floor( float(tr_range) / sp_range ) )

#         else:
#             raise ValueError( 'Invalid parameter value : {}'.format( meta[ 'projection' ] ) )

#         # compute the azimuth looks
#         looks[ 'azimuth' ] = int(math.floor(float(tr_azimuth) / sp_azimuth))

#         # set the look factors to 1 if they were computed to be 0
#         looks[ 'range' ] = looks[ 'range' ] if looks[ 'range' ] > 0 else 1
#         looks[ 'azimuth' ] = looks[ 'azimuth' ] if looks[ 'azimuth' ] > 0 else 1

#         return looks


    def getOutName( self, schema_part, meta ):

        """
        derive file identifier from pipeline / dataset
        """
            
        Lut = { 'pt0': 'bnr_orb_cal_ml_tf_tc_db',
                'pt1': 'bnr_orb_cal_ml',
                'pt2': 'bnr_orb_cal_ml_tf',
                'pt3': 'bnr_orb_cal_ml_tf_tc_db',
                'pt4': 'bnr_orb_cal_ml_tf_tc_ls'
              }        
        product = meta[ 'product' ]

        nm = 'S1{}_{}_{}_{}'.format(  product[ 'satellite' ], 
                                        product[ 'mode' ],
                                        meta[ 'acquisition' ][ 'start' ].strftime( '%y%m%dT%H%M%S' ),
                                        Lut[ schema_part ] )
        print( f'OUTNAME - {schema_part}: {nm}' )
        return nm
        
        
#         def stringifyPipeline( schema ):

#             """
#             create operator shortname string tokenised by underscores
#             """

#             # operator shortnames
#             Lut = { 'Remove-GRD-Border-Noise': 'bnr',
#                     'ThermalNoiseRemoval': 'tnr',
#                     'Apply-Orbit-File': 'Orb',
#                     'Calibration': 'Cal',
#                     'Multilook': 'ML',
#                     'Terrain-Flattening': 'TF',
#                     'Terrain-Correction': 'TC',
#                     'LinearToFromdB' : 'db' }

#             ops = []

#             # return op shortnames separated with underscore
#             for obj in schema[ 'graph' ][ 'node' ]:

#                 if obj[ 'operator' ] in Lut:
#                     ops.append( Lut[ obj[ 'operator' ] ] )

#             return '_'.join( ops )            

#         # append dataset parameters to op shortname string
#         product = meta[ 'product' ]
#         return 'S1{}_{}_{}_{}'.format(  product[ 'satellite' ], 
#                                         product[ 'mode' ],
#                                         meta[ 'acquisition' ][ 'start' ].strftime( '%y%m%dT%H%M%S' ),
#                                         stringifyPipeline( schema ) )


    def getSceneExtent( self, meta ):

        """
        determine scene bounding box in geographic coordinates
        """

        # initialise min / max 
        min_lon = 1e10; min_lat = 1e10
        max_lon = -1e10; max_lat = -1e10

        # each point in meta coordinates
        for pt in meta[ 'aoi' ]:

            min_lat = min ( min_lat, pt[ 0 ] )
            min_lon = min ( min_lon, pt[ 1 ] )

            max_lat = max ( max_lat, pt[ 0 ] )
            max_lon = max ( max_lon, pt[ 1 ] )

        # return limits
        return { 'lon' : { 'min' : min_lon, 'max' : max_lon },
                 'lat' : { 'min' : min_lat, 'max' : max_lat } }


    def splitGcps( self, gcps ):

        """
        sort gcps crossing antemeridian into list of lists ordered by row
        """

        # create dictionary for result
        obj = { 'west' : [[]], 
                'east' : [[]] }

        # for each gcp in geolocation grid
        prev_row = 0
        for gcp in gcps:

            # create new list for new gcp line
            if gcp.GCPLine != prev_row:
                obj[ 'west' ].append( [] ); obj[ 'east' ].append( [] )
                prev_row = gcp.GCPLine

            # append to list dependent on longitude signage
            if gcp.GCPX < 0.0:
                obj[ 'west' ][ -1 ].append( gcp )
            else:
                obj[ 'east' ][ -1 ].append( gcp )

        return obj


    def getSubset( self, gcps, block  ):

        """
        get interpolation safe subset dimensions
        """

        def getLineRange( gcps, block ):

            """
            get row range
            """

            # get geolocation grid lines encompassing block
            lines = {}; prev_row = 0
            for idx, gcp_row in enumerate( gcps ):

                if gcp_row[ 0 ].GCPLine > block[ 'start' ] and 'min' not in lines:
                    lines[ 'min' ] = idx - 1

                if gcp_row[ 0 ].GCPLine > block[ 'end' ] and 'max' not in lines:
                    lines[ 'max' ] = idx

            if 'max' not in lines:
                lines[ 'max' ] = len( gcps ) - 1

            return lines

        # geolocation grid extent
        lines = getLineRange( gcps, block )

        subset = {  'y1' : block[ 'start' ],
                    'y2' : block[ 'end' ] }

        for idx in range( lines[ 'min' ], lines[ 'max' ] + 1 ):

            # from rightmost column furthest from meridian
            if int( gcps[ idx ][ 0 ].GCPPixel ) == 0:

                # find leftmost gcp column within line range
                if 'x2' not in subset:
                    subset[ 'x2' ] = int ( gcps[ idx ][ -2 ].GCPPixel )

                subset[ 'x2' ] = int ( min( subset[ 'x2' ], gcps[ idx ][ -2 ].GCPPixel ) )

            else:

                # find leftmost column furthest from meridian
                if 'x1' not in subset:
                    subset[ 'x1' ] = int ( gcps[ idx ][ 1 ].GCPPixel )

                subset[ 'x1' ] = int( max( subset[ 'x1' ], gcps[ idx ][ 1 ].GCPPixel ) )

        # define remaining subset coordinates
        if 'x1' not in subset: 
            subset[ 'x1' ] = 0.0

        if 'x2' not in subset: 
            subset[ 'x2' ] = ( block[ 'samples' ] - 1 ) - subset[ 'x1' ]

        return [ subset[ 'x1' ], subset[ 'y1' ], subset[ 'x2' ], subset[ 'y2' ] - subset[ 'y1' ] ]


    def generateImage( self, out_path, results, pol, scene_name, hemisphere ):

        """
        combine subset output images into single mosaic 

        out_path => specific tmp path within the tmp dir
        """

        logging.info('COMBINING SUBSET OUTPUT IMAGES INTO SINGLE MOSAIC')
       
        # find subset images
        images = []
        for result in results:

            files = list( Path( result ).rglob( '*{}*.img'.format( pol ) ) )
            if len( files ) == 1:
                images.append( str ( files[ 0 ] ) )

        # use gdal warp to create mosaic
        kwargs = { 'format': 'GTiff', 'srcNodata' : 0.0, 'dstSRS' : 'epsg:4326' }  # SHOULD NODATA VALUE BE 0.0 OR -9999.0?
        pathname = os.path.join( out_path, f'{scene_name}_{hemisphere}_Gamma0_{pol}_db.tif' )

        ds = gdal.Warp( pathname, images, **kwargs )  # (output dataset name/object, array of dataset objects or filenames, kwargs)
        print('WHAT IS DS:', ds)
        del ds

        return pathname


