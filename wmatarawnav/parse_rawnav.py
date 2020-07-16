# -*- coding: utf-8 -*-
"""
Create by: abibeka, wytimmerman
Created on Thu Mar 26 10:09:45 2020
Purpose: Functions for processing rawnav data
"""

import zipfile, re, numpy as np, pandas as pd, io, os, shutil, glob
import pandasql as ps
from zipfile import BadZipfile
import geopandas as gpd
from shapely.geometry import Point
from pandas.io.parsers import ParserError

# FIXME : Change all functions below to snake_case---refactor code
# Parent Functions
########################################################################################################################

def get_zipped_files_from_zip_dir(zip_dir_list, zipped_files_dir_parent, glob_search="*.zip"):
    '''
    Get the list of files to read from Zipped folder. Also Unzip the parent folder.
    Will Unzip only once. Can also pass a list of paths to unzipped folders
    to achieve the same result.
    Parameters
    ----------
    zip_dir_list : List or str,
        List of zipped directories with Rawnav data
        or a single zipped directory with Rawnav data.
    zipped_files_dir_parent: str
        Parent folder where list of zipped directories with Rawnav data is kept.
        List of zipped directories with Rawnav data would be unzipped to this parent folder.
    glob_search: str
        Default value of "*.zip" will search for all .zip files. Can specify a pariticular file
        name pattern for debugging. 
    Raises
    ------
    IOError
        Input was not a string or a list. This function doesn't
        handle tuples.
    Returns
    -------
    List of Trip files (zipped) that need to be read.
    '''
    if isinstance(zip_dir_list, list):
        'do nothing'
    elif isinstance(zip_dir_list, str):
        zip_dir_list = [zip_dir_list]
    else:
        raise IOError("zip_dir_list should be a string or a List of directory")
    file_universe = []
    for ZipDir in zip_dir_list:
        if not os.path.exists(ZipDir.split('.zip')[0]):
            with zipfile.ZipFile(ZipDir, 'r') as zip:
                zip.extractall(zipped_files_dir_parent)
        zip_dir1 = ZipDir.split('.zip')[0]  # Will work even for Unzipped folders
        list_files = glob.glob(os.path.join(zip_dir1, glob_search))
        file_universe.extend(list_files)
    return file_universe


def find_rawnav_routes(file_universe, nmax=None, quiet=True):
    '''   
    Parameters
    ----------
    file_universe : str
        Path to zipped folder with rawnav text file. 
        i.e., rawnav02164191003.txt.zip
        Assumes that included text file has the same name as the zipped file,
        minus the '.zip' extension.
        Note: For absolute paths, use forward slashes.
    nmax : int, optional
        limit files to read to this number. If None, all zip files read.
    quiet : boolean, optional
        Whether to print status. The default is True.

    Returns
    -------
    ReturnDict.
    '''
    file_universe_set = file_universe[0:nmax]
    # Setup dataframe for iteration
    file_universe_df = pd.DataFrame({'fullpath': file_universe_set})
    file_universe_df['filename'] = file_universe_df['fullpath'].str.extract('(rawnav\d+.txt)')
    file_universe_df['file_busid'] = file_universe_df['fullpath'].str.extract('rawnav(\d{5})\S+.txt')
    file_universe_df['file_id'] = file_universe_df['fullpath'].str.extract('rawnav(\d+).txt')
    file_universe_df['file_busid'] = pd.to_numeric(file_universe_df['file_busid'])
    
    # Get Tags and Reformat
    file_universe_df['taglist'] = [find_all_tags(path, quiet=quiet) for path in file_universe_df['fullpath']]
    file_universe_df = file_universe_df.explode('taglist')
    file_universe_df[['line_num', 'route_pattern', 'tag_busid', 'tag_date', 'tag_time', 'Unk1', 'mi_to_ft']] = \
        file_universe_df['taglist'].str.split(',', expand=True)
    file_universe_df[['route', 'pattern']] = \
        file_universe_df['route_pattern'].str.extract('^(?:\s*)(?:(?!PO))(?:(?!PI))(?:(?!DH))(\S+)(\d{2})$')
    
    # Convert Column Types and Create new ones
    # Note that we leave some cols as text, as integer values don't support
    # NAs in Pandas. Some of this conversion will occur later once 'empty' tags are removed
    file_universe_df['tag_busid'] = pd.to_numeric(file_universe_df['tag_busid'])
    file_universe_df['tag_datetime'] = file_universe_df['tag_date'] + ' ' + file_universe_df['tag_time']
    file_universe_df['tag_datetime'] = pd.to_datetime(file_universe_df['tag_datetime'],
                                                      infer_datetime_format=True, errors='coerce')
    file_universe_df['tag_starthour'] = file_universe_df['tag_time'].str.extract('^(?:\s*)(\d{2}):')
    file_universe_df['tag_starthour'] = pd.to_numeric(file_universe_df['tag_starthour'])
    # Note that this will put some late Friday trips after 12AM into the "Saturday" bucket
    #   should probably revisit at a later date to adjust trips before 4 AM to day before.
    file_universe_df['tag_date'] = pd.to_datetime(file_universe_df['tag_date'], infer_datetime_format=True)
    file_universe_df['wday'] = file_universe_df['tag_date'].dt.day_name()
    file_universe_df['line_num_next'] =\
        file_universe_df.groupby(['filename'], sort = False)['line_num'].shift(-1)
    
    #Reorder Cols
    column_nm_map = ['fullpath', 
                     'filename', 
                     'file_id', 
                     'file_busid', 
                     'taglist',
                     'line_num',
                     'line_num_next',
                     'route_pattern',
                     'route', 
                     'pattern', 
                     'tag_busid', 
                     'tag_date', 
                     'tag_time', 
                     'tag_datetime', 
                     'tag_starthour',
                     'wday',
                     'Unk1',
                     'mi_to_ft']
    
    file_universe_df = file_universe_df.reindex(columns = column_nm_map, copy = False)
        
    return file_universe_df


def load_rawnav_data(zip_folder_path, skiprows):
    '''
    Parameters
    ----------
    zip_folder_path : str
        Path to the zipped rawnav.txt file..
    skiprows : int
        Number of rows with metadata.
    Raises
    ------
    ParserError
        More number of , in a file. pandas has issue with tokenizing data.   
    Returns
    -------
    pd.DataFrame with the file info.
    '''
    zf = zipfile.ZipFile(zip_folder_path)
    # Get Filename
    namepat = re.compile('(rawnav\d+\.txt)')
    zip_file_name = namepat.search(zip_folder_path).group(1)
    try:
        raw_data = pd.read_csv(zf.open(zip_file_name), skiprows=skiprows, header=None)
    except ParserError as parseerr:
        print("*" * 100)
        print("More number of ',' in a file {}. pandas has issue with tokenizing data. Error: {}".format(zip_file_name,parseerr))
        print("*" * 100)
        raw_data = None
    return raw_data


def clean_rawnav_data(data_dict, filename):
    '''
    Parameters
    ----------
    filename: rawnav file name
    data_dict : dict
        dict of raw data and the data on tag lines.
    Returns
    -------
    Cleaned data without any tags.
    '''
    
    rawnavdata = data_dict['RawData']
    tagline_data = data_dict['tagLineInfo']

    # Check the location of taglines from tagline_data data match the locations in rawnavdata
    try:
        temp = tagline_data.new_line_no.values.flatten()
        tag_indices = np.delete(temp, np.where(temp == -1))
        # Essentially, we reconstitute the tags in the file from the separated columns in rawnavdata and 
        #   make sure that they all match the separate tag data that came from the rawnav_inventory
        if (len(tag_indices)) != 0:
            check_tag_line_data = rawnavdata.loc[tag_indices, :]
            check_tag_line_data[[1, 4, 5]] = check_tag_line_data[[1, 4, 5]].astype(int)
            check_tag_line_data.loc[:, 'taglist'] = \
                (check_tag_line_data[[0, 1, 2, 3, 4, 5]].astype(str) + ',').sum(axis=1).str.rsplit(",", 1, expand=True)[
                    0]
            check_tag_line_data.loc[:, 'taglist'] = check_tag_line_data.loc[:, 'taglist'].str.strip()
            infopat = '^\s*(\S+),(\d{1,5}),(\d{2}\/\d{2}\/\d{2}),(\d{2}:\d{2}:\d{2}),(\S+),(\S+)'
            assert ((~check_tag_line_data.taglist.str.match(infopat, re.S)).sum() == 0)
    except:
        print("TagLists Did not match in file {}".format(filename))
    
    rawnavdata.reset_index(inplace=True);
    # TODO: This line prevents the function from being rerun, need to address later.
    #   odd, given local scoping of functions 
    # IS okay on a first run though
    rawnavdata.rename(columns={"index": "index_loc"}, inplace=True)
    
    # Get End of route Info
    tagline_data, delete_indices1 = add_end_route_info(rawnavdata, tagline_data)
    rawnavdata = rawnavdata[~rawnavdata.index.isin(np.append(tag_indices, delete_indices1))]
   
    # Remove APC and CAL labels and keep APC locations. 
    rawnavdata, apc_tag_loc = remove_apc_cal_tags(rawnavdata)
    rawnavdata = rawnavdata[rawnavdata.apply(check_valid_data_entry, axis=1)]
    apc_loc_dat = pd.Series(apc_tag_loc, name='apc_tag_loc')
    apc_loc_dat = \
        pd.merge_asof(apc_loc_dat, rawnavdata[["index_loc"]], left_on="apc_tag_loc", right_on="index_loc")
    rawnavdata.loc[:, 'row_before_apc'] = False
    rawnavdata.loc[apc_loc_dat.index_loc, 'row_before_apc'] = True
    tagline_data.rename(columns={'new_line_no': "index_run_start_original"}, inplace=True)
    
    # Get trip summary
    summary_data = get_run_summary(data=rawnavdata, tagline_data=tagline_data)
    
    # Rename vals
    column_nm_map = {0: 'lat', 
                     1: 'long', 
                     2: 'heading', 
                     3: 'door_state', 
                     4: 'veh_state', 
                     5: 'odom_ft', 
                     6: 'sec_past_st',
                     7: 'sat_cnt',
                     8: 'stop_window',
                     9: 'blank', 
                     10: 'lat_raw', 
                     11: 'long_raw'}
    rawnavdata.rename(columns=column_nm_map, inplace=True)
    # Apply column transformations
    rawnavdata = rawnavdata.assign(lat=lambda x: x.lat.astype('float'),
                                   long=lambda x: x.long.astype('float'),
                                   heading=lambda x: x.heading.astype('float'))
    #TODO: divider came out wrong here
    # Add composite key to the data
    rawnavdata = add_run_dividers(rawnavdata, summary_data)
    rawnavdata.loc[:, "filename"] = filename
    
    return_dict = {'rawnavdata': rawnavdata, 'summary_data': summary_data}
    
    return return_dict


def subset_rawnav_run(rawnav_data_dict_, rawnav_inventory_filtered_valid_, analysis_routes_):
    '''
    Subset data for analysis routes
    Parameters
    ----------
    rawnav_data_dict_ : dict
        Cleaned data without any tags. filename is the dictionary key.
    rawnav_inventory_filtered_ : pd.DataFrame
        DataFrame with file details to any file including at least one of our analysis routes 
        Note that other non-analysis routes will be included here, but information about these routes
        is currently necessary to split the rawnav file correctly.
    analysis_routes_ : list
        list of routes that need to be subset.
    Returns
    -------
    fin_dat : pd.DataFrame
        Concatenated data for an analysis route. 
    '''
    fin_dat = pd.DataFrame()
    search_df = rawnav_inventory_filtered_valid_[['route', 'filename']].set_index('route')
    try:
        route_files = np.unique(search_df.loc[analysis_routes_, :].values.flatten())
        fin_dat = pd.concat([rawnav_data_dict_[file] for file in route_files])
        fin_dat.reset_index(drop=True, inplace=True)
        fin_dat = fin_dat.query("route in @analysis_routes_")
    except KeyError as kerr:
        print(f'Route {analysis_routes_} not found. Error. {kerr}')
    return fin_dat


# Nested Functions
########################################################################################################################

def add_run_dividers(data, summary_data):
    '''
    Parameters
    ----------
    data : pd.DataFrame
        rawnav data without tags.
    summary_data : pd.DataFrame
        Tagline data.
    Returns
    -------
    rawnav data with composite keys.

    '''
    summary_data.columns
    tags_temp = summary_data[
        ['route_pattern', 'route', 'pattern', 'index_run_start', 'index_run_end']]
    q1 = '''SELECT data.index_loc,data.lat,data.long,data.heading,data.door_state,data.veh_state,data.odom_ft,
    data.sec_past_st,data.sat_cnt,data.stop_window,data.blank,data.lat_raw,data.long_raw,data.row_before_apc,
    tags_temp.route_pattern,tags_temp.route,tags_temp.pattern,tags_temp.index_run_start,
    tags_temp.index_run_end
    FROM data LEFT JOIN tags_temp on data.index_loc 
    BETWEEN  tags_temp.index_run_start and tags_temp.index_run_end
    '''
    data = ps.sqldf(q1, locals())
    return data


def get_run_summary(data, tagline_data):
    '''
    Parameters
    ----------
    data : pd.DataFrame
        rawnav data without tags.
    tagline_data : pd.DataFrame
        Tagline data.
    Returns
    -------
    Summary data : pd.DataFrame
        data with run level summary.
    '''

    temp = tagline_data[['index_run_start_original', 'index_run_end_original']]
    temp = temp.astype('int32')
    raw_da_cpy = data[['index_loc', 0, 1, 5, 6]].copy()
    raw_da_cpy[['index_loc', 5, 6]] = raw_da_cpy[['index_loc', 5, 6]].astype('int32')
    raw_da_cpy.rename(columns={0: 'lat', 
                               1: 'long', 
                               5: 'odom_ft', 
                               6: 'sec_past_st'}, 
                      inplace=True)
    # Get rows with run start from raw_da_cpy
    temp = pd.merge_asof(temp, raw_da_cpy, left_on="index_run_start_original", right_on="index_loc", direction='forward')
    temp.rename(columns={'lat': "lat_start", 
                         'long': "long_start",
                         'odom_ft': "odom_ft_start", 
                         'sec_past_st': "sec_start", 
                         "index_loc": "index_run_start"},
                inplace=True)
    # Get rows with run end from raw_da_cpy
    temp = pd.merge_asof(temp, 
                         raw_da_cpy, 
                         left_on="index_run_end_original", 
                         right_on="index_loc", 
                         direction='backward')
    temp.rename(columns={'lat': "lat_end", 
                         'long': "long_end",
                         'odom_ft': "odom_ft_end", 
                         'sec_past_st': "sec_end",
                         "index_loc": "index_run_end"}, inplace=True)
    temp.loc[:, "run_duration_from_sec"] = temp.sec_end - temp.sec_start
    temp.eval("""
              run_duration_from_sec = sec_end-sec_start
              dist_odom_mi = (odom_ft_end - odom_ft_start)/ 5280
              mph_odom = (dist_odom_mi/ run_duration_from_sec) * 3600
              """, inplace=True)
    temp[["lat_start", "long_start", "lat_end", "long_end"]] =\
        temp[["lat_start", "long_start", "lat_end", "long_end"]].astype(float)
    # Get distance b/w run start and end lat-longs
    temp.loc[:, 'dist_crow_fly_mi'] = get_distance_latlong_mi(temp, "lat_start", "long_start", "lat_end", "long_end")
    summary_dat = tagline_data.merge(temp, on=['index_run_start_original', 'index_run_end_original'], how='left')
    summary_dat.tag_date = summary_dat.tag_date.astype(str)
    summary_dat.loc[:, "start_date_time"] = pd.to_datetime(summary_dat['tag_date'] + " " + summary_dat['run_start_time'])
    summary_dat.loc[:, "end_date_time"] = pd.to_datetime(summary_dat['tag_date'] + " " + summary_dat['run_end_time'],
                                                       errors='coerce')
    summary_dat.loc[:, "run_duration_from_tags"] = pd.to_timedelta(
        summary_dat.loc[:, "end_date_time"] - summary_dat.loc[:, "start_date_time"])
    summary_dat.loc[:, "mph_run_tag"] = round(
        3600 * summary_dat.dist_odom_mi / summary_dat.run_duration_from_tags.dt.total_seconds(), 2)
    summary_dat.run_duration_from_tags = summary_dat.run_duration_from_tags.astype(str)
    summary_dat = summary_dat[['fullpath', 
                               'filename', 
                               'file_busid', 
                               'file_id', 
                               'taglist', 
                               'route_pattern', 
                               'tag_busid',
                               'route', 
                               'pattern', 
                               'wday',
                               'start_date_time', 
                               'end_date_time', 
                               'index_run_start_original',
                               'index_run_start',
                               'index_run_end_original',
                               'index_run_end', 
                               'sec_start',
                               'odom_ft_start', 
                               'sec_end', 
                               'odom_ft_end', 
                               "run_duration_from_sec", 
                               "run_duration_from_tags",
                               "dist_odom_mi",
                               "mph_odom", 
                               "mph_run_tag", 
                               "dist_crow_fly_mi", 
                               "lat_start", 
                               "long_start", 
                               "lat_end", 
                               "long_end"]]
    return summary_dat


# TODO: REimplement this,
                                   # index_run_start=lambda x: x.index_run_start.astype('int'),
                                   # index_run_end=lambda x: x.index_run_end.astype('int')

def remove_apc_cal_tags(data):
    '''
    Parameters
    ----------
    data : pd.DataFrame
        Unclean data with tag information.
    Returns
    -------
    data : pd.DataFrame
        data without APC and CAL tags. 
    apc_tag_loc : np.array
        Location of APC tags. Used to create a new column about bus door open status. 
    '''
    # Remove all rows with "CAL" label
    mask_cal = data.loc[:, 0].str.upper().str.strip() == "CAL"
    data = data[~mask_cal]
    # Remove APC tag and store tag location
    mask_apc = data.loc[:, 0].str.upper().str.strip() == "APC"
    apc_tag_loc = np.array(data[mask_apc].index)
    data = data[~mask_apc]
    return data, apc_tag_loc


def add_end_route_info(data, tagline_data):
    '''
    Parameters
    ----------
    data : pd.DataFrame
        Unclean data with info on end of route.
    tagline_data : pd.DataFrame
        Tagline data.
    Returns
    -------
    tagline_data : pd.DataFrame
        Tagline data with run end time adjusted based on
        "Busware navigation reported end of route..." or
        "Buswares is now using route zero" tags.
    deleteIndices : np.array
        indices to delete from raw data.
    '''
    pat = re.compile(
        '^\s*/\s*(?P<run_end_time>\d{2}:\d{2}:\d{2})\s*(?:Buswares navigation reported end of route|Buswares is now using route zero)',
        re.S)
    data.loc[:, 'run_end_time'] = data[0].str.extract(pat)
    end_of_route = data[['index_loc', 'run_end_time']]
    end_of_route = end_of_route[~(end_of_route.run_end_time.isna())]
    delete_indices = end_of_route.index_loc.values
    end_of_route.rename(columns={'index_loc': 'index_run_end_original'}, inplace=True)
    end_of_route.index_run_end_original = end_of_route.index_run_end_original.astype('int32')
    tagline_data.new_line_no = tagline_data.new_line_no.astype('int32')
    end_of_route = pd.merge_asof(end_of_route, 
                                 tagline_data[['tag_time', 'new_line_no']], 
                                 left_on="index_run_end_original",
                                 right_on='new_line_no', 
                                 direction='backward') 
    end_of_route = end_of_route[~(end_of_route.duplicated(subset=['new_line_no', 'tag_time'], keep='first'))]
    tagline_data = tagline_data.merge(end_of_route, on=['new_line_no', 'tag_time'], how='left')
    tagline_data.loc[:, 'tempLine'] = tagline_data['new_line_no'].shift(-1)
    tagline_data.loc[:, 'tempTime'] = tagline_data['tag_time'].shift(-1)
    tagline_data.loc[tagline_data.run_end_time.isna(), ['index_run_end_original', 'run_end_time']] = tagline_data.loc[
        tagline_data.run_end_time.isna(), ['tempLine', 'tempTime']].values
    if np.isnan(tagline_data.iloc[-1]['index_run_end_original']):
        tagline_data.loc[tagline_data.index.max(), 'index_run_end_original'] = max(data.index_loc)
    tagline_data.rename(columns={'tag_time': "run_start_time"}, inplace=True)
    return tagline_data, delete_indices


def get_distance_latlong_mi(data, lat1, long1, lat2, long2):
    '''
    Parameters
    ----------
    data : pd.DataFrame
        Any dataframe.
    lat1 : str
        1st lat column.
    long1 : str
        1st long column.
    lat2 : str
        2nd lat column.
    long2 : str
        2nd long column.
    Returns
    -------
    DistanceMi: np.array
        distances in mile between (Lat1,long1) and (Lat2,long2) columns in Data.
        same size as number of rows in Data.
    '''
    geometry1 = [Point(xy) for xy in zip(data[long1], data[lat1])]
    gdf = gpd.GeoDataFrame(geometry=geometry1, crs={'init': 'epsg:4326'})
    gdf.to_crs(epsg=3310, inplace=True)  # Distance in meters---Default is in degrees!
    geometry2 = [Point(xy) for xy in zip(data[long2], data[lat2])]
    gdf2 = gpd.GeoDataFrame(geometry=geometry2, crs={'init': 'epsg:4326'})
    gdf2.to_crs(epsg=3310, inplace=True)  # Distance in meters---Default is in degrees!
    # https://gis.stackexchange.com/questions/293310/how-to-use-geoseries-distance-to-get-the-right-answer
    distance_mi = gdf.geometry.distance(gdf2) * 0.000621371  # meters to miles
    return distance_mi.values


def get_distance_latlong_ft_from_geom(geometry1, geometry2):
    '''
    Parameters
    ----------
    geometry1 : pd.series of shapely.geometry.point
    geometry2 : pd.series of shapely.geometry.point
        same size as geometry1
    Returns
    -------
    DistanceFt: np.array
    distances in feet between each points in geometry1 and geometry2.
    same size as geometry1 or geometry2. 
    '''
    gdf = gpd.GeoDataFrame(geometry=geometry1, crs={'init': 'epsg:4326'})
    gdf.to_crs(epsg=3310, inplace=True)  # Distance in meters---Default is in degrees!
    gdf2 = gpd.GeoDataFrame(geometry=geometry2, crs={'init': 'epsg:4326'})
    gdf2.to_crs(epsg=3310, inplace=True)  # Distance in meters---Default is in degrees!
    # https://gis.stackexchange.com/questions/293310/how-to-use-geoseries-distance-to-get-the-right-answer
    distance_ft = gdf.geometry.distance(gdf2) * 3.28084  # meters to feet
    return (distance_ft.values)


def find_all_tags(zip_folder_path, quiet=True):
    '''
    Parameters
    ----------
    zip_folder_path: str
        Path to zipped folder with rawnav text file. 
        i.e., rawnav02164191003.txt.zip
        Assumes that included text file has the same name as the zipped file,
        minus the '.zip' extension.
        Note: For absolute paths, use forward slashes.
    Returns
    -------
    TagLineElements
        List of Character strings including line number, pattern, vehicle,
        date, and time
    '''
    if quiet != True:
        print("Searching for tags in: " + zip_folder_path)
    try:
        zf = zipfile.ZipFile(zip_folder_path)
        # Get Filename
        namepat = re.compile('(rawnav\d+\.txt)')
        zip_file_name = namepat.search(zip_folder_path).group(1)
        # Get Info
        infopat = '^\s*(\S+),(\d{1,5}),(\d{2}\/\d{2}\/\d{2}),(\d{2}:\d{2}:\d{2}),(\S+),(\S+)'
        tag_line_elements = []
        tag_line_num = 1
        with io.TextIOWrapper(zf.open(zip_file_name, 'r'), encoding="utf-8") as input_file:
            for current_line in input_file:
                for match in re.finditer(infopat, current_line, re.S):
                    # Turns out we don't really need capture groups
                    # with string split approach, but leaving in for possible
                    # future changes
                    returnvals = str(tag_line_num) + "," + match.group()
                    tag_line_elements.append(returnvals)
                tag_line_num = tag_line_num + 1
        if len(tag_line_elements) == 0:
            tag_line_elements.append(',,,,,,')
    except BadZipfile as BadZipEr:
        print("*" * 100)
        print("issue with opening zipped file: {}. Error: {}".format(zip_folder_path,BadZipEr))
        print("*" * 100)
        tag_line_elements = []
        tag_line_elements.append(',,,,,,')
    except KeyError as keyerr:
        print("*" * 100)
        print("Text file name doesn't match parent zip folder for': {}. Error: {}".format(zip_folder_path,keyerr))
        print("*" * 100)
        tag_line_elements = []
        tag_line_elements.append(',,,,,,')
    return tag_line_elements


def move_empty_incorrect_label_files(file, path_source_data, issue='EmptyFiles'):
    '''
    Parameters
    ----------
    file : str
        Rawnav files with empty or incorrect BusID
    path_source_data : str
        Sending the file "File" to a directory in path_source_data.
    issue : str, optional
        Type of issue with the file: missing/Empty. The default is 'EmptyFiles'.
    Returns
    -------
    None.
    '''
    # Copy empty files to another directory for checking.
    pat = re.compile('.*(Vehicles\s*[0-9]*-[0-9]*)')
    veh_nos = pat.search(file).group(1)
    move_folder_name = "EmptyMissClassFiles//" + veh_nos
    move_dir = os.path.join(path_source_data, move_folder_name, issue)
    pat = re.compile('(rawnav.*.txt)')
    try:
        if not os.path.exists(move_dir):
            os.makedirs(move_dir)
    except:
        print('Error Dir creation')
    shutil.copy(file, move_dir)  # Will change it to "move" later
    return None


def is_numeric(s):
    '''
    Check if Lat/Long is a String. data has tags like APC : Automatic passenger count
    CAL, Tags about Trip start and End. Find the location of these tags.
    '''
    try:
        float(s)
        return True
    except(ValueError, TypeError):
        return False


def check_valid_data_entry(row):
    '''
    row: Pandas DataFrame Row
    Check if a row is valid i.e. has
    lat, long, heading, door_state, VehinMotion, Odometer and TimeSec data.
    Also validate the data range.
    '''
    is_valid_entry = False
    try:
        lat = float(row[0])
        long = float(row[1])
        heading = float(row[2])
        door_state = row[3]
        veh_in_motion = row[4]
        if ((-90 <= lat <= 90) & (-180 <= long <= 180) & (0 <= heading <= 360) &
                (door_state in ['O', 'C']) & (veh_in_motion in ['M', 'S'])):
            is_valid_entry = True
    except:
        ""
    return is_valid_entry
