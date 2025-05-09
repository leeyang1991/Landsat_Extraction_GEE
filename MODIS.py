# coding=utf-8
import turtle

import urllib3
from __init__ import *
import ee
import math
import pprint
# coding=utf-8
import geopandas as gpd
from geopy import Point
from geopy.distance import distance as Distance
from shapely.geometry import Polygon


this_script_root = join(this_root, 'MODIS_download')


class Expand_points_to_rectangle:

    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'Expand_points_to_rectangle',
            this_script_root, mode=2)
        pass

    def run(self):
        point_list,name_list = self.read_point_shp()
        rectangle_list = self.expand_points_to_rectangle(point_list)
        self.write_rectangle_shp(rectangle_list,name_list)
        pass

    def read_point_shp(self):
        csv_f = join(data_root,'Blake/site_locations_review.csv')
        csv_df = pd.read_csv(csv_f)
        lon_list = csv_df['long'].tolist()
        lat_list = csv_df['lat'].tolist()
        name_list = csv_df['site'].tolist()
        point_list = zip(lon_list, lat_list)
        point_list = list(point_list)
        return point_list,name_list

    def expand_points_to_rectangle(self, point_list):
        distance_i = 3*500/1000./2
        # print(point_list)
        rectangle_list = []
        for point in point_list:
            lon = point[0]
            lat = point[1]
            p = Point(latitude=lat, longitude=lon)
            north = Distance(kilometers=distance_i).destination(p, 0)
            south = Distance(kilometers=distance_i).destination(p, 180)
            east = Distance(kilometers=distance_i).destination(p, 90)
            west = Distance(kilometers=distance_i).destination(p, 270)
            # rectangle = Polygon([(west.longitude, west.latitude), (east.longitude, east.latitude),
            #                         (north.longitude, north.latitude), (south.longitude, south.latitude)])
            # east = (east.longitude, east.latitude)
            # west = (west.longitude, west.latitude)
            # north = (north.longitude, north.latitude)
            # south = (south.longitude, south.latitude)

            east_lon = east.longitude
            west_lon = west.longitude
            north_lat = north.latitude
            south_lat = south.latitude

            ll_point = (west_lon, south_lat)
            lr_point = (east_lon, south_lat)
            ur_point = (east_lon, north_lat)
            ul_point = (west_lon, north_lat)

            polygon_geom = Polygon([ll_point, lr_point, ur_point, ul_point])

            rectangle_list.append(polygon_geom)
        return rectangle_list

    def write_rectangle_shp(self, rectangle_list,name_list):
        outdir = join(self.this_class_arr, 'sites')
        T.mkdir(outdir)
        outf = join(outdir, 'sites.shp')
        crs = {'init': 'epsg:4326'}  # 设置坐标系
        polygon = gpd.GeoDataFrame(crs=crs, geometry=rectangle_list)  # 将多边形对象转换为GeoDataFrame对象
        polygon['name'] = name_list

        # 保存为shp文件
        polygon.to_file(outf)
        pass

    def GetDistance(self,lng1, lat1, lng2, lat2):
        radLat1 = self.rad(lat1)
        radLat2 = self.rad(lat2)
        a = radLat1 - radLat2
        b = self.rad(lng1) - self.rad(lng2)
        s = 2 * math.asin(math.sqrt(
            math.pow(math.sin(a / 2), 2) + math.cos(radLat1) * math.cos(radLat2) * math.pow(math.sin(b / 2), 2)))
        s = s * 6378.137 * 1000
        distance = round(s, 4)
        return distance

        pass

    def rad(self,d):
        return d * math.pi / 180


class MODIS_LAI:

    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'LAI',
            this_script_root, mode=2)
        # ee.Initialize()

    def run(self):

        self.download_images()
        # self.unzip()
        # self.reproj()
        # self.statistic()
        pass

    def download_images(self):
        outdir = join(self.this_class_arr,'download_images')
        T.mk_dir(outdir)
        rectangle_f = join(Expand_points_to_rectangle().this_class_arr,'sites/sites.shp')
        rectangle_df = gpd.read_file(rectangle_f)
        geometry_list = rectangle_df['geometry'].tolist()
        site_list = rectangle_df['name'].tolist()
        params_list = []
        for i,geo in enumerate(geometry_list):
            param = (i,site_list,outdir,geo,geometry_list)
            params_list.append(param)
            # self.kernel_download_from_gee(param)
        MULTIPROCESS(self.kernel_download_from_gee,params_list).run(process=5,process_or_thread='t')


    def kernel_download_from_gee(self,param):
        i,site_list,outdir,geo,geometry_list = param
        site = site_list[i]
        outdir_i = join(outdir, site)
        T.mk_dir(outdir_i)
        ll = geo.bounds[0:2]
        # print(ll)
        # exit()
        ur = geo.bounds[2:4]
        region = ee.Geometry.Rectangle(ll[0], ll[1], ur[0], ur[1])
        startDate = '2000-01-01'
        endDate = '2022-12-31'
        l8 = ee.ImageCollection('MODIS/061/MOD13A2')
        # l8 = ee.ImageCollection('LANDSAT/LC08/C01/T1_SR')
        l8 = l8.filterDate(startDate, endDate).filterBounds(region)

        info_dict = l8.getInfo()
        # pprint.pprint(info_dict)
        # exit()
        # for key in info_dict:
        #     print(key)
        ids = info_dict['features']
        for i in tqdm(ids, desc=f'{i + 1}/{len(geometry_list)}'):
            dict_i = eval(str(i))
            # pprint.pprint(dict_i['id'])
            # exit()
            outf_name = dict_i['id'].split('/')[-1] + '.zip'
            out_path = join(outdir_i, outf_name)
            if isfile(out_path):
                continue
            # print(outf_name)
            # exit()
            # print(dict_i['id'])
            # l8 = l8.median()
            # l8_qa = l8.select(['QA_PIXEL'])
            # l8_i = ee.Image(dict_i['LANDSAT/LC08/C02/T1_L2/LC08_145037_20200712'])
            l8_i = ee.Image(dict_i['id'])
            l8_optical_bands = l8_i.select('NDVI').multiply(0.0001)
            exportOptions = {
                'scale': 1000,
                'maxPixels': 1e13,
                'region': region,
                # 'fileNamePrefix': 'exampleExport',
                # 'description': 'imageToAssetExample',
            }
            url = l8_optical_bands.getDownloadURL(exportOptions)

            try:
                self.download_i(url, out_path)
            except:
                print('download error', out_path)
                continue
        pass

    def download_i(self,url,outf):
        # try:
        http = urllib3.PoolManager()
        r = http.request('GET', url, preload_content=False)
        body = r.read()
        with open(outf, 'wb') as f:
            f.write(body)

    def unzip(self):
        fdir = join(self.this_class_arr,'download_images')
        outdir = join(self.this_class_arr,'unzip')
        T.mk_dir(outdir)
        for folder in T.listdir(fdir):
            fdir_i = join(fdir,folder)
            outdir_i = join(outdir,folder)
            T.unzip(fdir_i,outdir_i)
        pass

    def wkt(self):
        wkt = '''
        PROJCS["Sinusoidal",
    GEOGCS["GCS_Undefined",
        DATUM["Undefined",
            SPHEROID["User_Defined_Spheroid",6371007.181,0.0]],
        PRIMEM["Greenwich",0.0],
        UNIT["Degree",0.0174532925199433]],
    PROJECTION["Sinusoidal"],
    PARAMETER["False_Easting",0.0],
    PARAMETER["False_Northing",0.0],
    PARAMETER["Central_Meridian",0.0],
    UNIT["Meter",1.0]]'''
        return wkt

    def reproj(self):
        fdir = join(self.this_class_arr,'unzip')
        outdir = join(self.this_class_arr,'reproj')
        T.mk_dir(outdir)
        for site in T.listdir(fdir):
            fdir_i = join(fdir,site)
            outdir_i = join(outdir,site)
            T.mk_dir(outdir_i)
            for date in T.listdir(fdir_i):
                fdir_i_i = join(fdir_i,date)
                for f in T.listdir(fdir_i_i):
                    fpath = join(fdir_i_i,f)
                    outpath = join(outdir_i,date+'.tif')
                    SRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt())
                    ToRaster().resample_reproj(fpath,outpath,.005,srcSRS=SRS, dstSRS='EPSG:4326')

    def statistic(self):
        fdir = join(self.this_class_arr,'reproj')
        outdir = join(self.this_class_arr,'statistic')
        T.mk_dir(outdir)
        for site in T.listdir(fdir):
            fdir_i = join(fdir,site)
            mean_list = []
            date_list = []
            for f in T.listdir(fdir_i):
                if not f.endswith('.tif'):
                    continue
                date = f.split('.')[0]
                y,m,d = date.split('_')
                y = int(y)
                m = int(m)
                d = int(d)
                date_obj = datetime.datetime(y,m,d)
                fpath = join(fdir_i,f)
                arr = ToRaster().raster2array(fpath)[0]
                arr[arr<=0] = np.nan
                mean = np.nanmean(arr)
                mean_list.append(mean)
                date_list.append(f'{y}-{m:02d}-{d:02d}')
            df = pd.DataFrame({'date':date_list,'NDVI':mean_list})
            outf = join(outdir,site)
            T.df_to_excel(df,outf)

        pass

class MODIS_IGBP:

    def __init__(self):
        self.datadir = join(data_root, 'MODIS','MCD12Q1')

    def run(self):
        # self.download_images()
        # self.unzip()
        # self.merge()
        # self.reproj()
        pass

    def download_images(self):
        ee.Initialize()

        startDate = '2001-01-01'
        endDate = '2003-1-1'
        resolution = 1000 # meter
        band_name = 'LC_Type1'
        product_name = 'MODIS/061/MCD12Q1'
        outdir = join(self.datadir,'download_images')
        T.mk_dir(outdir,force=True)

        Collection = ee.ImageCollection(product_name)
        Collection = Collection.filterDate(startDate, endDate)
        info_dict = Collection.getInfo()
        # exit()
        # pprint.pprint(info_dict)
        # exit()
        # for key in info_dict:
        #     print(key)
        ids = info_dict['features']
        for i in ids:
            dict_i = eval(str(i))
            date = dict_i['id'].split('/')[-1]
            outdir_i = join(outdir,date)
            T.mk_dir(outdir_i,force=True)
            Image = ee.Image(dict_i['id'])
            Image_band = Image.select(band_name)
            region_list = self.rectangle(rect=[-180, 90, 180, -90],block_res=15)
            flag = 1
            params_list = []
            for region in tqdm(region_list,desc=f'{date}'):
                params_i = [resolution,region,Image_band,outdir_i,flag]
                params_list.append(params_i)
                flag += 1
            MULTIPROCESS(self.kernel_download,params_list).run(process_or_thread='t',process=10)

        pass

    def kernel_download(self,params):
        resolution,region,l8_optical_bands,outdir_i,flag = params
        outf_name = join(outdir_i, f'{flag}.zip')
        if isfile(outf_name):
            return

        # print(region)
        exportOptions = {
            'scale': resolution,
            'region': region,
        }
        url = l8_optical_bands.getDownloadURL(exportOptions)
        try:
            self.download_i(url, outf_name)
        except:
            print('download error', outf_name)

    def rectangle(self,rect=(-180, 90, 180, -90),block_res=90):
        rect_list = []
        lon_start = rect[0]
        lat_start = rect[3]
        lon_end = rect[2]
        lat_end = rect[1]
        for lon in np.arange(lon_start, lon_end, block_res):
            for lat in np.arange(lat_start, lat_end, block_res):
                rect_i = [lon, lat, lon + block_res, lat + block_res]
                # print(rect_i)
                rect_i_new = [rect_i[0], rect_i[3], rect_i[2], rect_i[1]]
                rect_i_new = [float(i) for i in rect_i_new]
                # exit()
                rect_list.append(rect_i_new)
        # print(rect_list)
        # print('len(rect_list)', len(rect_list))
        rect_list_obj = []
        for rect_i in rect_list:
            # print(rect_i)
            rect_i_obj = ee.Geometry.Rectangle(rect_i[0], rect_i[1], rect_i[2], rect_i[3])
            # rect_i_obj = ee.Geometry.Rectangle(rect_i[0], rect_i[1], rect_i[2], rect_i[3])
            rect_list_obj.append(rect_i_obj)
        return rect_list_obj


    def download_i(self,url,outf):
        # try:
        http = urllib3.PoolManager()
        r = http.request('GET', url, preload_content=False)
        body = r.read()
        with open(outf, 'wb') as f:
            f.write(body)

    def unzip(self):
        fdir = join(self.datadir,r'download_images')
        outdir = join(self.datadir,r'unzip')
        T.mk_dir(outdir,force=True)
        for folder in T.listdir(fdir):
            fdir_i = join(fdir,folder)
            outdir_i = join(outdir,folder)
            T.unzip(fdir_i,outdir_i)
        pass

    def merge(self):
        fdir = join(self.datadir,r'unzip')
        outdir = join(self.datadir,r'merge')
        T.mk_dir(outdir,force=True)
        for date in tqdm(T.listdir(fdir)):
            fdir_i = join(fdir,date)
            fpath_list = []
            for folder in T.listdir(fdir_i):
                fdir_i_i = join(fdir_i,folder)
                for f in T.listdir(fdir_i_i):
                    if not f.endswith('.tif'):
                        continue
                    fpath = join(fdir_i_i,f)
                    fpath_list.append(fpath)
            srcSRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt())
            outf = join(outdir,f'{date}.tif')
            if isfile(outf):
                continue
            gdal.Warp(outf,fpath_list,srcSRS=srcSRS, outputType=gdal.GDT_Byte)

        pass

    def reproj(self):
        fdir = join(self.datadir,r'merge')
        outdir = join(self.datadir,r'reproj')
        T.mk_dir(outdir,force=True)
        for f in T.listdir(fdir):
            if not f.endswith('.tif'):
                continue
            fpath = join(fdir,f)
            outpath = join(outdir,f)
            SRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt())
            ToRaster().resample_reproj(fpath,outpath,0.08333,srcSRS=SRS, dstSRS='EPSG:4326')
        pass

    def wkt(self): # Sinusoidal
        wkt = '''
        PROJCS["Sinusoidal",
    GEOGCS["GCS_Undefined",
        DATUM["Undefined",
            SPHEROID["User_Defined_Spheroid",6371007.181,0.0]],
        PRIMEM["Greenwich",0.0],
        UNIT["Degree",0.0174532925199433]],
    PROJECTION["Sinusoidal"],
    PARAMETER["False_Easting",0.0],
    PARAMETER["False_Northing",0.0],
    PARAMETER["Central_Meridian",0.0],
    UNIT["Meter",1.0]]'''
        return wkt


class MODIS_GPP:

    def __init__(self):
        self.datadir = join(data_root, 'GPP','MOD17A2H')

    def run(self):
        # self.download_images()
        # self.unzip()
        self.merge()
        # self.reproj()
        pass

    def download_images(self):
        ee.Initialize()

        startDate = '2001-01-01'
        endDate = '2023-1-1'
        resolution = 500 # meter
        band_name = 'Gpp'
        product_name = 'MODIS/061/MOD17A2H'
        outdir = join(self.datadir,'download_images')
        T.mk_dir(outdir,force=True)

        Collection = ee.ImageCollection(product_name)
        Collection = Collection.filterDate(startDate, endDate)
        info_dict = Collection.getInfo()
        # exit()
        # pprint.pprint(info_dict)
        # exit()
        # for key in info_dict:
        #     print(key)
        ids = info_dict['features']
        for i in ids:
            dict_i = eval(str(i))
            date = dict_i['id'].split('/')[-1]
            outdir_i = join(outdir,date)
            T.mk_dir(outdir_i,force=True)
            Image = ee.Image(dict_i['id'])
            Image_band = Image.select(band_name)
            region_list = self.rectangle(rect=[-180, 90, 180, -90],block_res=15)
            flag = 1
            params_list = []
            for region in tqdm(region_list,desc=f'{date}'):
                params_i = [resolution,region,Image_band,outdir_i,flag]
                params_list.append(params_i)
                flag += 1
            MULTIPROCESS(self.kernel_download,params_list).run(process_or_thread='t',process=10)

        pass

    def kernel_download(self,params):
        resolution,region,l8_optical_bands,outdir_i,flag = params
        outf_name = join(outdir_i, f'{flag}.zip')
        if isfile(outf_name):
            return

        # print(region)
        exportOptions = {
            'scale': resolution,
            'region': region,
        }
        url = l8_optical_bands.getDownloadURL(exportOptions)
        try:
            self.download_i(url, outf_name)
        except:
            print('download error', outf_name)

    def rectangle(self,rect=(-180, 90, 180, -90),block_res=90):
        rect_list = []
        lon_start = rect[0]
        lat_start = rect[3]
        lon_end = rect[2]
        lat_end = rect[1]
        for lon in np.arange(lon_start, lon_end, block_res):
            for lat in np.arange(lat_start, lat_end, block_res):
                rect_i = [lon, lat, lon + block_res, lat + block_res]
                # print(rect_i)
                rect_i_new = [rect_i[0], rect_i[3], rect_i[2], rect_i[1]]
                rect_i_new = [float(i) for i in rect_i_new]
                # exit()
                rect_list.append(rect_i_new)
        # print(rect_list)
        # print('len(rect_list)', len(rect_list))
        rect_list_obj = []
        for rect_i in rect_list:
            # print(rect_i)
            rect_i_obj = ee.Geometry.Rectangle(rect_i[0], rect_i[1], rect_i[2], rect_i[3])
            # rect_i_obj = ee.Geometry.Rectangle(rect_i[0], rect_i[1], rect_i[2], rect_i[3])
            rect_list_obj.append(rect_i_obj)
        return rect_list_obj


    def download_i(self,url,outf):
        # try:
        http = urllib3.PoolManager()
        r = http.request('GET', url, preload_content=False)
        body = r.read()
        with open(outf, 'wb') as f:
            f.write(body)

    def unzip(self):
        fdir = join(self.datadir,r'download_images')
        outdir = join(self.datadir,r'unzip')
        T.mk_dir(outdir,force=True)
        for folder in T.listdir(fdir):
            print(folder)
            fdir_i = join(fdir,folder)
            outdir_i = join(outdir,folder)
            T.unzip(fdir_i,outdir_i)
        pass

    def merge(self):
        fdir = join(self.datadir,r'unzip')
        outdir = join(self.datadir,r'merge')
        T.mk_dir(outdir,force=True)
        for date in tqdm(T.listdir(fdir)):
            fdir_i = join(fdir,date)
            fpath_list = []
            for folder in T.listdir(fdir_i):
                fdir_i_i = join(fdir_i,folder)
                for f in T.listdir(fdir_i_i):
                    if not f.endswith('.tif'):
                        continue
                    fpath = join(fdir_i_i,f)
                    fpath_list.append(fpath)
            srcSRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt())
            outf = join(outdir,f'{date}.tif')
            if isfile(outf):
                continue
            gdal.Warp(outf,fpath_list,srcSRS=srcSRS, outputType=gdal.GDT_Int32)

        pass

    def reproj(self):
        fdir = join(self.datadir,r'merge')
        outdir = join(self.datadir,r'reproj')
        T.mk_dir(outdir,force=True)
        for f in T.listdir(fdir):
            if not f.endswith('.tif'):
                continue
            fpath = join(fdir,f)
            outpath = join(outdir,f)
            SRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt())
            ToRaster().resample_reproj(fpath,outpath,0.08333,srcSRS=SRS, dstSRS='EPSG:4326')
        pass

    def wkt(self): # Sinusoidal
        wkt = '''
        PROJCS["Sinusoidal",
    GEOGCS["GCS_Undefined",
        DATUM["Undefined",
            SPHEROID["User_Defined_Spheroid",6371007.181,0.0]],
        PRIMEM["Greenwich",0.0],
        UNIT["Degree",0.0174532925199433]],
    PROJECTION["Sinusoidal"],
    PARAMETER["False_Easting",0.0],
    PARAMETER["False_Northing",0.0],
    PARAMETER["Central_Meridian",0.0],
    UNIT["Meter",1.0]]'''
        return wkt

class MODIS_GPP_annual_mean:

    def __init__(self):
        self.datadir = join(data_root, 'MODIS_GPP_long_term_mean')

    def run(self):
        # self.download_images()
        # self.unzip()
        self.merge()
        # self.reproj()
        pass

    def download_images(self):
        ee.Initialize()

        startDate = '2001-01-01'
        endDate = '2023-1-1'
        resolution = 500 # meter
        band_name = 'Gpp'
        product_name = 'MODIS/061/MOD17A2HGF'
        outdir = join(self.datadir,'download_images')
        T.mk_dir(outdir,force=True)

        Collection = ee.ImageCollection(product_name)
        Collection = Collection.filterDate(startDate, endDate)
        longterm_average_Image = Collection.reduce(ee.Reducer.mean())
        Image_band = longterm_average_Image.select(f'{band_name}_mean')
        # region_list = self.rectangle(rect=[-180, 90, 180, -90],block_res=15)
        region_list = self.rectangle(rect=[-127, 50, -65, 23],block_res=2)
        flag = 1
        params_list = []
        outdir_i = join(outdir,f'{startDate}_{endDate}')
        T.mk_dir(outdir_i,force=True)
        for region in tqdm(region_list):
            params_i = [resolution,region,Image_band,outdir_i,flag]
            params_list.append(params_i)
            flag += 1
        MULTIPROCESS(self.kernel_download,params_list).run(process_or_thread='t',process=10)

        pass

    def kernel_download(self,params):
        resolution,region,l8_optical_bands,outdir_i,flag = params
        outf_name = join(outdir_i, f'{flag}.zip')
        if isfile(outf_name):
            return

        # print(region)
        exportOptions = {
            'scale': resolution,
            'region': region,
        }
        url = l8_optical_bands.getDownloadURL(exportOptions)
        try:
            self.download_i(url, outf_name)
        except:
            print('download error', outf_name)

    def rectangle(self,rect=(-180, 90, 180, -90),block_res=90):
        rect_list = []
        lon_start = rect[0]
        lat_start = rect[3]
        lon_end = rect[2]
        lat_end = rect[1]
        for lon in np.arange(lon_start, lon_end, block_res):
            for lat in np.arange(lat_start, lat_end, block_res):
                rect_i = [lon, lat, lon + block_res, lat + block_res]
                # print(rect_i)
                rect_i_new = [rect_i[0], rect_i[3], rect_i[2], rect_i[1]]
                rect_i_new = [float(i) for i in rect_i_new]
                # exit()
                rect_list.append(rect_i_new)
        # print(rect_list)
        # print('len(rect_list)', len(rect_list))
        rect_list_obj = []
        for rect_i in rect_list:
            # print(rect_i)
            rect_i_obj = ee.Geometry.Rectangle(rect_i[0], rect_i[1], rect_i[2], rect_i[3])
            # rect_i_obj = ee.Geometry.Rectangle(rect_i[0], rect_i[1], rect_i[2], rect_i[3])
            rect_list_obj.append(rect_i_obj)
        return rect_list_obj


    def download_i(self,url,outf):
        # try:
        http = urllib3.PoolManager()
        r = http.request('GET', url, preload_content=False)
        body = r.read()
        with open(outf, 'wb') as f:
            f.write(body)

    def unzip(self):
        fdir = join(self.datadir,r'download_images')
        outdir = join(self.datadir,r'unzip')
        T.mk_dir(outdir,force=True)
        for folder in T.listdir(fdir):
            print(folder)
            fdir_i = join(fdir,folder)
            outdir_i = join(outdir,folder)
            self._unzip(fdir_i,outdir_i)
        pass

    def _unzip(self, zipfolder, outdir):
        # zipfolder = join(self.datadir,'zips')
        # outdir = join(self.datadir,'unzip')
        T.mkdir(outdir)
        for f in tqdm(T.listdir(zipfolder)):
            outdir_i = join(outdir, f.replace('.zip', ''))
            T.mkdir(outdir_i)
            fpath = join(zipfolder, f)
            # print(fpath)
            zip_ref = zipfile.ZipFile(fpath, 'r')
            zip_ref.extractall(outdir_i)
            zip_ref.close()

    def merge(self):
        fdir = join(self.datadir,r'unzip')
        outdir = join(self.datadir,r'merge')
        T.mk_dir(outdir,force=True)
        for date in tqdm(T.listdir(fdir)):
            fdir_i = join(fdir,date)
            fpath_list = []
            for folder in T.listdir(fdir_i):
                fdir_i_i = join(fdir_i,folder)
                for f in T.listdir(fdir_i_i):
                    if not f.endswith('.tif'):
                        continue
                    fpath = join(fdir_i_i,f)
                    fpath_list.append(fpath)
            srcSRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt_84())
            # srcSRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt_sin())
            outf = join(outdir,f'{date}.tif')
            if isfile(outf):
                continue
            gdal.Warp(outf,fpath_list,srcSRS=srcSRS, outputType=gdal.GDT_Int32)

        pass

    def reproj(self):
        fdir = join(self.datadir,r'merge')
        outdir = join(self.datadir,r'reproj')
        T.mk_dir(outdir,force=True)
        for f in T.listdir(fdir):
            if not f.endswith('.tif'):
                continue
            fpath = join(fdir,f)
            outpath = join(outdir,f)
            SRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt())
            ToRaster().resample_reproj(fpath,outpath,0.08333,srcSRS=SRS, dstSRS='EPSG:4326')
        pass

    def wkt_sin(self): # Sinusoidal
        wkt = '''
        PROJCS["Sinusoidal",
    GEOGCS["GCS_Undefined",
        DATUM["Undefined",
            SPHEROID["User_Defined_Spheroid",6371007.181,0.0]],
        PRIMEM["Greenwich",0.0],
        UNIT["Degree",0.0174532925199433]],
    PROJECTION["Sinusoidal"],
    PARAMETER["False_Easting",0.0],
    PARAMETER["False_Northing",0.0],
    PARAMETER["Central_Meridian",0.0],
    UNIT["Meter",1.0]]'''
        return wkt

    def wkt_84(self):
        wkt_str = '''GEOGCRS["WGS 84",
    ENSEMBLE["World Geodetic System 1984 ensemble",
        MEMBER["World Geodetic System 1984 (Transit)"],
        MEMBER["World Geodetic System 1984 (G730)"],
        MEMBER["World Geodetic System 1984 (G873)"],
        MEMBER["World Geodetic System 1984 (G1150)"],
        MEMBER["World Geodetic System 1984 (G1674)"],
        MEMBER["World Geodetic System 1984 (G1762)"],
        MEMBER["World Geodetic System 1984 (G2139)"],
        ELLIPSOID["WGS 84",6378137,298.257223563,
            LENGTHUNIT["metre",1]],
        ENSEMBLEACCURACY[2.0]],
    PRIMEM["Greenwich",0,
        ANGLEUNIT["degree",0.0174532925199433]],
    CS[ellipsoidal,2],
        AXIS["geodetic latitude (Lat)",north,
            ORDER[1],
            ANGLEUNIT["degree",0.0174532925199433]],
        AXIS["geodetic longitude (Lon)",east,
            ORDER[2],
            ANGLEUNIT["degree",0.0174532925199433]],
    USAGE[
        SCOPE["Horizontal component of 3D system."],
        AREA["World."],
        BBOX[-90,-180,90,180]],
    ID["EPSG",4326]]'''
        return wkt_str

class MODIS_NDVI:

    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'NDVI_250m_bighorn',
            this_script_root, mode=2)
        self.product = 'NDVI'
        # self.product = 'total_precipitation'
        ee.Initialize(project='lyfq-263413')
        # ee.Authenticate()
        # pause()
        # exit()

    def run(self):
        # year_list = list(range(2020,2025))
        # MULTIPROCESS(self.download_images,year_list).run(process=10,process_or_thread='t')
        # for year in range(2000,2025):
        #     print(year)
        #     self.download_images(year)
        # self.check()
        # self.unzip()
        # self.reproj()
        self.clip()
        # self.statistic()
        pass

    def download_images(self,year=1982):
        outdir = join(self.this_class_arr,self.product,str(year))
        T.mk_dir(outdir,force=True)
        startDate = f'{year}-01-01'
        endDate = f'{year+1}-01-01'
        Collection = ee.ImageCollection('MODIS/061/MOD13A2')
        Collection = Collection.filterDate(startDate, endDate)

        info_dict = Collection.getInfo()
        # pprint.pprint(info_dict)
        # print(len(info_dict['features']))
        # exit()
        # for key in info_dict:
        #     print(key)
        ids = info_dict['features']
        for i in ids:
            dict_i = eval(str(i))
            # pprint.pprint(dict_i['id'])
            # exit()
            outf_name = dict_i['id'].split('/')[-1] + '.zip'
            out_path = join(outdir, outf_name)
            if isfile(out_path):
                continue
            # print(outf_name)
            # exit()
            # print(dict_i['id'])
            # l8 = l8.median()
            # l8_qa = l8.select(['QA_PIXEL'])
            # l8_i = ee.Image(dict_i['LANDSAT/LC08/C02/T1_L2/LC08_145037_20200712'])
            Image = ee.Image(dict_i['id'])
            # Image_product = Image.select('total_precipitation')
            Image_product = Image.select(['NDVI'])
            # print(Image_product);exit()
            region = [-111, 32.2, -110, 32.6]# left, bottom, right, top
            exportOptions = {
                'scale': 250,
                'maxPixels': 1e13,
                'region': region,
                # 'fileNamePrefix': 'exampleExport',
                # 'description': 'imageToAssetExample',
            }
            url = Image_product.getDownloadURL(exportOptions)
            # print(url)

            try:
                self.download_i(url, out_path)
            except:
                print('download error', out_path)
                continue
        pass




    def download_i(self,url,outf):
        # try:
        http = urllib3.PoolManager()
        r = http.request('GET', url, preload_content=False)
        body = r.read()
        with open(outf, 'wb') as f:
            f.write(body)

    def unzip(self):
        fdir = join(self.this_class_arr,self.product)
        outdir = join(self.this_class_arr,'unzip',self.product)
        T.mk_dir(outdir,force=True)
        for folder in T.listdir(fdir):
            print(folder)
            fdir_i = join(fdir,folder)
            # T.open_path_and_file(fdir_i,folder)
            # exit()
            outdir_i = join(outdir,folder)
            T.unzip(fdir_i,outdir_i)
        pass

    def check(self):
        fdir = join(self.this_class_arr, self.product)
        # outdir = join(self.this_class_arr, 'unzip', self.product)
        # T.mk_dir(outdir, force=True)
        for folder in T.listdir(fdir):
            fdir_i = join(fdir, folder)
            for f in tqdm(T.listdir(fdir_i),desc=folder):
                fpath = join(fdir_i, f)
                try:
                    zipfile.ZipFile(fpath, 'r')
                except:
                    os.remove(fpath)
                    print(fpath)
                    continue
                pass
        pass


    def wkt(self):
        wkt = '''
        PROJCS["Sinusoidal",
    GEOGCS["GCS_Undefined",
        DATUM["Undefined",
            SPHEROID["User_Defined_Spheroid",6371007.181,0.0]],
        PRIMEM["Greenwich",0.0],
        UNIT["Degree",0.0174532925199433]],
    PROJECTION["Sinusoidal"],
    PARAMETER["False_Easting",0.0],
    PARAMETER["False_Northing",0.0],
    PARAMETER["Central_Meridian",0.0],
    UNIT["Meter",1.0]]'''
        return wkt

    def reproj(self):
        fdir = join(self.this_class_arr,'unzip',self.product)
        outdir = join(self.this_class_arr,'reproj',self.product)
        T.mk_dir(outdir,force=True)
        for year in tqdm(T.listdir(fdir)):
            for date in T.listdir(join(fdir,year)):
                for f in T.listdir(join(fdir,year,date)):
                    fpath = join(fdir,year,date,f)
                    date_str = f.split('.')[0]
                    y,m,d = date_str.split('_')
                    # print(fpath);exit()
                    outpath = join(outdir,f'{y}{m}{d}.tif')
                    # print(outpath)
                    SRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt())
                    wkg_wgs84 = DIC_and_TIF().wkt_84()
                    ToRaster().resample_reproj(fpath,outpath,.0025,srcSRS=SRS, dstSRS=wkg_wgs84)
                    # print(outpath)
                    # exit()

    def clip(self):
        fdir = join(self.this_class_arr,'reproj',self.product)
        outdir = join(self.this_class_arr,'clip',self.product)
        #/mnt/sdb2/yang/Global_Resilience/MODIS_download/NDVI_250m_bighorn/arr/bighorn_shp/bighorn_shp
        shp = join(self.this_class_arr,'bighorn_shp','bighorn_shp')
        T.mkdir(outdir,force=True)
        for f in tqdm(T.listdir(fdir)):
            if not f.endswith('.tif'):
                continue
            fpath = join(fdir,f)
            outpath = join(outdir,f)

            ToRaster().clip_array(fpath,outpath,shp)
            # T.open_path_and_file(outdir)
            # exit()

        pass

    def statistic(self):
        fdir = join(self.this_class_arr,'reproj',self.product)
        statistic_dict = {}
        for f in T.listdir(fdir):
            date = f.split('.')[0]
            year,mon,day = date[:4],date[4:6],date[6:]
            if not year in statistic_dict:
                statistic_dict[year] = []
            statistic_dict[year].append(f)
        for year in statistic_dict:
            flist = statistic_dict[year]
            print(year,len(flist))


def main():
    # Expand_points_to_rectangle().run()
    # MODIS_LAI().run()
    # MODIS_GPP().run()
    MODIS_NDVI().run()
    # MODIS_GPP_annual_mean().run()
    pass

if __name__ == '__main__':
    main()