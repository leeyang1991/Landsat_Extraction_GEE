# coding=utf-8
import datetime
import os

import matplotlib.pyplot as plt
import numpy as np
import ternary
# import urllib3
from __init__ import *
import ee
import math
import pprint
# coding=utf-8
import geopandas as gpd
from geopy import Point
from geopy.distance import distance as Distance
from shapely.geometry import Polygon

this_script_root = join(this_root, 'results')

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
        csv_f = join(this_root,'site/xlsx/site.xlsx')
        csv_df = pd.read_excel(csv_f)
        lon_list = csv_df['Longitude (degrees)'].tolist()
        lat_list = csv_df['Latitude (degrees)'].tolist()
        name_list = csv_df['Site ID'].tolist()
        point_list = zip(lon_list, lat_list)
        point_list = list(point_list)
        return point_list,name_list

    def expand_points_to_rectangle(self, point_list):
        distance_i = 2*500/1000./2 # 500 meters radius
        # print(point_list)
        rectangle_list = []
        for point in point_list:
            lon = point[0]
            lat = point[1]
            print(lon,lat)
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

class MODIS_LAI_Extraction:

    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'MODIS_LAI_Extraction1',
            this_script_root, mode=2)
        ee.Initialize()

    def run(self):

        # self.download_images()
        # self.unzip()
        # self.reproj()
        self.extract()
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
        MULTIPROCESS(self.kernel_download_from_gee,params_list).run(process=20,process_or_thread='t')


    def kernel_download_from_gee(self,param):
        i,site_list,outdir,geo,geometry_list = param
        site = site_list[i]
        print(site)
        outdir_i = join(outdir, site)
        T.mk_dir(outdir_i)
        ll = geo.bounds[0:2]
        # print(ll)
        # exit()
        ur = geo.bounds[2:4]
        # ur_new = (ur[0],ur[1])
        # print(ll[0], ll[1], ur[0], ur[1])
        # exit()
        region = ee.Geometry.Rectangle(ll[0], ll[1], ur[0], ur[1])
        # print(region)
        # exit()
        startDate = '2002-01-01'
        endDate = '2022-12-31'
        # l8 = ee.ImageCollection('MODIS/061/MOD13A2')
        l8 = ee.ImageCollection('MODIS/061/MCD15A3H')
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
            download_flag = 0
            try:
                zip_ref = zipfile.ZipFile(out_path, 'r')
            except:
                download_flag = 1
            if isfile(out_path) and download_flag == 0:
                continue
            # print(outf_name)
            # exit()
            # print(dict_i['id'])
            # l8 = l8.median()
            # l8_qa = l8.select(['QA_PIXEL'])
            # l8_i = ee.Image(dict_i['LANDSAT/LC08/C02/T1_L2/LC08_145037_20200712'])
            l8_i = ee.Image(dict_i['id'])
            l8_optical_bands = l8_i.select('Lai').multiply(0.1)
            exportOptions = {
                'scale': 500,
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
        flag = 1
        for site in T.listdir(fdir):
            fdir_i = join(fdir,site)
            outdir_i = join(outdir,site)
            T.mk_dir(outdir_i)
            for date in tqdm(T.listdir(fdir_i),desc=f'{flag}/{len(T.listdir(fdir))}'):
                fdir_i_i = join(fdir_i,date)
                for f in T.listdir(fdir_i_i):
                    fpath = join(fdir_i_i,f)
                    outpath = join(outdir_i,date+'.tif')
                    SRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt())
                    ToRaster().resample_reproj(fpath,outpath,.005,srcSRS=SRS, dstSRS='EPSG:4326')
            flag += 1

    def extract(self):
        fdir = join(self.this_class_arr,'reproj')
        outdir = join(self.this_class_arr,'extract')
        T.mk_dir(outdir)
        df = pd.DataFrame()
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
                # fpath = join(fdir_i,f)
                # arr = ToRaster().raster2array(fpath)[0]
                # arr[arr<=0] = np.nan
                # mean = np.nanmean(arr)
                # mean_list.append(mean)
                # date_list.append(f'{y}-{m:02d}-{d:02d}')
                date_list.append(date_obj)
            df['date'] = date_list
            df = df.set_index('date')
            break

        for site in T.listdir(fdir):
            fdir_i = join(fdir,site)
            mean_list = []
            date_list = []
            for f in tqdm(T.listdir(fdir_i),desc=site):
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
                arr = np.array(arr,dtype=float)
                # arr[arr<=0] = np.nan
                # mean = np.nanmean(arr)
                mean = np.nanmax(arr)
                mean_list.append(mean)
            df[site] = mean_list
        T.print_head_n(df)
        outf = join(outdir,'MODIS_LAI.df')
        T.save_df(df,outf)
        T.df_to_excel(df,outf)
        pass

class MODIS_GPP_Extraction:
    '''
    unit: kg*C m^2 8-day
    '''
    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'MODIS_GPP_Extraction1',
            this_script_root, mode=2)
        # ee.Initialize()

    def run(self):

        # self.download_images()
        # self.download_from_gee_full()
        # self.unzip()
        # self.unzip_full()
        # self.reproj()
        # self.reproj_full()
        # self.rename_reproj()
        # self.extract()
        # self.extract_tif()
        self.unit_trans()
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
        MULTIPROCESS(self.kernel_download_from_gee,params_list).run(process=10,process_or_thread='t')


    def kernel_download_from_gee(self,param):
        i,site_list,outdir,geo,geometry_list = param
        site = site_list[i]
        print(site)
        outdir_i = join(outdir, site)
        T.mk_dir(outdir_i)
        ll = geo.bounds[0:2]
        # print(ll)
        # exit()
        ur = geo.bounds[2:4]
        # ur_new = (ur[0],ur[1])
        # print(ll[0], ll[1], ur[0], ur[1])
        # exit()
        region = ee.Geometry.Rectangle(ll[0], ll[1], ur[0], ur[1])
        # print(region)
        # exit()
        startDate = '2002-01-01'
        endDate = '2022-12-31'
        # l8 = ee.ImageCollection('MODIS/061/MOD13A2')
        # l8 = ee.ImageCollection('MODIS/061/MCD15A3H')
        # l8 = ee.ImageCollection('MODIS/061/MOD17A2H')
        l8 = ee.ImageCollection('UMT/NTSG/v2/MODIS/GPP')
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
            download_flag = 0
            try:
                zip_ref = zipfile.ZipFile(out_path, 'r')
            except:
                download_flag = 1
            if isfile(out_path) and download_flag == 0:
                continue
            # print(outf_name)
            # exit()
            # print(dict_i['id'])
            # l8 = l8.median()
            # l8_qa = l8.select(['QA_PIXEL'])
            # l8_i = ee.Image(dict_i['LANDSAT/LC08/C02/T1_L2/LC08_145037_20200712'])
            l8_i = ee.Image(dict_i['id'])
            l8_optical_bands = l8_i.select('GPP').multiply(0.0001)
            exportOptions = {
                'scale': 500,
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

    def download_from_gee_full(self):
        outdir = join(self.this_class_arr,'download_full')
        T.mk_dir(outdir)
        startDate = '2001-01-01'
        endDate = '2021-12-31'
        l8 = ee.ImageCollection('UMT/NTSG/v2/MODIS/GPP')
        l8 = l8.filterDate(startDate, endDate)

        info_dict = l8.getInfo()
        # pprint.pprint(info_dict)
        # exit()
        # for key in info_dict:
        #     print(key)
        # exit()
        ids = info_dict['features']
        for i in tqdm(ids):
            dict_i = eval(str(i))
            # pprint.pprint(dict_i['id'])
            # exit()
            outf_name = dict_i['id'].split('/')[-1] + '.zip'
            out_path = join(outdir, outf_name)
            # print(out_path)
            # exit()
            download_flag = 0
            try:
                zip_ref = zipfile.ZipFile(out_path, 'r')
            except:
                download_flag = 1
            if isfile(out_path) and download_flag == 0:
                continue
            # print(outf_name)
            # exit()
            # print(dict_i['id'])
            # l8 = l8.median()
            # l8_qa = l8.select(['QA_PIXEL'])
            # l8_i = ee.Image(dict_i['LANDSAT/LC08/C02/T1_L2/LC08_145037_20200712'])
            l8_i = ee.Image(dict_i['id'])
            l8_optical_bands = l8_i.select('GPP').multiply(0.0001)
            exportOptions = {
                'scale': 5000,
                'maxPixels': 1e13,
                # 'region': region,
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

    def unzip_full(self):
        fdir = join(self.this_class_arr,'download_full')
        outdir = join(self.this_class_arr,'unzip_full')
        T.mk_dir(outdir)
        # for folder in T.listdir(fdir):
        #     fdir_i = join(fdir,folder)
        #     outdir_i = join(outdir,folder)
        T.unzip(fdir,outdir)
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
        flag = 1
        for site in T.listdir(fdir):
            fdir_i = join(fdir,site)
            outdir_i = join(outdir,site)
            T.mk_dir(outdir_i)
            for date in tqdm(T.listdir(fdir_i),desc=f'{flag}/{len(T.listdir(fdir))}'):
                fdir_i_i = join(fdir_i,date)
                for f in T.listdir(fdir_i_i):
                    fpath = join(fdir_i_i,f)
                    outpath = join(outdir_i,date+'.tif')
                    SRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt())
                    ToRaster().resample_reproj(fpath,outpath,.05,srcSRS=SRS, dstSRS='EPSG:4326')
            flag += 1

    def reproj_full(self):
        fdir = join(self.this_class_arr,'unzip_full')
        outdir = join(self.this_class_arr,'reproj_full')
        T.mk_dir(outdir)
        flag = 1
        for date in tqdm(T.listdir(fdir)):
            fdir_i = join(fdir,date)
            for f in T.listdir(fdir_i):
                if not f.endswith('.tif'):
                    continue
                fpath = join(fdir_i,f)
                # arr = ToRaster().raster2array(fpath)[0]
                # plt.imshow(arr)
                # plt.show()
                # print(fpath)
                outpath = join(outdir,date+'.tif')
                # print(outpath)
                # exit()
                # SRS = DIC_and_TIF().gen_srs_from_wkt(self.wkt())
                ToRaster().resample_reproj(fpath,outpath,.05)
                # exit()
        flag += 1

    def rename_reproj(self):
        fdir = join(self.this_class_arr,'reproj_full')
        for f in T.listdir(fdir):
            print(f)
            date = f.split('.tif')[0]
            year = int(date[:4])
            doy = int(date[4:])
            date_obj = datetime.datetime(year,1,1) + datetime.timedelta(doy-1)
            month = date_obj.month
            day = date_obj.day
            new_fname = f'{year}{month:02d}{day:02d}.tif'
            fpath = join(fdir,f)
            new_fpath = join(fdir,new_fname)
            os.rename(fpath,new_fpath)
        pass

    def extract_gee(self):
        fdir = join(self.this_class_arr,'reproj')
        outdir = join(self.this_class_arr,'extract')
        T.mk_dir(outdir)
        df = pd.DataFrame()
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
                # fpath = join(fdir_i,f)
                # arr = ToRaster().raster2array(fpath)[0]
                # arr[arr<=0] = np.nan
                # mean = np.nanmean(arr)
                # mean_list.append(mean)
                # date_list.append(f'{y}-{m:02d}-{d:02d}')
                date_list.append(date_obj)
            df['date'] = date_list
            df = df.set_index('date')
            break

        for site in T.listdir(fdir):
            fdir_i = join(fdir,site)
            mean_list = []
            date_list = []
            for f in tqdm(T.listdir(fdir_i),desc=site):
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
                arr = np.array(arr,dtype=float)
                # arr[arr<=0] = np.nan
                mean = np.nanmean(arr)
                mean_list.append(mean)
            df[site] = mean_list
        T.print_head_n(df)
        outf = join(outdir,'MODIS_GPP.df')
        T.save_df(df,outf)
        T.df_to_excel(df,outf)
        pass

    def extract_tif(self):
        outdir = join(self.this_class_arr,'extract_tif')
        T.mk_dir(outdir)
        pix_list,name_list = self.get_pix_list()
        GPP_fdir = '/Users/liyang/Projects_data/LAI_extraction/results/MODIS_GPP_Extraction1/arr/reproj_full'
        result_dict = {}
        for f in tqdm(T.listdir(GPP_fdir)):
            if not f.endswith('.tif'):
                continue
            fpath = join(GPP_fdir,f)
            date = f.split('.')[0]
            # print(date)
            # exit()
            year = int(date[:4])
            month = int(date[4:6])
            day = int(date[6:8])
            date_obj = datetime.datetime(year,month,day)
            # print(date_obj)
            # exit()
            arr = ToRaster().raster2array(fpath)[0]
            # print(arr.shape)
            # plt.imshow(arr)
            # plt.show()
            arr = np.array(arr, dtype=float)
            arr[arr<=0] = np.nan
            arr[arr>999] = np.nan
            # arr = arr*0.01
            # plt.imshow(arr,interpolation='nearest',cmap='jet')
            # plt.colorbar()
            # plt.show()
            for i in range(len(pix_list)):
                pix = pix_list[i]
                name = name_list[i]
                # print(pix)
                r,c = pix
                fail = 0
                if r < 0 or r>= len(arr) or c < 0 or c >= len(arr[0]):
                    fail = 1
                if fail == 1:
                    val = np.nan
                else:
                    val = arr[pix]
                if not name in result_dict:
                    result_dict[name] = {}
                result_dict[name][date_obj] = val

        df = T.dic_to_df(result_dict,'date')
        df = df.set_index('date').T
        # T.print_head_n(df)
        # exit()
        outf = join(outdir,f'GPP_MODIS.df')
        T.save_df(df,outf)
        T.df_to_excel(df,outf)
        pass

    def unit_trans(self):
        dff = join(self.this_class_arr,'extract_tif','GPP_MODIS.df')
        dff_trans = join(self.this_class_arr,'extract_tif','GPP_MODIS_trans.df')
        df = T.load_df(dff)
        T.print_head_n(df)
        for col in df.columns:
            vals = df[col]
            df[col] = vals * 1000 / 8
        T.save_df(df,dff_trans)
        T.df_to_excel(df,dff_trans)

    def get_pix_list(self):
        point_list, name_list = Expand_points_to_rectangle().read_point_shp()
        tif_template = '/Users/liyang/Projects_data/LAI_extraction/results/MODIS_GPP_Extraction1/arr/reproj_full/20010101.tif'
        # print(point_list)
        D = DIC_and_TIF(tif_template=tif_template)
        # print(D.pixelWidth)
        # print(D.pixelHeight)
        # print(D.originX)
        # # print(D.endX)
        # print(D.originY)
        # print(D.endY)
        # exit()

        lon_list = []
        lat_list = []
        for point in point_list:
            lon,lat = point
            # print(lon,lat)
            lon_list.append(lon)
            lat_list.append(lat)
        # exit()
        pix_list = D.lon_lat_to_pix(lon_list, lat_list)
        return pix_list,name_list

class Extract_GPP_CFE:
    def __init__(self):
        '''
        unit: gC m-2 day-1
        '''
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'Extract_GPP_CFE',
            this_script_root, mode=2)
        pass

    def run(self):
        # self.extract()
        self.plot_ts()
        pass

    def get_pix_list(self):
        point_list, name_list = Expand_points_to_rectangle().read_point_shp()
        tif_template = '/Volumes/NVME4T/hotdrought_CMIP/data/GPP/tif/LT_CFE-Hybrid_NT/1982-2020/CEDAR-GPP_v01_LT_CFE-Hybrid_NT_198311.tif'
        # print(point_list)
        D = DIC_and_TIF(tif_template=tif_template)

        lon_list = []
        lat_list = []
        for point in point_list:
            lon,lat = point
            lon_list.append(lon)
            lat_list.append(lat)
        pix_list = D.lon_lat_to_pix(lon_list, lat_list)
        return pix_list,name_list

    def extract(self):
        outdir = join(self.this_class_arr,'extract')
        T.mk_dir(outdir)
        pix_list,name_list = self.get_pix_list()
        # product = 'LT_CFE-Hybrid_NT'
        product = 'LT_Baseline_NT'
        GPP_fdir = f'/Volumes/NVME4T/hotdrought_CMIP/data/GPP/tif/{product}/1982-2020'
        result_dict = {}
        for f in tqdm(T.listdir(GPP_fdir)):
            if not f.endswith('.tif'):
                continue
            fpath = join(GPP_fdir,f)
            date = f.split('.')[0].split('_')[-1]
            # print(date)
            year = int(date[:4])
            month = int(date[4:6])
            date_obj = datetime.datetime(year,month,1)
            # print(date_obj)
            # exit()
            arr = ToRaster().raster2array(fpath)[0]
            for i in range(len(pix_list)):
                pix = pix_list[i]
                name = name_list[i]
                val = arr[pix]
                if not name in result_dict:
                    result_dict[name] = {}
                result_dict[name][date_obj] = val

        df = T.dic_to_df(result_dict,'date')
        df = df.set_index('date').T
        # T.print_head_n(df)
        # exit()
        outf = join(outdir,f'{product}.df')
        T.save_df(df,outf)
        T.df_to_excel(df,outf)
        pass

    def plot_ts(self):
        fdir = join(self.this_class_arr,'extract')
        product = 'LT_Baseline_NT'
        fpath = join(fdir,f'{product}.df')
        df = T.load_df(fpath)
        # for i,row in df.iterrows():
        #     print(i)
        # exit()
        T.print_head_n(df)
        site = 'US-Wkg'
        vals = df[site]
        date = df.index
        plt.plot(date,vals)
        plt.show()
        pass

class Extract_GIMMS4g:
    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'Extract_GIMMS4g',
            this_script_root, mode=2)
        pass

    def run(self):
        self.extract()
        pass

    def get_pix_list(self):
        point_list, name_list = Expand_points_to_rectangle().read_point_shp()
        tif_template = '/Volumes/NVME4T/hotdrought_CMIP/data/LAI4g/tif/19820101.tif'
        # print(point_list)
        D = DIC_and_TIF(tif_template=tif_template)

        lon_list = []
        lat_list = []
        for point in point_list:
            lon,lat = point
            lon_list.append(lon)
            lat_list.append(lat)
        pix_list = D.lon_lat_to_pix(lon_list, lat_list)
        return pix_list,name_list

    def extract(self):
        outdir = join(self.this_class_arr,'extract')
        T.mk_dir(outdir)
        pix_list,name_list = self.get_pix_list()
        GPP_fdir = '/Volumes/NVME4T/hotdrought_CMIP/data/LAI4g/tif'
        result_dict = {}
        for f in tqdm(T.listdir(GPP_fdir)):
            if not f.endswith('.tif'):
                continue
            fpath = join(GPP_fdir,f)
            date = f.split('.')[0]
            # print(date)
            # exit()
            year = int(date[:4])
            month = int(date[4:6])
            day = int(date[6:8])
            date_obj = datetime.datetime(year,month,day)
            # print(date_obj)
            # exit()
            arr = ToRaster().raster2array(fpath)[0]
            arr = np.array(arr, dtype=float)
            arr[arr<-999] = np.nan
            arr[arr>999] = np.nan
            arr = arr*0.01
            # plt.imshow(arr,interpolation='nearest',cmap='jet')
            # plt.colorbar()
            # plt.show()
            for i in range(len(pix_list)):
                pix = pix_list[i]
                name = name_list[i]
                val = arr[pix]
                if not name in result_dict:
                    result_dict[name] = {}
                result_dict[name][date_obj] = val

        df = T.dic_to_df(result_dict,'date')
        df = df.set_index('date').T
        # T.print_head_n(df)
        # exit()
        outf = join(outdir,f'LAI4g.df')
        T.save_df(df,outf)
        T.df_to_excel(df,outf)
        pass

class Extract_GIMMS3g:
    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'Extract_GIMMS3g',
            this_script_root, mode=2)
        pass

    def run(self):
        # self.rename()
        self.extract()
        pass

    def rename(self):
        fdir = '/Users/liyang/Projects_data/LAI_extraction/data/BU-GIMMS3gV1-LAI-1981-2018'
        for f in T.listdir(fdir):
            date = self.__fname_to_date(f)
            fpath = join(fdir,f)
            new_f = f'{date}.tif'
            print(new_f)
            os.rename(fpath,join(fdir,new_f))
        # exit()
        pass

    def __mon_to_num(self,mon):

        mon_num_dict = {
            'jan':1,
            'feb':2,
            'mar':3,
            'apr':4,
            'may':5,
            'jun':6,
            'jul':7,
            'aug':8,
            'sep':9,
            'oct':10,
            'nov':11,
            'dec':12
        }
        return mon_num_dict[mon]

    def __letter_to_num(self,letter):
        letter_to_num_dict = {
            'a':1,
            'b':15,
        }
        return letter_to_num_dict[letter]

    def __fname_to_date(self,fname):
        date_str = fname.split('.')[-2]
        year_str = date_str[0:4]
        mon_str = date_str[4:7]
        day_str = date_str[7:8]
        mon = self.__mon_to_num(mon_str)
        day = self.__letter_to_num(day_str)
        date = f'{year_str}{mon:02d}{day:02d}'
        return date


    def get_pix_list(self):
        point_list, name_list = Expand_points_to_rectangle().read_point_shp()
        tif_template = '/Users/liyang/Projects_data/LAI_extraction/data/BU-GIMMS3gV1-LAI-1981-2018/19820101.tif'
        # print(point_list)
        D = DIC_and_TIF(tif_template=tif_template)

        lon_list = []
        lat_list = []
        for point in point_list:
            lon,lat = point
            lon_list.append(lon)
            lat_list.append(lat)
        pix_list = D.lon_lat_to_pix(lon_list, lat_list)
        return pix_list,name_list

    def extract(self):
        outdir = join(self.this_class_arr,'extract')
        T.mk_dir(outdir)
        pix_list,name_list = self.get_pix_list()
        GPP_fdir = '/Users/liyang/Projects_data/LAI_extraction/data/BU-GIMMS3gV1-LAI-1981-2018'
        result_dict = {}
        for f in tqdm(T.listdir(GPP_fdir)):
            if not f.endswith('.tif'):
                continue
            fpath = join(GPP_fdir,f)
            date = f.split('.')[0]
            # print(date)
            # exit()
            year = int(date[:4])
            month = int(date[4:6])
            day = int(date[6:8])
            date_obj = datetime.datetime(year,month,day)
            # print(date_obj)
            # exit()
            arr = ToRaster().raster2array(fpath)[0]
            # arr = np.array(arr, dtype=float)
            # arr[arr<-999] = np.nan
            # arr[arr>999] = np.nan
            # plt.imshow(arr,interpolation='nearest',cmap='jet')
            # plt.colorbar()
            # plt.show()
            for i in range(len(pix_list)):
                pix = pix_list[i]
                name = name_list[i]
                val = arr[pix]
                if not name in result_dict:
                    result_dict[name] = {}
                result_dict[name][date_obj] = val

        df = T.dic_to_df(result_dict,'date')
        df = df.set_index('date').T
        # T.print_head_n(df)
        # exit()
        outf = join(outdir,f'LAI3g.df')
        T.save_df(df,outf)
        T.df_to_excel(df,outf)
        pass


class Statistic_all:

    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'Statistic_all',
            this_script_root, mode=2)
        pass

    def run(self):
        self.foo()
        pass

    def foo(self):
        # dff_path_dict = self.LAI_dff_path_dict()
        dff_path_dict = self.GPP_dff_path_dict()
        site_list = ''
        for product in dff_path_dict:
            df = dff_path_dict[product]
            site_list = df.columns.tolist()

        vals_list = []
        label_list = []
        for site in site_list:
            for product in dff_path_dict:
                # print(product)
                df = dff_path_dict[product]
                vals = df[site].tolist()
                # vals = vals[:888]
                vals = T.remove_np_nan(vals)
                vals_list.append(vals)
                date_list = df.index.tolist()
                year_list = []
                mon_list = []
                day_list = []
                for date in date_list:
                    year = date.year
                    mon = date.month
                    day = date.day
                    year_list.append(year)
                    mon_list.append(mon)
                    day_list.append(day)
                df['year'] = year_list
                df['month'] = mon_list
                df['day'] = day_list
                # df = df[df.index.year>2000]
                df = df[df['year']>=2001]
                # T.print_head_n(df)
                # print(df)
                # print(date_list)
                # exit()
                # print(product,len(vals))
                label_list.append(f'{site}-{product}')
            label_list.append(f'')
            vals_list.append([np.nan])
            # plt.legend()
            # plt.title(site)
            # plt.show()
            # plt.scatter(vals_list[0],vals_list[1])
            # plt.show()
        plt.boxplot(vals_list,labels=label_list,showfliers=False,vert=False)
        plt.show()
        pass

    def LAI_dff_path_dict(self):

        path_dict = {
            # 'LT_Baseline_NT':join(Extract_GPP_CFE().this_class_arr,'extract/LT_Baseline_NT.df'),
            # 'LT_CFE-Hybrid_NT':join(Extract_GPP_CFE().this_class_arr,'extract/LT_CFE-Hybrid_NT.df'),
            'LAI4g':join(Extract_GIMMS4g().this_class_arr,'extract/LAI4g.df'),
            'LAI3g':join(Extract_GIMMS3g().this_class_arr,'extract/LAI3g.df'),
            'LAI_MODIS':join(MODIS_LAI_Extraction().this_class_arr,'extract/MODIS_LAI.df'),
        }
        df_dict = {}
        for key in path_dict:
            dff = path_dict[key]
            df = T.load_df(dff)
            df_dict[key] = df
        return df_dict

    def GPP_dff_path_dict(self):

        path_dict = {
            'LT_Baseline_NT':join(Extract_GPP_CFE().this_class_arr,'extract/LT_Baseline_NT.df'),
            'LT_CFE-Hybrid_NT':join(Extract_GPP_CFE().this_class_arr,'extract/LT_CFE-Hybrid_NT.df'),
            # 'MODIS_GPP':join(MODIS_GPP_Extraction().this_class_arr,'extract_tif/GPP_MODIS.df'),
            'MODIS_GPP':join(MODIS_GPP_Extraction().this_class_arr,'extract_tif/GPP_MODIS_trans.df'),
            # 'LAI4g':join(Extract_GIMMS4g().this_class_arr,'extract/LAI4g.df'),
            # 'LAI3g':join(Extract_GIMMS3g().this_class_arr,'extract/LAI3g.df'),
        }
        df_dict = {}
        for key in path_dict:
            dff = path_dict[key]
            df = T.load_df(dff)
            df_dict[key] = df
        return df_dict



def main():
    # Expand_points_to_rectangle().run()
    # MODIS_LAI_Extraction().run()
    # MODIS_GPP_Extraction().run()
    # Extract_GPP_CFE().run()
    # Extract_GIMMS4g().run()
    # Extract_GIMMS3g().run()
    Statistic_all().run()
    pass


if __name__ == '__main__':
    main()