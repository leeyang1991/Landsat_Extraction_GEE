# coding=utf-8
import thinc.layers
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

def main():
    # Expand_points_to_rectangle().run()
    MODIS_LAI().run()
    pass

if __name__ == '__main__':
    main()