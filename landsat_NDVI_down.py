# coding=utf-8
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


this_script_root = join(this_root, 'landsat_NDVI_down')


class landsat_NDVI:

    def __init__(self):
        self.datadir = join(data_root, 'NDVI')

    def run(self):
        self.download_images()
        # self.unzip()
        # self.merge()
        # self.reproj()
        pass

    def download_images(self):
        ee.Initialize()

        startDate = '1982-01-01'
        endDate = '2023-1-1'
        # resolution = 25000 # meter
        band_name = 'NDVI'
        product_name = 'LANDSAT/COMPOSITES/C02/T1_L2_32DAY_NDVI'
        outdir = join(self.datadir,'download_images/zip')
        T.mk_dir(outdir,force=True)

        Collection = ee.ImageCollection(product_name)
        Collection = Collection.filterDate(startDate, endDate)

        info_dict = Collection.getInfo()
        # pprint.pprint(info_dict)
        # exit()
        # for key in info_dict:
        #     print(key)
        print('------------------')
        ids = info_dict['features']
        # print(ids)
        for i in tqdm(ids):
            dict_i = eval(str(i))
            # pprint.pprint(dict_i)
            fname = dict_i['properties']['system:index']
            # outf = join(outdir,outf)
            # exit()
            # print(dict_i['id'])
            l8_i = ee.Image(dict_i['id'])
            l8_optical_bands = l8_i.select(band_name)

            task = ee.batch.Export.image.toDrive(**{
                'image': l8_optical_bands,
                'description': fname,
                'folder': 'NDVI_download',
                'scale': 10000,
            })
            task.start()
            time_init = 0
            while task.active():
                # print('Polling for task (id: {}).'.format(task.id))
                print('waiting')
                print(fname)
                time.sleep(10)
                time_init += 10
                print('time:',time_init)

            # url = l8_optical_bands.getDownloadURL(exportOptions)
            # print(url)
            # self.download_i(url,outf)
            # exit(111)

        pass



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


def main():
    landsat_NDVI().run()
    pass

if __name__ == '__main__':
    main()