# coding=utf-8
import matplotlib.pyplot as plt
import urllib3
from __init__ import *
import ee
from Preprocess import *
import math
import pprint
# import geemap
# exit()

this_script_root = join(this_root, 'ERA5')

class ERA5_daily:

    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'ERA5_daily',
            this_script_root, mode=2)
        self.product = 'mean_2m_air_temperature'
        # ee.Initialize()

    def run(self):
        # for year in range(1982,2021):
        #     self.download_images(year)
        # self.download_images()
        self.unzip()
        # self.reproj()
        # self.statistic()
        pass

    def download_images(self,year=1982):
        outdir = join(self.this_class_arr,self.product,str(year))
        T.mk_dir(outdir,force=True)
        startDate = f'{year}-01-01'
        endDate = f'{year+1}-01-01'
        Collection = ee.ImageCollection('ECMWF/ERA5/DAILY')
        # l8 = ee.ImageCollection('LANDSAT/LC08/C01/T1_SR')
        Collection = Collection.filterDate(startDate, endDate)

        info_dict = Collection.getInfo()
        # pprint.pprint(info_dict)
        # print(len(info_dict['features']))
        # exit()
        # for key in info_dict:
        #     print(key)
        ids = info_dict['features']
        for i in tqdm(ids):
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
            Image_product = Image.select(self.product)
            exportOptions = {
                'scale': 27830,
                'maxPixels': 1e13,
                # 'region': region,
                # 'fileNamePrefix': 'exampleExport',
                # 'description': 'imageToAssetExample',
            }
            url = Image_product.getDownloadURL(exportOptions)

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
            fdir_i = join(fdir,folder)
            # T.open_path_and_file(fdir_i,folder)
            # exit()
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

class ERA5_hourly:

    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'ERA5_hourly',
            this_script_root, mode=2)
        self.product = 'temperature_2m'

    def run(self):
        ee.Initialize()
        date_range_list = self.gen_date_list(start_date = '1982-01-01',end_date = '2023-01-02')
        outdir = join(self.this_class_arr,self.product)

        params_list = []
        for date_range in tqdm(date_range_list):
            startDate = date_range[0]
            endDate = date_range[1]
            params = [startDate,endDate,outdir]
            params_list.append(params)
            # self.download_images(startDate,endDate,outdir)

        MULTIPROCESS(self.download_images,params_list).run(process=10,process_or_thread='t')



        # self.unzip()
        # self.reproj()
        # self.statistic()
        pass

    def download_images(self,params):
        startDate, endDate, outdir = params
        start_year = startDate.split('-')[0]
        outdir_mean = join(outdir, 'mean',start_year)
        # outdir_max = join(outdir, 'max',start_year)
        # outdir_min = join(outdir, 'min',start_year)

        T.mk_dir(outdir_mean,force=True)
        # T.mk_dir(outdir_max,force=True)
        # T.mk_dir(outdir_min,force=True)
        # print(startDate)
        # exit()

        out_path_mean = join(outdir_mean, f'{startDate.replace("-","")}.zip')
        # out_path_max = join(outdir_max, f'{startDate.replace("-","")}.zip')
        # out_path_min = join(outdir_min, f'{startDate.replace("-","")}.zip')
        T.mk_dir(outdir,force=True)
        # startDate = f'{year}-01-01'
        # endDate = f'{year+1}-01-01'
        # startDate = '2011-04-03'
        # endDate = '2011-04-04'
        Collection = ee.ImageCollection('ECMWF/ERA5_LAND/HOURLY')
        # l8 = ee.ImageCollection('LANDSAT/LC08/C01/T1_SR')
        Collection = Collection.filterDate(startDate, endDate)
        # Image = Collection.mean()
        Image_product_mean = Collection.select(self.product).mean()
        # Image_product_max = Collection.select(self.product).max()
        # Image_product_min = Collection.select(self.product).min()

        exportOptions = {
            # 'scale': 27830,
            'scale': 27830*2,
            # 'scale': 11132,
            'maxPixels': 1e13,
            # 'region': region,
            # 'fileNamePrefix': 'exampleExport',
            # 'description': 'imageToAssetExample',
        }
        url_mean = Image_product_mean.getDownloadURL(exportOptions)
        # url_max = Image_product_max.getDownloadURL(exportOptions)
        # url_min = Image_product_min.getDownloadURL(exportOptions)

        try:
            self.download_i(url_mean, out_path_mean)
            # self.download_i(url_max, out_path_max)
            # self.download_i(url_min, out_path_min)
        except:
            # print('download error', out_path_mean, out_path_max, out_path_min)
            print('download error', out_path_mean)

        # info_dict = Collection.getInfo()
        # pprint.pprint(info_dict)
        # print(len(info_dict['features']))
        # exit()
        # for key in info_dict:
        #     print(key)
        # ids = info_dict['features']
        # for i in tqdm(ids):
        #     dict_i = eval(str(i))
            # pprint.pprint(dict_i['id'])
            # exit()
            # outf_name = dict_i['id'].split('/')[-1] + '.zip'
            # out_path = join(outdir, outf_name)
            # if isfile(out_path):
            #     continue
            # print(outf_name)
            # exit()
            # print(dict_i['id'])
            # l8 = l8.median()
            # l8_qa = l8.select(['QA_PIXEL'])
            # l8_i = ee.Image(dict_i['LANDSAT/LC08/C02/T1_L2/LC08_145037_20200712'])
            # Image = ee.Image(dict_i['id'])
            # Image_product = Image.select('total_precipitation')
            # Image_product = Image.select(self.product)

        # pass

    def gen_date_list(self,start_date = '1982-01-01',end_date = '2023-01-02'):

        days_count = T.count_days_of_two_dates(start_date, end_date)
        date_list = []
        base_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        for i in range(days_count):
            date = base_date + datetime.timedelta(days=i)
            date_list.append(date.strftime('%Y-%m-%d'))
        date_range_list = []
        for i in range(len(date_list) - 1):
            date_range_list.append([date_list[i], date_list[i + 1]])
        return date_range_list


    def download_i(self,url,outf):
        try:
            zip_ref = zipfile.ZipFile(outf, 'r')
        except:
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
            fdir_i = join(fdir,folder)
            # T.open_path_and_file(fdir_i,folder)
            # exit()
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
    # ERA5_daily().run()
    ERA5_hourly().run()
    pass

if __name__ == '__main__':
    main()
