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

this_script_root = join(this_root, 'ERA5_daily')

class ERA5_daily:

    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'ERA5_daily',
            this_script_root, mode=2)
        self.product = 'mean_2m_air_temperature'
        ee.Initialize()

    def run(self):
        for year in range(1982,2021):
            self.download_images(year)
        # self.download_images()
        # self.unzip()
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
    ERA5_daily().run()
    pass

if __name__ == '__main__':
    main()
