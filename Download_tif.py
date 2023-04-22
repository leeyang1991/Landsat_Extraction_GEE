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

this_script_root = join(this_root, 'Download_tif')

class Landsat8:

    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'Landsat8',
            this_script_root, mode=2)
        ee.Initialize()

    def run(self):

        # self.RGB_compose()
        # self.reproj()
        # self.count_image_number()
        # self.download_images()
        self.unzip()
        self.rename()
        pass


    def count_image_number(self):
        MASK_CLOUD = 1
        # MASK_CLOUD = 0
        rectangle_f = join(Expand_points_to_rectangle().this_class_arr, 'grasssite_rectangle/grasssite_rectangle.shp')
        rectangle_df = gpd.read_file(rectangle_f)
        geometry_list = rectangle_df['geometry'].tolist()
        for geo in geometry_list:
            ll = geo.bounds[0:2]
            ur = geo.bounds[2:4]
            region = ee.Geometry.Rectangle(ll[0], ll[1], ur[0], ur[1])
            startDate = '2013-01-01'
            endDate = '2020-12-31'
            l8 = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
            # l8 = ee.ImageCollection('LANDSAT/LC08/C01/T1_SR')
            l8 = l8.filterDate(startDate, endDate).filterBounds(region)

            info_dict = l8.getInfo()
            # pprint.pprint(info_dict)
            # exit()
            # for key in info_dict:
            #     print(key)
            print('------------------')
            ids = info_dict['features']
            date_list = []
            y = []
            c_list = []
            for i in ids:
                dict_i = eval(str(i))
                pprint.pprint(dict_i['id'])
                date = dict_i['properties']['DATE_ACQUIRED']
                date_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
                cloud_cover = dict_i['properties']['CLOUD_COVER']
                if cloud_cover > 20:
                    continue
                #     y.append(2)
                #     c_list.append('r')
                # else:
                #     y.append(1)
                #     c_list.append('b')
                date_list.append(date_obj)
                y.append(1)
            # plt.bar(date_list, y,color=c_list,width=15)
            plt.bar(date_list, y,width=15,alpha=0.7)
            # plt.ylim(-1,2)
            plt.show()


            pass

        pass

    def download_images(self):
        outdir = join(self.this_class_arr,'download_images')
        T.mk_dir(outdir)
        MASK_CLOUD = 1
        # MASK_CLOUD = 0
        rectangle_f = join(Expand_points_to_rectangle().this_class_arr,'grasssite_rectangle/grasssite_rectangle.shp')
        rectangle_df = gpd.read_file(rectangle_f)
        geometry_list = rectangle_df['geometry'].tolist()[0:1]
        # print(geometry_list)
        # exit()
        for geo in geometry_list:
            ll = geo.bounds[0:2]
            ur = geo.bounds[2:4]
            region = ee.Geometry.Rectangle(ll[0],ll[1],ur[0],ur[1])
            startDate = '2013-01-01'
            endDate = '2020-12-31'
            l8 = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
            # l8 = ee.ImageCollection('LANDSAT/LC08/C01/T1_SR')
            l8 = l8.filterDate(startDate, endDate).filterBounds(region)

            info_dict = l8.getInfo()
            # pprint.pprint(info_dict)
            # exit()
            # for key in info_dict:
            #     print(key)
            print('------------------')
            ids = info_dict['features']
            for i in tqdm(ids):
                dict_i = eval(str(i))
                # pprint.pprint(dict_i['id'])
                outf_name = dict_i['id'].split('/')[-1]+'.zip'
                cloud_cover = dict_i['properties']['CLOUD_COVER']
                if cloud_cover > 20:
                    # print(dict_i['id'],cloud_cover,'cloud_cover too high')
                    continue
                # print(dict_i['id'])
                # l8 = l8.median()
                # l8_qa = l8.select(['QA_PIXEL'])
                # l8_i = ee.Image(dict_i['LANDSAT/LC08/C02/T1_L2/LC08_145037_20200712'])
                l8_i = ee.Image(dict_i['id'])
                l8_optical_bands = l8_i.select('SR_B.').multiply(2.75e-05).add(-0.2)
                l8_qa_band = l8_i.select('QA_PIXEL')
                if MASK_CLOUD == 1:
                    l8_cloud_mask = self.mask_clouds(l8_optical_bands,l8_qa_band)
                else:
                    l8_cloud_mask = l8_optical_bands
                exportOptions = {
                    'scale': 30,
                    'maxPixels': 1e13,
                    'region': region,
                    'fileNamePrefix': 'exampleExport',
                    'description': 'imageToAssetExample',
                }
                # url = l8_optical_bands.getDownloadURL(exportOptions)
                url = l8_cloud_mask.getDownloadURL(exportOptions)
                out_path = join(outdir,outf_name)
                try:
                    self.download_i(url,out_path)
                except:
                    print('download error',out_path)
                    continue
                # T.open_path_and_file(outdir)
                # exit()

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
        T.unzip(fdir,outdir)

        pass

    def rename(self):
        fdir = join(self.this_class_arr,'unzip')
        for folder in T.listdir(fdir):
            folder_path = join(fdir,folder)
            for f in T.listdir(folder_path):
                f_path = join(folder_path,f)
                f_new = f.replace('download.','')
                f_new_path = join(folder_path,f_new)
                os.rename(f_path,f_new_path)


    def cloud_shadows(self,image):
        # QA = image.select(['pixel_qa'])
        QA = image.select(['QA_PIXEL'])
        return self.getQABits(QA, 4, 4, 'cloud_shadows').eq(0)
        pass

    def snow(self,image):
        # QA = image.select(['pixel_qa'])
        QA = image.select(['QA_PIXEL'])
        return self.getQABits(QA, 4, 4, 'cloud_shadows').eq(0)
        pass


    def getQABits(self,image, start, end, newName):
        pattern = 0
        image = image.int()
        # e
        # make image type int
        # image = image.toInt16()
        for i in range(start, end + 1):
            pattern += math.pow(2, i)
            # print(pattern)
        return image.select([0], [newName]).bitwiseAnd(int(pattern)).rightShift(start)

    def clouds(self,image):
        QA = image.select(['QA_PIXEL'])
        return self.getQABits(QA, 3, 3, 'clouds').eq(0)

    def mask_clouds(self,image,image_qa):
        # image_qa,image = params
        cs = self.cloud_shadows(image_qa)
        c = self.clouds(image_qa)
        s = self.snow(image_qa)
        image = image.updateMask(cs).updateMask(c).updateMask(s)
        return image


    def RGB_compose(self):
        # fdir = '/Users/liyang/Downloads/download-4'
        fdir = '/Users/liyang/Downloads/download-5'
        red = join(fdir,'download.SR_B4.tif')
        green = join(fdir,'download.SR_B3.tif')
        blue = join(fdir,'download.SR_B2.tif')

        red_band = gdal.Open(red)
        green_band = gdal.Open(green)
        blue_band = gdal.Open(blue)

        width = red_band.RasterXSize
        height = red_band.RasterYSize

        driver = gdal.GetDriverByName('GTiff')
        out = driver.Create(join(fdir,'RGB.tif'),width,height,3,gdal.GDT_Float32)

        out.GetRasterBand(1).WriteArray(red_band.GetRasterBand(1).ReadAsArray())
        out.GetRasterBand(2).WriteArray(green_band.GetRasterBand(1).ReadAsArray())
        out.GetRasterBand(3).WriteArray(blue_band.GetRasterBand(1).ReadAsArray())

        out.SetProjection(red_band.GetProjection())
        out.SetGeoTransform(red_band.GetGeoTransform())

        red_band = None
        green_band = None
        blue_band = None
        out = None

    def reproj(self):
        in_file = '/Users/liyang/Downloads/download-5/RGB.tif'
        out_file = '/Users/liyang/Downloads/download-5/RGB_reproj.tif'
        ToRaster().resample_reproj(in_file,out_file,30,srcSRS='EPSG:32644', dstSRS='EPSG:3857')


class Landsat5:

    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'Landsat5',
            this_script_root, mode=2)
        ee.Initialize()

    def run(self):

        # self.RGB_compose()
        # self.reproj()
        self.download_images()
        pass

    def download_images(self):
        MASK_CLOUD = 1
        # MASK_CLOUD = 0
        rectangle_f = join(Expand_points_to_rectangle().this_class_arr,'grasssite_rectangle/grasssite_rectangle.shp')
        rectangle_df = gpd.read_file(rectangle_f)
        geometry_list = rectangle_df['geometry'].tolist()
        for geo in geometry_list:
            ll = geo.bounds[0:2]
            ur = geo.bounds[2:4]
            region = ee.Geometry.Rectangle(ll[0],ll[1],ur[0],ur[1])
            startDate = '2001-01-01'
            endDate = '2004-12-31'
            l8 = ee.ImageCollection('LANDSAT/LT05/C02/T1_L2')
            # l8 = ee.ImageCollection('LANDSAT/LC08/C01/T1_SR')
            l8 = l8.filterDate(startDate, endDate).filterBounds(region)

            info_dict = l8.getInfo()
            # pprint.pprint(info_dict)
            # exit()
            # for key in info_dict:
            #     print(key)
            print(geo)
            print('------------------')
            ids = info_dict['features']
            print(ids)
            for i in ids:
                dict_i = eval(str(i))
                # pprint.pprint(dict_i)
                cloud_cover = dict_i['properties']['CLOUD_COVER']
                if cloud_cover > 20:
                    print(dict_i['id'],cloud_cover,'cloud_cover too high')
                    continue
                print(dict_i['id'])
                # l8 = l8.median()
                # l8_qa = l8.select(['QA_PIXEL'])
                # l8_i = ee.Image(dict_i['LANDSAT/LC08/C02/T1_L2/LC08_145037_20200712'])
                l8_i = ee.Image(dict_i['id'])
                l8_optical_bands = l8_i.select('SR_B.').multiply(2.75e-05).add(-0.2)
                l8_qa_band = l8_i.select('QA_PIXEL')
                if MASK_CLOUD == 1:
                    l8_cloud_mask = self.mask_clouds(l8_optical_bands,l8_qa_band)
                else:
                    l8_cloud_mask = l8_optical_bands
                exportOptions = {
                    'scale': 30,
                    'maxPixels': 1e13,
                    'region': region
                }
                # url = l8_optical_bands.getDownloadURL(exportOptions)
                url = l8_cloud_mask.getDownloadURL(exportOptions)
                print(url)
                exit()


            pass

    def cloud_shadows(self,image):
        # QA = image.select(['pixel_qa'])
        QA = image.select(['QA_PIXEL'])
        return self.getQABits(QA, 4, 4, 'cloud_shadows').eq(0)
        pass

    def snow(self,image):
        # QA = image.select(['pixel_qa'])
        QA = image.select(['QA_PIXEL'])
        return self.getQABits(QA, 4, 4, 'cloud_shadows').eq(0)
        pass


    def getQABits(self,image, start, end, newName):
        pattern = 0
        image = image.int()
        # e
        # make image type int
        # image = image.toInt16()
        for i in range(start, end + 1):
            pattern += math.pow(2, i)
            # print(pattern)
        return image.select([0], [newName]).bitwiseAnd(int(pattern)).rightShift(start)

    def clouds(self,image):
        QA = image.select(['QA_PIXEL'])
        return self.getQABits(QA, 3, 3, 'clouds').eq(0)

    def mask_clouds(self,image,image_qa):
        # image_qa,image = params
        cs = self.cloud_shadows(image_qa)
        c = self.clouds(image_qa)
        s = self.snow(image_qa)
        image = image.updateMask(cs).updateMask(c).updateMask(s)
        return image


    def RGB_compose(self):
        # fdir = '/Users/liyang/Downloads/download-4'
        fdir = '/Users/liyang/Downloads/download-5'
        red = join(fdir,'download.SR_B4.tif')
        green = join(fdir,'download.SR_B3.tif')
        blue = join(fdir,'download.SR_B2.tif')

        red_band = gdal.Open(red)
        green_band = gdal.Open(green)
        blue_band = gdal.Open(blue)

        width = red_band.RasterXSize
        height = red_band.RasterYSize

        driver = gdal.GetDriverByName('GTiff')
        out = driver.Create(join(fdir,'RGB.tif'),width,height,3,gdal.GDT_Float32)

        out.GetRasterBand(1).WriteArray(red_band.GetRasterBand(1).ReadAsArray())
        out.GetRasterBand(2).WriteArray(green_band.GetRasterBand(1).ReadAsArray())
        out.GetRasterBand(3).WriteArray(blue_band.GetRasterBand(1).ReadAsArray())

        out.SetProjection(red_band.GetProjection())
        out.SetGeoTransform(red_band.GetGeoTransform())

        red_band = None
        green_band = None
        blue_band = None
        out = None

    def reproj(self):
        in_file = '/Users/liyang/Downloads/download-5/RGB.tif'
        out_file = '/Users/liyang/Downloads/download-5/RGB_reproj.tif'
        ToRaster().resample_reproj(in_file,out_file,30,srcSRS='EPSG:32644', dstSRS='EPSG:3857')

def main():
    Landsat8().run()
    # Landsat5().run()
    # GEE_MAP().run()
    pass

if __name__ == '__main__':
    main()