# coding=utf-8
from __init__ import *
import ee
from Preprocess import *
import math
# import geemap
# exit()

this_script_root = join(this_root, 'Download_tif')

class Download_tif:

    def __init__(self):
        self.this_class_arr, self.this_class_tif, self.this_class_png = T.mk_class_dir(
            'Download_tif',
            this_script_root, mode=2)
        ee.Initialize()

    def run(self):
        rectangle_f = join(Expand_points_to_rectangle().this_class_arr,'grasssite_rectangle/grasssite_rectangle.shp')
        rectangle_df = gpd.read_file(rectangle_f)
        geometry_list = rectangle_df['geometry'].tolist()
        for geo in geometry_list:
            print(geo)
            ll = geo.bounds[0:2]
            ur = geo.bounds[2:4]
            region = ee.Geometry.Rectangle(ll[0],ll[1],ur[0],ur[1])
            startDate = '2020-06-01'
            endDate = '2020-6-15'
            l8 = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
            # l8 = ee.ImageCollection('LANDSAT/LC08/C01/T1_SR')
            l8 = l8.filterDate(startDate, endDate).filterBounds(region)
            # l8 = l8.median()
            # l8_qa = l8.select(['QA_PIXEL'])
            l8_optical_bands = l8.select('SR_B.').median().multiply(0.0000275).add(-0.2)
            l8_qa_band = l8.select('QA_PIXEL').select([0]).median()
            # print(l8_qa_band.getInfo())
            # exit()
            # l8_cloud_mask = self.mask_clouds(l8_optical_bands,l8_qa_band)
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

            # 导出图像集合
            # task = ee.batch.Export.image.toDrive(image=image,
            #                                      description='landsat8_images',
            #                                      # folder='landsat8_data',
            #                                      fileNamePrefix='landsat8_image',
            #                                      fileFormat='GeoTIFF',
            #                                      crs='EPSG:4326',
            #                                      driveFolder='landsat8_data',
            #                                      scale=30,
            #                                      region=region,
            #                                      )
            #
            # # 启动导出任务
            # task.start()
            pass

    def cloud_shadows(self,image):
        # QA = image.select(['pixel_qa'])
        QA = image.select(['QA_PIXEL'])
        return self.getQABits(QA, 3, 3, 'cloud_shadows').eq(0)
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
        return self.getQABits(QA, 5, 5, 'clouds').eq(0)

    def mask_clouds(self,image,image_qa):
        # image_qa,image = params
        cs = self.cloud_shadows(image_qa)
        c = self.clouds(image_qa)
        image = image.updateMask(cs).updateMask(c)
        return image

class GEE_MAP:
    def __init__(self):
        pass

    def run(self):

        pass

def main():
    Download_tif().run()
    # GEE_MAP().run()
    pass

if __name__ == '__main__':
    main()