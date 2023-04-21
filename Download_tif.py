# coding=utf-8
from __init__ import *
import ee
from Preprocess import *
import math
import geemap
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
            endDate = '2020-7-31'
            l8 = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
            l8 = l8.filterDate(startDate, endDate).filterBounds(region)
            l8_optical_bands = l8.select('SR_B.').median().multiply(0.0000275).add(-0.2)
            cloudScore = ee.Algorithms.Landsat.simpleCloudScore(l8.select(['QA_PIXEL']))
            # exit()
            print(print('Cloud score layer:', cloudScore.getInfo()))
            exit()
            # masked = l8_optical_bands.updateMask(scored.Not())
            # print(cloudy_scene)
            # exit()
            # cloudy_scene = cloudy_scene.rename('cloudy_scene')
            # l8_optical_bands_mask_cloud = l8_optical_bands
            # l8_optical_bands = l8_optical_bands.multiply(0.0000275).add(-0.2)
            # l8 = l8.map(lambda image: ee.Algorithms.Landsat.simpleCloudScore(image).select(['ST_CDIST']).lt(20).multiply(10000).toInt16().
            #             addBands(ee.Algorithms.Landsat.simpleCloudScore(image).select(['ST_CDIST']).lt(20).Not().rename('mask')).float(). \
            #             addBands(image.select(['SR_B1']).multiply(0.0001).multiply(math.pi).divide(ee.Algorithms.Landsat.calibratedRadiance(image)).multiply(10000).toInt16()) \
            #             .updateMask(image.select(['SR_B1']).multiply(0.0001).multiply(math.pi).divide(ee.Algorithms.Landsat.calibratedRadiance(image)).reduce(ee.Reducer.min()).gt(0.0))
            #             )
            exportOptions = {
                'scale': 30,
                'maxPixels': 1e13,
                'region': region
            }
            # image = ee.Image(l8.first())
            # band_image = l8_optical_bands.select('SR_B3')
            # bands = band_image.bandNames().getInfo()
            #
            # # 打印波段列表
            # print('Band names:', bands)
            # exit()
            # url = l8_optical_bands.getDownloadURL(exportOptions)
            url = masked.getDownloadURL(exportOptions)
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