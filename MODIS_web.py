# coding=utf-8
from sklearn.metrics.pairwise import linear_kernel

from __init__ import *
import os
import re
import urllib3
from bs4 import BeautifulSoup
import requests
# exit()
this_script_root = join(this_root, 'MODIS_download')

class ModisDownload:

    def __init__(self):
        self.__conf__()
        self.product = self.product_url.split('/')[-2]
        self.download_dir = join(self.download_path,'HDF',self.product)
        self.urls_dir = join(self.download_path,'urls',self.product)

        T.mk_dir(self.download_dir, force=True)
        T.mk_dir(self.urls_dir, force=True)
        pass

    def __conf__(self):
        '''
        tips: disable IPV6
        configuration
        '''
        ####### 下载路径 #######
        self.download_path = this_script_root

        ####### 下载区域 #######
        # self.region_shp = r'F:\shp\china.shp'

        ####### 要下载的产品地址 #######
        self.product_url = 'https://e4ftl01.cr.usgs.gov/MOLT/MOD17A2HGF.061/'  # GPP

        ####### 开始和结束的日期 #######
        self.date_start = [2000, 1]  # 包含
        self.date_end = [2025, 5]  # 不包含

        ####### 并行下载线程数 #######
        self.thread = 10
        self.thread_url = 10
        # 并行数量太大容易被服务器拒绝

        ####### 账号密码 #######
        self.username = 'leeyang1991@gmail.com'  # Update this line
        self.password = 'asdfasdf911007'  # Update this line
        pass

    def run(self):
        # # 1. 获取要下载的hdf的url
        # self.get_hdf_urls()
        # # 2. 下载
        self.download_hdf()


    def lon_lat_to_Sinusoidal_tiles(self,lon,lat):
        '''
        Code from https://www.earthdatascience.org/tutorials/convert-modis-tile-to-lat-lon/
        modis_tiles.txt originates from https://modis-land.gsfc.nasa.gov/pdf/sn_bound_10deg.txt
        :param lon: longitude
        :param lat: latitude
        :return: tiles: v and h
        '''
        data = np.genfromtxt('modis_tiles.txt',
                             skip_header=7,
                             skip_footer=3)

        vh = []
        for i in data:
            if lat >= i[4] and lat <= i[5] and lon >= i[2] and lon <= i[3]:
                vert = i[0]
                horiz = i[1]
                vh.append([vert,horiz])
        # print vh

        return vh


    def get_product_dates(self):
        '''
        :param product_url:'https://e4ftl01.cr.usgs.gov/MOLT/MOD17A2H.006/
        :return:
        '''
        print( 'fetching dates...')
        product_url = self.product_url
        # print(product_url)
        request = urllib3.request('GET',product_url)
        content = request.data
        # content = str(content)
        # print(content);exit()
        # response = urllib2.urlopen(request)
        # body = response.read()
        # <a href="2000.01.01/">2000.01.01/</a>
        soup = BeautifulSoup(content,features="html.parser")
        links = soup.find_all('a')
        date_list = []
        for link in links:
            href = link.get('href')
            if not href.endswith('/'):
                continue
            if href.startswith('/'):
                continue
            if href.startswith('http'):
                continue
            date = href.replace('/','')
            date_list.append(date)

        return date_list


    def pick_date(self,start,end):
        '''
        :param start: [yyyy,mm] 含
        :param end: [yyyy,mm] 不含
        :return:
        '''
        s_year,s_mon = start
        e_year,e_mon = end
        avail_date = self.get_product_dates()
        init_date = datetime.datetime(s_year,s_mon,1)
        end_date = datetime.datetime(e_year,e_mon,1)
        delta_day = end_date-init_date
        delta_day = delta_day.days
        date_list = []
        for d in range(delta_day):
            date_delta = datetime.timedelta(d)
            date = init_date+date_delta
            year,mon,day = date.year,date.month,date.day
            date_str = '{:d}.{:02d}.{:02d}'.format(year,mon,day)
            date_list.append(date_str)

        picked_date = []
        for d in date_list:
            if d in avail_date:
                picked_date.append(d)

        return picked_date

    def kernel_get_hdf_urls(self,params):
        dates,ii,date_urls = params
        url_text_file = join(self.urls_dir,dates[ii]+'.txt')
        if os.path.isfile(url_text_file):
            return None
        request = urllib3.request('GET',date_urls[ii])
        content = request.data
        soup = BeautifulSoup(content,'html.parser')
        fw = open(url_text_file, 'w')
        for link in soup.find_all('a'):
            href = link.get('href')
            if not href.endswith('.hdf'):
                continue

            download_url = date_urls[ii] + href
            fw.write(download_url + '\n')
        fw.close()
        # exit()

        # pass


    def get_hdf_urls(self):
        '''
        :return:
        '''
        start = self.date_start
        end = self.date_end
        picked_date = self.pick_date(start,end)

        date_urls = []
        dates = []
        for date in picked_date:
            url = self.product_url+date+'/'
            date_urls.append(url)
            dates.append(date)
        # print(dates)
        # exit()

        params = []
        for ii in range(len(dates)):
            params.append([dates,ii,date_urls])
            # self.kernel_get_hdf_urls([dates,ii,date_urls])

        MULTIPROCESS(self.kernel_get_hdf_urls,params).run(process=self.thread_url,process_or_thread='t',desc='fetching urls...')
        pass


    def kernel_download_hdf(self,url):
        '''
        :param urls: url 'https://e4ftl01.cr.usgs.gov/MOLT/MOD17A2H.006/2000.05.16/MOD17A2H.A2000137.h21v09.006.2015139130317.hdf'
        '''
        outdir = self.download_dir
        # print(outdir)
        date = url.split('/')[-2]
        tile = url.split('/')[-1].split('.')[-4]
        # print(date,tile)
        outf = join(outdir,date,tile+'.hdf')
        if isfile(outf):
            return
        # exit()
        # outf = join(outdir,'test.hdf')
        fw = open(outf,'wb')
        session = requests.Session()
        # print(url)
        session.auth = requests.auth.HTTPBasicAuth(self.username,self.password)
        r = session.get(url,stream=True)
        for chunk in r.iter_content(chunk_size=8192):
            fw.write(chunk)
        fw.close()


    pass

    def download_hdf(self):
        url_dir = self.urls_dir

        # tiles = self.shp_to_Sinusoidal_tiles()
        # tiles_str = []
        # for t in tiles:
        #     v, h = t
        #     ts = '.h{:02d}v{:02d}.'.format(h,v)
        #     tiles_str.append(ts)

        selected_urls = []
        for f in os.listdir(url_dir):
            # fr = open(url_dir+f,'r')
            fr = open(join(url_dir,f),'r')
            # print(fr)
            lines = fr.readlines()
            fr.close()
            for line in lines:
                line = line.split('\n')[0]
                save_path = join(self.download_dir,line.split('/')[-2])
                T.mk_dir(save_path, force=1)
                selected_urls.append(line)
            # print(selected_urls)
            # exit()

        #### debug ####
        # for i in selected_urls:
            # print i
            # self.kernel_download_hdf(i)
        #### debug ####

        MULTIPROCESS(self.kernel_download_hdf,selected_urls).run(process=self.thread,process_or_thread='t',desc='downloading..')

        pass




def main():
    ModisDownload().run()
    pass


if __name__ == '__main__':
    main()



