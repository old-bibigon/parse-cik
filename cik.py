#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

from sqlalchemy import types, Column, ForeignKey, orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
import datetime
import os
import urllib.request, urllib.error, urllib.parse
import tenacity
import re
import logging
import lxml.html
import simplejson

Session = scoped_session(sessionmaker())
Base = declarative_base()

code_of_regions = {
     '01': 'adygei',
     '02': 'altai_rep',
     '03': 'bashkortostan',
     '04': 'buriat',
     '05': 'dagestan',
     '06': 'ingush',
     '07': 'kabardin-balkar',
     '08': 'kalmyk',
     '09': 'karachaev-cherkess',
     '10': 'karel',
     '11': 'komi',
     '12': 'mari-el',
     '13': 'mordov',
     '14': 'yakut',
     '15': 'n_osset-alania',
     '16': 'tatarstan',
     '17': 'tyva',
     '18': 'udmurt',
     '19': 'khakas',
     '20': 'chechen',
     '21': 'chuvash',
     '22': 'altai_terr',
     '23': 'krasnodar',
     '24': 'krasnoyarsk',
     '25': 'primorsk',
     '26': 'stavropol',
     '27': 'khabarovsk',
     '28': 'amur',
     '29': 'arkhangelsk',
     '30': 'astrakhan',
     '31': 'belgorod',
     '32': 'bryansk',
     '33': 'vladimir',
     '34': 'volgograd',
     '35': 'vologod',
     '36': 'voronezh',
     '37': 'ivanovo',
     '38': 'irkutsk',
     '39': 'kaliningrad',
     '40': 'kaluga',
     '42': 'kemerovo',
     '43': 'kirov',
     '44': 'kostroma',
     '45': 'kurgan',
     '46': 'kursk',
     '47': 'leningrad-reg',
     '48': 'lipetsk',
     '49': 'magadan',
     '50': 'moscow_reg',
     '51': 'murmansk',
     '52': 'nnov',
     '53': 'novgorod',
     '54': 'novosibirsk',
     '55': 'omsk',
     '56': 'orenburg',
     '57': 'orel',
     '58': 'penza',
     '60': 'pskov',
     '61': 'rostov',
     '62': 'ryazan',
     '63': 'samara',
     '64': 'saratov',
     '65': 'sakhalin',
     '66': 'sverdlovsk',
     '67': 'smolensk',
     '68': 'tambov',
     '69': 'tver',
     '70': 'tomsk',
     '71': 'tula',
     '72': 'tyumen',
     '73': 'ulyanovsk',
     '74': 'chelyabinsk',
     '76': 'yaroslavl',
     '77': 'moscow_city',
     '78': 'st-petersburg',
     '79': 'jewish_aut',
     '83': 'nenetsk',
     '86': 'khantu-mansy',
     '87': 'chukot',
     '89': 'yamal-nenetsk',
     '90': 'permkrai',
     '91': 'kamchatka_krai',
     '92': 'zabkray',
     '93': 'crimea',
     '94': 'sevastopol'
}

all_regions = list(code_of_regions.values())
region2code = dict([(y,x) for (x,y) in list(code_of_regions.items())])


@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, max=60), stop=tenacity.stop_after_attempt(10))
def down_data_retry(url):
	try:
		data = urllib.request.urlopen(url, timeout=60).read()
		if data == '':
			logging.info('Empty url content %s', url)
			raise urllib.error.URLError('Empty url content %s', url)
		return data
	except:
		logging.info('Retry to download %s', url)
		raise

def down_data(url, to_file, force=False):
    if os.path.isfile(to_file) and force == False:
        data = open(to_file, 'rb').read()
    else:
        try:
            data = down_data_retry(url)
        except:
            logging.error('Not download %s', url, exc_info=True)
            return ''

        if not os.path.isdir( os.path.dirname(to_file) ):
            os.makedirs(os.path.dirname(to_file))
        open(to_file, 'wb').write(data)
    return data

class al_base():
    def set_attrs(self, attrs):
        for (k, v) in list(attrs.items()):
            #обход для несмапленных полей
            try:
                hasattr(self, k)
            except UnicodeEncodeError:
                k = k.encode('utf8')
                
            if hasattr(self, k):
                if v and isinstance(self.__table__.c[k].type, types.DateTime):
                    v = datetime.datetime.strptime( v[:v.find('.')], '%Y-%m-%dT%H:%M:%S')
                if v and isinstance(self.__table__.c[k].type, types.Boolean):
                    v = True if v == 'Y' else False
                if getattr(self, k, None) != v:
                    setattr(self, k, v)
        Session.flush()    

class cikUIK(al_base, Base):
    __tablename__ = 'cik_uik'
    id = Column( types.Integer(), primary_key=True)
    iz_id = Column( types.BigInteger(), index=True ) #id на сайте cik
    reserve_iz_id = Column( types.BigInteger(), index=True ) #id резервного состава на сайте cik

    parent_id = Column( types.Integer(), ForeignKey('cik_uik.id', ondelete="SET NULL"), index=True )
    type_ik = Column( types.String(10) )
    region = Column( types.String(50) )             #регион РФ
    url = Column( types.String(255) )               #url комиссии
    
    name = Column( types.String(255) )              #название
    address = Column( types.Text() )                #адрес комиссии
    phone = Column( types.String(100) )             #телефон комиссии
    fax = Column( types.String(100) )               #факс
    email = Column( types.String(100) )             #email
    end_date = Column( types.String(100) )          #дата окончания полномочий

    address_voteroom = Column( types.String(255) )  #адрес помещения для голосования
    phone_voteroom = Column( types.String(255) )    #телефон помещения для голосования

    lat_ik = Column( types.Float() )                   #широта ИК
    lon_ik = Column( types.Float() )                   #долгота ИК

    lat_voteroom = Column( types.Float() )             #широта помещения для голосования
    lon_voteroom = Column( types.Float() )             #долгота помещения для голосования

    children = orm.relationship("cikUIK")
    
#        self.url = 'http://www.%s.vybory.izbirkom.ru/ik/%s' % (region, iz_id or '')

    @classmethod
    def add_or_update(cls, attrs):
        if 'iz_id' not in attrs: 
            return None
        uik = Session.query( cls ).filter(cls.iz_id == attrs['iz_id']).first()
        if not uik:
            uik = cls(iz_id = attrs['iz_id'])
            Session.add(uik)
        uik.set_attrs(attrs)
        return uik

    @property
    def local_path(self):
        return os.path.join( 'orig', self.region, self.type_ik, str(self.iz_id))
    
    def normalize_attrs(self, attrs):
        norm_keys = {
            'Адрес комиссии': 'address',
            'Телефон': 'phone',
            'Факс': 'fax',
            'Адрес электронной почты': 'email',
            'Срок окончания полномочий': 'end_date',
            'Адрес помещения для голосования': 'address_voteroom',
            'Телефон помещения для голосования': 'phone_voteroom',
        }
        for (k, v) in list(norm_keys.items()):
            if k in attrs:
                attrs[v] = attrs.pop(k)
        return attrs

    def parse_data(self, data, update=True, recursion=False):
        ehtml = lxml.html.fromstring(data)
        
#        logging.debug('html: %s', lxml.html.tostring(ehtml, encoding='utf8'))
        attrs = {}
        div_main = ehtml.xpath('//div[@id="main"]/*/div[@class="center-colm"]')[0]
        try: attrs['name'] = div_main.xpath('h2')[0].text
        except: pass
        
        #аттрибуты комиссии
        for (k, v) in re.findall( '<p>\s*<strong>(.*?): </strong>(.*?)\s*</p>', 
                            lxml.html.tostring(div_main, encoding=str),
                            flags = re.M | re.S ):
            attrs[k] = re.sub('</?span.*?>', '', v)

        #координаты ИК
        try:
            coord_span = ehtml.xpath('//span[@id="view_in_map_ik"]')[0]
            attrs['lat_ik'] = float(coord_span.attrib.get('coordlat'))
            attrs['lon_ik'] = float(coord_span.attrib.get('coordlon'))
        except:
            pass

        #координаты помещения для голования
        try:
            coord_span = ehtml.xpath('//span[@id="view_in_map_voteroom"]')[0]
            attrs['lat_voteroom'] = float(coord_span.attrib.get('coordlat'))
            attrs['lon_voteroom'] = float(coord_span.attrib.get('coordlon'))
        except:
            pass
        
        attrs = self.normalize_attrs( attrs )
        if update:
            self.set_attrs(attrs)

        #члены комиссии
        people_tbl = ehtml.xpath('//div[@id="main"]/*/div[@class="center-colm"]//table')[0]
        for p in people_tbl.xpath('.//tr'):
            vals = [x.text_content().strip() for x in p.xpath('.//td') ]
            if len(vals) > 0:
                people_attrs = dict( list(zip( ('number', 'fio', 'post', 'party'), vals )) )
                people_attrs['ik_id'] = self.id
                cikPeople.add_or_update(people_attrs)

        if recursion:
            self.search_childs(data, recursion=True)
#        Session.commit()
        return attrs

    def parse(self, update=True, recursion=False):
        'парсинг комиссии'
        logging.info('parse %s (%s) from %s', self.name, self.iz_id, self.url)
        if int(self.iz_id) < -10: #не парсим фейковые (-1 -- временный id для новых ИК)
            return
        data = down_data(self.url, self.local_path + '.htm').decode('cp1251')
        try:
            self.parse_data(data, update, recursion)
        except:
            logging.error('Ошибка при загрузке %s (%s) из %s', self.name, self.region, self.url, exc_info=True)
            if os.path.isfile(self.local_path + '.htm'):
                os.rename(self.local_path + '.htm', self.local_path + '.htm.error')
    
    def search_childs(self, data, recursion=False):
        'выдергивание подчинённых комиссий'
        childs = []
        if self.type_ik == 'ik':
            url = "http://www.vybory.izbirkom.ru/region/%s?action=ikTree&region=%s" % (
                    self.region, region2code[self.region])
            txt = down_data(url, self.local_path + '_childs.js').decode('cp1251')
            vals = simplejson.loads(txt)[0]

            self.set_attrs({
                'iz_id': vals.get('id', ''),
            })
            
            for child in vals.get('children', [] ):
                childs.append({
                    'url': 'http://www.%s.vybory.izbirkom.ru/region/%s?action=ik&vrn=%s' % (self.region, self.region, child.get('id', None)),
                    'name': child.get('text', ''),
                    'iz_id': child.get('id', ''),
                    'region': self.region,
                    'parent_id': self.id,
                    'type_ik': 'tik'
               })
           
        elif self.type_ik == 'tik':
            url = "http://www.vybory.izbirkom.ru/region/%s?action=ikTree&region=%s&vrn=%s&onlyChildren=true" % (
                self.region, region2code[self.region], self.iz_id)
            txt = down_data(url, self.local_path + '_childs.js').decode('cp1251')
            vals = simplejson.loads(txt)
            
            for child in vals:
                childs.append({
                    'url': 'http://www.%s.vybory.izbirkom.ru/region/%s?action=ik&vrn=%s' % (
                        self.region, self.region, child.get('id', None)),
                    'name': child.get('text', ''),
                    'iz_id': child.get('id', ''),
                    'region': self.region,
                    'parent_id': self.id,
                    'type_ik': 'uik'
                })
        
        for child in childs:
            uik = cikUIK.add_or_update(child)
            if uik and recursion: uik.parse(recursion=True)

    def parse_reserve(self, recursion=False):
        'парсинг составов резервов комиссий'
        if self.type_ik == 'ik':
            url = 'http://www.vybory.izbirkom.ru/%s/ik_r/' % (self.region, )
        elif self.reserve_iz_id != None:
            url = 'http://www.vybory.izbirkom.ru/%s/ik_r/%s' % (self.region, self.reserve_iz_id)
        else:
            logging.error('не указан reserve_id для %s (%s)', self.name, self.id)
            return None

        data_reserv = down_data(url, self.local_path + '_reserve.htm').decode('cp1251')
        ehtml = lxml.html.fromstring(data_reserv)
        people_tbl = ehtml.xpath('//div[@id="main"]/*/div[@class="center-colm"]//table')[0]
        for p in people_tbl.xpath('.//tr'):
            vals = [x.text_content().strip() for x in p.xpath('.//td') ]
            if len(vals) > 0:
                people_attrs = dict( list(zip( ('number', 'fio', 'post', 'party'), vals )) )
                people_attrs['ik_id'] = self.id
                cikPeopleReserve.add_or_update(people_attrs)
        if recursion:
            self.parse_reserve_childs(recursion)
    
    def parse_reserve_childs(self, recursion=False):
        'обход резервных составов подчинённых комиссий'
        def generate_many_names(in_name, extra_names=[]):
            '''пытаемся придумать множество возможных названий комиссий'''
            names = [in_name, ]
            names.extend( [x.lower() for x in extra_names if x])
            
            for nname in [
                in_name.replace('икмо ', 'избирательная комиссия муниципального образования ' ),
                in_name.replace('икмо ', 'избирательная комиссия ' ),
                in_name.replace('территориальная икмо', 'тик муниципального образования'),
                re.sub( '№(\s+|0+)', '№', in_name.replace('уик ', 'участковая избирательная комиссия ' )),
            ]:
                if in_name != nname:
                    names.append(nname)
            
            new_names = []
            for name in names:
                for nname in [ re.sub('(ий|ый)\s+', 'ого ', name.replace('район', 'района')),
                               re.sub('ого\s+', 'ый ', name.replace('района', 'район')),
                               re.sub('ого\s+', 'ий ', name.replace('района', 'район')) ]:
                    if nname != name:
                        new_names.append(nname)
            names.extend(new_names)

            names.append( in_name.replace('избирательная комиссия', '').replace('муниципальный','') )

            new_names = []
            for name in names:
                for nname in [name.replace("'", '"'), name.replace('"', "'"), 
                              'тик '+name, re.sub('\s+', ' ', name)]:
                    if nname != name:
                        new_names.append(nname)

            names.extend(new_names)
            return names
        
        if self.type_ik == 'uik':
            return

        harded_names = {}
        
        if self.type_ik == 'ik':
            url_childs = 'http://www.vybory.izbirkom.ru/%s/ik_r_tree/' % (self.region, )
            childs_reserve = down_data(url_childs, self.local_path + '_childs_reserve.js').decode('cp1251')
            vals = simplejson.loads(childs_reserve)[0].get('children', [])
            harded_names = {
                'муниципальный район Заполярный район': 'ТИК Заполярного района',
                'городской округ город Нарьян-Мар': 'ТИК Нарьян-Марского городского округа',
                'Территориальная избирательная комиссия муниципального образования "Темкинский район"': 'ТИК муниципального образования "Темкинский район" Смоленской области',
                'Избирательная комиссия Можайского муниципального района': 'ТИК Можайского района',
                'город Абакан': 'ТИК г. Абакана',
            } #ТИКи которые 
            normal_ik = dict([ (x.name.lower(), x) for x in self.children]) #подчинённые ТИКи для поиска

        elif self.type_ik == 'tik':
            url_childs = 'http://www.vybory.izbirkom.ru/%s/ik_r_tree/%s' % (self.region, self.reserve_iz_id)
            childs_reserve = down_data(url_childs, self.local_path + '_childs_reserve.js').decode('cp1251')
            vals = simplejson.loads(childs_reserve)
            normal_ik = {}
            for x in self.children:
                name_x = re.sub( '\s+', ' ', re.sub( '№(\s+|0+)', '№', x.name.lower()))
                normal_ik[name_x] = x
                #пытаемся выделить номер уика и его приписать
                m = re.search(r'№\s*([0-9-]+)(,\1)?(?:\s|\n|$)', x.name.lower())
                if m:
                    name_xx = 'уик №%i' % (int(m.groups()[0].replace('-','')), )
                    if name_xx != name_x:
                        normal_ik[name_xx] = x
            
        for child in vals:
            extra_names = []
            child_url = 'http://www.vybory.izbirkom.ru/%s/ik_r/%s' % (self.region, self.reserve_iz_id)
            child_name = child.get('text', '')
            child_id = int( child.get('id', '') )
            if self.type_ik == 'tik' and child_id < 10000 :
                extra_names.append('уик №%i' % (child_id, ))
            found_ik = None
            #пытаемся сопоставить по имени дочерний ИК с уже содержащимся в базе
            for name in generate_many_names(child_name.lower(), extra_names=[ harded_names.get(child_name, None) ] + extra_names):
                if name in normal_ik:
                    found_ik = normal_ik[name]
                    logging.info('сопоставление резерва ИК - %s [%s] == %s (%s)', child_name, name, found_ik.name, found_ik.id)
                    #found_tik.parse_reserve(url=child_url)
                    found_ik.reserve_iz_id = child_id
                    break
                else:
                    logging.debug('?= тест %s', name)
            if not found_ik:
                #не нашли ИК, придумываем фейковый
                logging.warn('Не удалось найти ИК "%s" региона %s в %s (%s)', 
                            child_name, self.region, self.name, self.id)
                logging.debug('поиск среди: %s', ';'.join(sorted(normal_ik.keys())))
                new_params = {
                        'name': child_name,
                        'region': self.region,
                        'parent_id': self.id,
                        'reserve_iz_id': child_id,
                        'url': child_url,
                    }

                if self.type_ik == 'ik':
                    new_params.update({'type_ik': 'tik',
                        'iz_id': (-1) * child_id,
                    })
                elif self.type_ik == 'tik':
                    new_params.update({'type_ik': 'uik',
                        'iz_id': (-1)*((all_regions.index(self.region)+1)*100000 + child_id) if child_id < 10000 else child_id*(-1),
                    })
                found_ik = cikUIK.add_or_update(new_params)

            if found_ik and recursion: 
                found_ik.parse_reserve(recursion)
            
#        logging.info('url=%s', url)
        
class cikPeople_base(al_base):
    __tablename__ = 'cik_people'
    id = Column( types.Integer(), primary_key=True)

    number = Column( types.SmallInteger(), index=True ) #пн
    fio = Column( types.Text() ) #фио
    post = Column( types.Text() ) #должность
    party = Column( types.Text() ) #Кем рекомендован в состав комиссии

    @classmethod
    def add_or_update(cls, attrs):
        if 'ik_id' not in attrs or 'number' not in attrs: 
            return None
        people = Session.query( cls ).filter(cls.ik_id == attrs['ik_id'], cls.number == attrs['number']).first()
        if not people:
            people = cls(ik_id = attrs['ik_id'], number = attrs['number'])
            Session.add(people)
        people.set_attrs(attrs)
        return people

class cikPeople(cikPeople_base, Base):
    'Основной состав комиссии'
    __tablename__ = 'cik_people'
    ik_id = Column( types.Integer(), ForeignKey('cik_uik.id', ondelete="CASCADE"), index=True )

class cikPeopleReserve(cikPeople_base, Base):
    'Резервный состав комиссии'
    __tablename__ = 'cik_people_reserve'
    ik_id = Column( types.Integer(), ForeignKey('cik_uik.id', ondelete="CASCADE"), index=True )

def init_model(engine):
    Session.configure(bind=engine)
    Base.metadata.create_all(engine)
