#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

from sqlalchemy import types, Column, ForeignKey, orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
import datetime
import os
import urllib
import re
import logging
import lxml.html
import simplejson

Session = scoped_session(sessionmaker())
Base = declarative_base()

all_regions = [ 'adygei',
    "altai_rep", "bashkortostan", "buriat", "dagestan", "ingush",
    "kabardin-balkar", "kalmyk", "karachaev-cherkess", "karel", "komi",
    "mari-el", "mordov", "yakut", "n_osset-alania", "tatarstan",
    "tyva", "udmurt", "khakas", "chechen", "chuvash",
    "altai_terr", "zabkray", "kamchatka_krai", "krasnodar", "krasnoyarsk",
    "permkrai", "primorsk", "stavropol", "khabarovsk", "amur",
    "arkhangelsk", "astrakhan", "belgorod", "bryansk", "vladimir",
    "volgograd", "vologod", "voronezh", "ivanovo", "irkutsk",
    "kaliningrad", "kaluga", "kemerovo", "kirov", "kostroma",
    "kurgan", "kursk", "leningrad-reg", "lipetsk", "magadan",
    "moscow_reg", "murmansk", "nnov", "novgorod", "novosibirsk",
    "omsk", "orenburg", "orel", "penza", "pskov",
    "rostov", "ryazan", "samara", "saratov", "sakhalin",
    "sverdlovsk", "smolensk", "tambov", "tver", "tomsk",
    "tula", "tyumen", "ulyanovsk", "chelyabinsk", "yaroslavl",
    "moscow_city", "st-petersburg", "jewish_aut", "nenetsk", "khantu-mansy",
    "chukot", "yamal-nenetsk",
    "crimea", "sevastopol",
]


def down_data(url, to_file, force=False):
    if os.path.isfile(to_file) and force == False:
        data = open(to_file).read()
    else:
        try:
            data = urllib.urlopen(url).read()
        except:
            logging.error('Not download %s', url, exc_info=True)
            return ''

        if not os.path.isdir( os.path.dirname(to_file) ):
            os.makedirs(os.path.dirname(to_file))
        open(to_file, 'wt').write(data)
    return data

class al_base():
    def set_attrs(self, attrs):
        for (k, v) in attrs.items():
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
    region = Column( types.String(50) )
    url = Column( types.String(255) )
    
    name = Column( types.String(255) )
    address = Column( types.Text() )
    phone = Column( types.String(100) )
    fax = Column( types.String(100) )
    email = Column( types.String(100) )
    end_date = Column( types.String(100) ) #дата окончания полномочий

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
            u'Адрес комиссии': 'address',
            u'Телефон': 'phone',
            u'Факс': 'fax',
            u'Адрес электронной почты': 'email',
            u'Срок окончания полномочий': 'end_date',
        }
        for (k, v) in norm_keys.items():
            if k in attrs:
                attrs[v] = attrs.pop(k)
        return attrs

    def parse(self, update=True, recursion=False):
        u'парсинг комиссии'
        logging.info('parse %s (%s)', self.name, self.iz_id)
        if self.iz_id < -10: #не парсим фейковые (-1 -- временный id для новых ИК)
            return
        data = down_data(self.url, self.local_path + '.htm').decode('cp1251')
        ehtml = lxml.html.fromstring(data)
        
        attrs = {}
        div_main = ehtml.xpath('//div[@id="main"]/*/div[@class="center-colm"]')[0]
        try: attrs['name'] = div_main.xpath('h2')[0].text
        except: pass
        
        #аттрибуты комиссии
        for (k, v) in re.findall( '<p><strong>(.*?): </strong>(.*?)</p>', lxml.html.tostring(div_main, encoding=unicode) ):
            attrs[k] = v
        
        attrs = self.normalize_attrs( attrs )
        if update:
            self.set_attrs(attrs)
        
        #члены комиссии
        people_tbl = ehtml.xpath('//div[@id="main"]/*/div[@class="center-colm"]//table')[0]
        for p in people_tbl.xpath('.//tr'):
            vals = [x.text_content().strip() for x in p.xpath('.//td') ]
            if len(vals) > 0:
                people_attrs = dict( zip( ('number', 'fio', 'post', 'party'), vals ) )
                people_attrs['ik_id'] = self.id
                cikPeople.add_or_update(people_attrs)

        if recursion:
            self.search_childs(data, recursion=True)
#        Session.commit()
        return attrs
    
    def search_childs(self, data, recursion=False):
        u'выдергивание подчинённых комиссий'
        childs = []
        if self.type_ik == 'ik':
            url = "http://www.vybory.izbirkom.ru/%s/ik_tree/" % (self.region, )
            txt = down_data(url, self.local_path + '_childs.js').decode('cp1251')
            vals = simplejson.loads(txt)[0]

            self.set_attrs({
                'iz_id': vals.get('id', ''),
            })
            
            for child in vals.get('children', [] ):
                childs.append({
                    'url': 'http://www.vybory.izbirkom.ru' + child.get('a_attr', {}).get('href', ''),
                    'name': child.get('text', ''),
                    'iz_id': child.get('id', ''),
                    'region': self.region,
                    'parent_id': self.id,
                    'type_ik': 'tik'
               })
           
        elif self.type_ik == 'tik':
            url = "http://www.vybory.izbirkom.ru/%s/ik_tree/?operation=get_children&id=%s" % (self.region, self.iz_id)
            txt = down_data(url, self.local_path + '_childs.js').decode('cp1251')
            vals = simplejson.loads(txt)
            
            for child in vals:
                childs.append({
                    'url': 'http://www.vybory.izbirkom.ru' + child.get('a_attr', {}).get('href', ''),
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
        u'парсинг составов резервов комиссий'
        if self.type_ik == 'ik':
            url = 'http://www.vybory.izbirkom.ru/%s/ik_r/' % (self.region, )
        elif self.reserve_iz_id != None:
            url = 'http://www.vybory.izbirkom.ru/%s/ik_r/%s' % (self.region, self.reserve_iz_id)
        else:
            logging.error(u'не указан reserve_id для %s (%s)', self.name, self.id)
            return None

        data_reserv = down_data(url, self.local_path + '_reserve.htm').decode('cp1251')
        ehtml = lxml.html.fromstring(data_reserv)
        people_tbl = ehtml.xpath('//div[@id="main"]/*/div[@class="center-colm"]//table')[0]
        for p in people_tbl.xpath('.//tr'):
            vals = [x.text_content().strip() for x in p.xpath('.//td') ]
            if len(vals) > 0:
                people_attrs = dict( zip( ('number', 'fio', 'post', 'party'), vals ) )
                people_attrs['ik_id'] = self.id
                cikPeopleReserve.add_or_update(people_attrs)
        if recursion:
            self.parse_reserve_childs(recursion)
    
    def parse_reserve_childs(self, recursion=False):
        u'обход резервных составов подчинённых комиссий'
        def generate_many_names(in_name, extra_names=[]):
            u'''пытаемся придумать множество возможных названий комиссий'''
            names = [in_name, ]
            names.extend( [x.lower() for x in extra_names if x])
            
            for nname in [
                in_name.replace(u'икмо ', u'избирательная комиссия муниципального образования ' ),
                in_name.replace(u'икмо ', u'избирательная комиссия ' ),
                in_name.replace(u'территориальная икмо', u'тик муниципального образования'),
                re.sub( u'№(\s+|0+)', u'№', in_name.replace(u'уик ', u'участковая избирательная комиссия ' )),
            ]:
                if in_name != nname:
                    names.append(nname)
            
            new_names = []
            for name in names:
                for nname in [ re.sub(u'(ий|ый)\s+', u'ого ', name.replace(u'район', u'района')),
                               re.sub(u'ого\s+', u'ый ', name.replace(u'района', u'район')),
                               re.sub(u'ого\s+', u'ий ', name.replace(u'района', u'район')) ]:
                    if nname != name:
                        new_names.append(nname)
            names.extend(new_names)

            names.append( in_name.replace(u'избирательная комиссия', '').replace(u'муниципальный','') )

            new_names = []
            for name in names:
                for nname in [name.replace("'", '"'), name.replace('"', "'"), 
                              u'тик '+name, re.sub('\s+', ' ', name)]:
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
                u'муниципальный район Заполярный район': u'ТИК Заполярного района',
                u'городской округ город Нарьян-Мар': u'ТИК Нарьян-Марского городского округа',
                u'Территориальная избирательная комиссия муниципального образования "Темкинский район"': u'ТИК муниципального образования "Темкинский район" Смоленской области',
                u'Избирательная комиссия Можайского муниципального района': u'ТИК Можайского района',
                u'город Абакан': u'ТИК г. Абакана',
            } #ТИКи которые 
            normal_ik = dict([ (x.name.lower(), x) for x in self.children]) #подчинённые ТИКи для поиска

        elif self.type_ik == 'tik':
            url_childs = 'http://www.vybory.izbirkom.ru/%s/ik_r_tree/%s' % (self.region, self.reserve_iz_id)
            childs_reserve = down_data(url_childs, self.local_path + '_childs_reserve.js').decode('cp1251')
            vals = simplejson.loads(childs_reserve)
            normal_ik = {}
            for x in self.children:
                name_x = re.sub( '\s+', ' ', re.sub( u'№(\s+|0+)', u'№', x.name.lower()))
                normal_ik[name_x] = x
                #пытаемся выделить номер уика и его приписать
                m = re.search(ur'№\s*([0-9-]+)(,\1)?(?:\s|\n|$)', x.name.lower())
                if m:
                    name_xx = u'уик №%i' % (int(m.groups()[0].replace('-','')), )
                    if name_xx != name_x:
                        normal_ik[name_xx] = x
            
        for child in vals:
            extra_names = []
            child_url = 'http://www.vybory.izbirkom.ru' + child.get('a_attr', {}).get('href', '')
            child_name = child.get('text', '')
            child_id = int( child.get('id', '') )
            if self.type_ik == 'tik' and child_id < 10000 :
                extra_names.append(u'уик №%i' % (child_id, ))
            found_ik = None
            #пытаемся сопоставить по имени дочерний ИК с уже содержащимся в базе
            for name in generate_many_names(child_name.lower(), extra_names=[ harded_names.get(child_name, None) ] + extra_names):
                if name in normal_ik:
                    found_ik = normal_ik[name]
                    logging.info(u'сопоставление резерва ИК - %s [%s] == %s (%s)', child_name, name, found_ik.name, found_ik.id)
                    #found_tik.parse_reserve(url=child_url)
                    found_ik.reserve_iz_id = child_id
                    break
                else:
                    logging.debug(u'?= тест %s', name)
            if not found_ik:
                #не нашли ИК, придумываем фейковый
                logging.warn(u'Не удалось найти ИК "%s" региона %s в %s (%s)', 
                            child_name, self.region, self.name, self.id)
                logging.debug(u'поиск среди: %s', ';'.join(sorted(normal_ik.keys())))
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
    u'Основной состав комиссии'
    __tablename__ = 'cik_people'
    ik_id = Column( types.Integer(), ForeignKey('cik_uik.id', ondelete="CASCADE"), index=True )

class cikPeopleReserve(cikPeople_base, Base):
    u'Резервный состав комиссии'
    __tablename__ = 'cik_people_reserve'
    ik_id = Column( types.Integer(), ForeignKey('cik_uik.id', ondelete="CASCADE"), index=True )

def init_model(engine):
    Session.configure(bind=engine)
    Base.metadata.create_all(engine)
