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
    iz_id = Column( types.BigInteger(), index=True )

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
        logging.info('parse %s', self.name)
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
        Session.commit()
        return attrs
    
    
    def search_childs(self, data, recursion=False):
        childs = []
        if self.type_ik == 'ik':
            txt = re.findall(r'"data" : (\[.*\])', data)[0]
            vals = simplejson.loads(txt)[0]
            
            self.set_attrs({
                'iz_id': vals.get('attr', {}).get('id', ''),
            })
            print vals
            
            for child in vals.get('children', [] ):
                childs.append({
                    'url': 'http://www.vybory.izbirkom.ru' + child.get('data', {}).get('attr', {}).get('href', ''),
                    'name': child.get('data', {}).get('title', ''),
                    'iz_id': child.get('attr', {}).get('id', ''),
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
                    'url': 'http://www.vybory.izbirkom.ru' + child.get('data', {}).get('attr', {}).get('href', ''),
                    'name': child.get('data', {}).get('title', ''),
                    'iz_id': child.get('attr', {}).get('id', ''),
                    'region': self.region,
                    'parent_id': self.id,
                    'type_ik': 'uik'
                })
        
        for child in childs:
            uik = cikUIK.add_or_update(child)
            if uik and recursion: uik.parse(recursion=True)
        logging.debug(childs)
        
class cikPeople(al_base, Base):
    __tablename__ = 'cik_people'
    id = Column( types.Integer(), primary_key=True)
    ik_id = Column( types.Integer(), index=True )

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


def init_model(engine):
    Session.configure(bind=engine)
    Base.metadata.create_all(engine)
