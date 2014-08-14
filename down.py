#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import urllib
import os
import sys
import pickle
import datetime
import optparse
import logging

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
]

if __name__ == '__main__':
    parser = optparse.OptionParser("usage: %prog")
    parser.add_option("-r", "--region", dest="regions",
                      default='all', help=u'какой регион выкачать (через ,)')
    parser.add_option("--db", dest="path_db",
                      default='cik.sqlite', help=u'путь к дб')
    parser.add_option("-v", "--verbose", dest="debug", action="count",
                      default=False, help=u'verbose level')
    
    (options, args) = parser.parse_args()

    if options.debug == 1: logging.basicConfig(level = logging.INFO)
    elif options.debug >= 2: logging.basicConfig(level = logging.DEBUG)
    else: logging.basicConfig(level = logging.ERROR)

    from cik import Session, Base, init_model, cikUIK
    from sqlalchemy import create_engine

    engine = create_engine('sqlite:///%s' % (options.path_db, ))
    init_model(engine)

    if options.regions == 'all': 
        down_regions = all_regions
    else:
        down_regions = options.regions.split(',')

    for reg in down_regions:
        if reg not in all_regions: continue
        
        ik = Session.query(cikUIK).filter( cikUIK.region == reg, cikUIK.type_ik == 'ik' ).first()
        if not ik:
            ik = cikUIK( region = reg, type_ik = 'ik', url='http://www.%s.vybory.izbirkom.ru/ik/' % (reg, ) )
            ik.iz_id = -1
            Session.add(ik)
            
        vals = ik.parse(recursion=True)
        Session.commit()
