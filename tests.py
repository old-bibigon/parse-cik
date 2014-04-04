#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cik import Session, Base, init_model, cikUIK
from sqlalchemy import create_engine
import logging

logging.basicConfig(level = logging.DEBUG)

def test_ik():
    ik = Session.query(cikUIK).filter( cikUIK.region == 'kirov', cikUIK.type_ik == 'ik' ).first()
    if not ik:
        ik = cikUIK( region = 'kirov', type_ik = 'ik', url='http://www.kirov.vybory.izbirkom.ru/ik/' )
        ik.iz_id = 7437000291808
        Session.add(ik)
    vals = ik.parse(recursion=False)
    for (k, v) in vals.items():
        print k, v
    
def test_tik():
    iz_id = 443400194087
    tik = Session.query(cikUIK).filter( cikUIK.type_ik == 'tik', cikUIK.iz_id == iz_id ).first()
    if not tik:
        tik = cikUIK( region = 'kirov', type_ik = 'tik', iz_id = iz_id )
        Session.add(tik)
    tik.url = 'http://www.kirov.vybory.izbirkom.ru/kirov/ik/443400194087'
    vals = tik.parse(recursion=False)
    for (k, v) in vals.items():
        print k, v
    

def test_uik():
    pass

if __name__ == '__main__':
    engine = create_engine('sqlite:///test_cik.sqlite')
    init_model(engine)
    
    test_ik()
    test_tik()
    test_uik()
