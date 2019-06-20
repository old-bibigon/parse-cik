parse-cik
=========

Скрипт для скачки данных о составе избирательных комиссий с сайта ЦИКа
=======

Зависимости: lxml, simplejson, sqlalchemy, sqlite

Использование: `python down.py`

## Опции командной строки

<pre>
  -r, --region          список регионов для скачивания. Можно указывать несколько через запятую.
                        По умолчанию скачиваются все регионы
  --db                  путь к базе данных. По умолчанию cik.sqlite
  --with-reserve        скачивать резервы УИК. По умолчанию отключено
  -v, --verbose         печатать отладочную информацию
</pre>

Список кодов регионов: adygei, altai_rep, bashkortostan, buriat, dagestan, ingush, kabardin-balkar, kalmyk, karachaev-cherkess, karel, komi, mari-el, mordov, yakut, n_osset-alania, tatarstan, tyva, udmurt, khakas, chechen, chuvash, altai_terr, krasnodar, krasnoyarsk, primorsk, stavropol, khabarovsk, amur, arkhangelsk, astrakhan, belgorod, bryansk, vladimir, volgograd, vologod, voronezh, ivanovo, irkutsk, kaliningrad, kaluga, kemerovo, kirov, kostroma, kurgan, kursk, leningrad-reg, lipetsk, magadan, moscow_reg, murmansk, nnov, novgorod, novosibirsk, omsk, orenburg, orel, penza, pskov, rostov, ryazan, samara, saratov, sakhalin, sverdlovsk, smolensk, tambov, tver, tomsk, tula, tyumen, ulyanovsk, chelyabinsk, yaroslavl, moscow_city, st-petersburg, jewish_aut, nenetsk, khantu-mansy, chukot, yamal-nenetsk, permkrai, kamchatka_krai, zabkray, crimea, sevastopol

[Описание данных](http://gis-lab.info/qa/cik-data.html)
