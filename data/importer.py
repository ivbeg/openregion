# -*- coding: utf8 -*-

from sets import Set
from decimal import Decimal
import sys, os, csv
from pymongo import Connection, ASCENDING


REGION_TRANS_SCHEMA = [{'name': 'borderRegions', 'type' : 'array'}, 
{'name' : 'taxCode', 'type' : 'array'}, 
{'name' : 'automobileCodes', 'type' : 'array'}, 
{'name' : 'borderCountries', 'type' : 'array'}, 
{'name' : 'oceanBasin', 'type' : 'array'}, 
{'name' : 'closeSeas', 'type' : 'array'}]

FEDDIST_TRANS_SCHEMA = [{'name' : 'bordersByAbbr', 'type' : 'array'}, ]
MILDIST_TRANS_SCHEMA = [{'name' : 'bordersByAbbr', 'type' : 'array'}, ]
EMPTY_SCHEMA = []

FEDROADS_TRANS_SCHEMA = [{'name' : 'asiaRoadsRelated', 'type' : 'array'}, 
{'name' : 'euRoadsRelated', 'type' : 'array'}, 
{'name' : 'russRoads', 'type' : 'array'}, 
{'name' : 'roadSurface', 'type' : 'array'}, 
{'name' : 'regions', 'type' : 'array'}, 
]
BVU_TRANS_SCHEMA = [{'name' : 'regions', 'type' : 'array'}, ]
RAILREGIONS_TRANS_SCHEMA = [{'name' : 'bordersRail', 'type' : 'array'}, {'name' : 'regions', 'type' : 'array'},]
GROUPTYPES_TRANS_SCHEMA = []
GROUPS_TRANS_SCHEMA = [{'name' : 'regions', 'type' : 'array'}, ]
PIPELINES_TRANS_SCHEMA = [{'name' : 'regions', 'type' : 'array'}, ]
ORGS_TRANS_SCHEMA = [{'name' : 'regions', 'type' : 'array'}, ]
ARBD_TRANS_SCHEMA = [{'name' : 'regions', 'type' : 'array'}, ]

DB_NAME = 'admmeta'
DB_LINKED = 'sources'

def __create_list(l):
	return {'list' : l, 'num' : len(l)}

def import_file(filename, schema, collname, addIndexes=[]):
	i = 0
	c = Connection()
	db = c[DB_NAME]
	db.drop_collection(collname)
	coll = db[collname]
	for k in addIndexes:
		coll.ensure_index(k, ASCENDING, unique=False)
	csvr = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
	for r in csvr:		
		i += 1
		for k in r.keys():
			r[k] = r[k].decode('cp1251')
		for k in schema:
			if k['type'] == 'array':
				name = r[k['name']].strip()
				arr = name.split(',') if len(name) > 0 else []
				r[k['name']] = __create_list(arr)
		coll.save(r)
		if i % 20 == 0:
			print collname, 'imported', i
	print collname, 'total imported', i


def merge_file(filename, schema, collname, addIndexes=[], mergekey="subjectCode"):
	i = 0
	c = Connection()
	db = c[DB_NAME]
	coll = db[collname]
	csvr = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
	for r in csvr:		
		i += 1
		for k in r.keys():
			r[k] = r[k].decode('cp1251')
		for k in schema:
			if k['type'] == 'array':
				name = r[k['name']].strip()
				arr = name.split(',') if len(name) > 0 else []
				r[k['name']] = __create_list(arr)
		o = coll.find_one({mergekey : r[mergekey]})
		if o:
			o.update(r)
		else:
			print r
		coll.save(o)
		if i % 20 == 0:
			print collname, 'imported', i
	print collname, 'total imported', i



def update_grouptypes():
	c = Connection()
	db = c[DB_NAME]
	rcoll = db['regions']
	gcoll = db['groups']
	keymap = {'fedcity' : u'город федерального значения', 'republics' : u'республика', 'oblast' : u'область', 'autonomarea' : u'автономный округ', 'autonomoblast' : u'автономная область', 'krai' : u'край'}
	for k, v in keymap.items():
		keys = []
		all = rcoll.find({'regType': v})
		for o in all:
			keys.append(o['subjectCode'])
		group = gcoll.find_one({'key' : k})
		group['regions'] = __create_list(keys)
		gcoll.save(group)
		



def update_grouptype(tablename=u'feddist', gtype='feddist', schema={'key' : 'key', 'regions' : 'regions', 'name' : 'name', 'websiteUrl' : 'websiteUrl'}):
	c = Connection()
	db = c[DB_NAME]
	rcoll = db[tablename]
	gcoll = db['groups']	
	for o in rcoll.find():
		g = gcoll.find_one({'key' : schema['key']})
		if g is None:
			g = {}
		for k, v in schema.items():
			g[k] = o[v] 
		g['grouptype'] = gtype
		gcoll.save(g)
	print 'Table', tablename, 'processed'


def	update_feddistricts():
	c = Connection()
	db = c[DB_NAME]
	rcoll = db['regions']
	fcoll = db['feddistricts']			
	for f in fcoll.find():
		keys = []
		all = rcoll.find({'federalDistrict': f['key']})
		for o in all:
			keys.append(o['subjectCode'])
		f['regions'] = __create_list(keys)
		fcoll.save(f)
		

def	update_mildistricts():
	c = Connection()
	db = c[DB_NAME]
	rcoll = db['regions']
	fcoll = db['mildistricts']			
	for f in fcoll.find():
		keys = []
		all = rcoll.find({'militaryDistrict': f['key']})
		for o in all:
			keys.append(o['subjectCode'])
		f['regions'] = __create_list(keys)
		fcoll.save(f)

def	update_ecoregions():
	c = Connection()
	db = c[DB_NAME]
	rcoll = db['regions']
	fcoll = db['ecoarea']			
	for f in fcoll.find():
		keys = []
		all = rcoll.find({'economicRegion': f['key']})
		for o in all:
			keys.append(o['subjectCode'])
		f['regions'] = __create_list(keys)
		fcoll.save(f)


def find_save_notransport(name, key, tablename='railregions'):
	c = Connection()
	db = c[DB_NAME]
	rcoll = db['regions']
	fcoll = db[tablename]			
	gcoll = db['groups']			

	keys = []
	for o in rcoll.find():
		keys.append(o['subjectCode'])
	nset = Set(keys)
	aset = Set()
	for o in fcoll.find():
		aset = aset.union(Set(o['regions']['list']))
	d = nset.difference(aset)
#	print dir(d)
	g = gcoll.find_one({'key' : key})
	if not g:
		g = {}
	g['name'] = name
	g['key'] = key
	g['grouptype'] = 'notransportregs'
	g['regions'] = __create_list(list(d))
	g['websiteUrl'] = ""
	gcoll.save(g)


def generate_borders():
	c = Connection()
	db = c[DB_NAME]
	rcoll = db['regions']
	db.drop_collection('borders')
	bcoll = db['borders']
	bcoll.ensure_index('key', 1)
	for r in rcoll.find():
		for b in r['borderRegions']['list']:
			regcodes = [r['subjectCode'], b]		
			regcodes.sort()
			key = 'borderR%sR%s' %(regcodes[0], regcodes[1])
			border = {'key': key, 'regcodes' : regcodes, 'borderadmtype': 'r2r', 'bordergeotype' : 'land'}
			o = bcoll.find_one({'key' : key})
			if o is None:
				bcoll.save(border)
		for b in r['borderCountries']['list']:
			key = 'borderR%sC%s' %(regcodes[0], b)
			border = {'key': key, 'reg_key' : r['subjectCode'], 'country_key' : b, 'borderadmtype': 'r2c', 'bordergeotype' : 'land'}
			o = bcoll.find_one({'key' : key})
			if o is None:
				bcoll.save(border)


def find_regions_rels():
	c = Connection()
	db = c[DB_NAME]
	rcoll = db['regions']			
	gcoll = db['groups']
	db.drop_collection('regrelations')
	relcoll = db['regrelations']			
	all = []
	for o in rcoll.find():
		all.append(o)

	for left in all:
		for right in all:
			relation = None
			if left['subjectCode'] == right['subjectCode']: continue
			if right['subjectCode'] in left['borderRegions']['list']:				
				relation = {'rreg_key' : right['subjectCode'], 'rreg_name' : right['nameRU'], 'rtype': 'bordering'}
			if relation is not None:
				relation['lreg_key'] = left['subjectCode']
				relation['lreg_name'] = left['nameRU']
				relcoll.save(relation)

	
	for left in all:			
		groups = gcoll.find()
		for g in groups:
			if left['subjectCode'] in g['regions']['list']:
				relation = {'lreg_key' : left['subjectCode'], 'lreg_name' : left['nameRU'], 'rtype': 'memberof', 'rgroup_key' : g['key'], 'rgroup_name' : g['name']}
				relation['rgroup_type'] = g['grouptype']
				relcoll.save(relation)
			



def find_groups_rels():
	c = Connection()
	db = c[DB_NAME]
	gcoll = db['groups']			
	bcoll = db['borders']
	db.drop_collection('grouprelations')
	rcoll = db['grouprelations']			
	all = []
	for o in gcoll.find({'regions.num' : {'$ne' : 0}}):
		all.append(o)

	for left in all:
		rellist = []
		for right in all:
			relation = None
			if left['key'] == right['key']: continue
			ls = Set(left['regions']['list'])
			rs = Set(right['regions']['list'])
			if len(ls) == len(rs):
				diff = ls.difference(rs)
				if len(diff) == 0:
					print "DIFF"
					relation = {'group_key' : right['key'], 'group_name' : right['name'], 'rtype': 'same'}
				else:
					iss = ls.intersection(rs)
					if len(iss) > 0:
						relation = {'group_key' : right['key'], 'group_name' : right['name'], 'rtype': 'intersect', 'regions' : __create_list(list(iss))}
			else:
				if ls.issubset(rs):
					relation = {'group_key' : right['key'], 'group_name' : right['name'], 'rtype': 'subset'}
				elif ls.issuperset(rs):
					relation = {'group_key' : right['key'], 'group_name' : right['name'], 'rtype': 'superset'}
				else:
					diff = ls.intersection(rs)
					if len(diff) != 0:
						relation = {'group_key' : right['key'], 'group_name' : right['name'], 'rtype': 'intersect', 'regions' : __create_list(list(diff))}
			borders = []
			if relation is None:
				for l1 in ls:
					for l2 in rs:
						regcodes = [l1, l2]
						regcodes.sort()
						key = 'borderR%sR%s' %(regcodes[0], regcodes[1])
						brd = bcoll.find_one({'key' : key, 'borderadmtype' : 'r2r'})
						if brd is not None:
							borders.append(brd)
				if len(borders) > 0:
					relation = {'group_key' : right['key'], 'group_name' : right['name'], 'rtype': 'bordering', 'borders' : __create_list(list(borders))}
			if relation is not None:
				relation['bgroup_key'] = left['key']
				relation['bgroup_name'] = left['name']
				relation['group_type'] = right['grouptype']
				relation['bgroup_type'] = left['grouptype']
				relation['is_grouptype_same'] = (right['grouptype'] == left['grouptype'])
				rcoll.save(relation)
#				rellist.append(relation)		
				print left['key'], relation['rtype'], right['key']
#				print left['key'], 'relation', relation
#		left['relations'] = __create_list(rellist)
#		gcoll.save(left)



def update_groups_by_regions():
	c = Connection()
	db = c[DB_NAME]
	gcoll = db['groups']			
	rcoll = db['regions']
	all = []
	for o in gcoll.find({'regions.num' : {'$ne' : 0}}):
		all.append(o)
	for o in all:
		found = False
		isLandlocked = True
		area = 0
		population = 0
		for r in o['regions']['list']:
			reg = rcoll.find_one({'subjectCode' : r})
#			print o['name'].encode('cp866', 'replace'), r
			if reg['isLandlocked'] != 'yes':
				isLandlocked = False
			area += reg['area']
			population += reg['population']
		o['isLandlocked'] = isLandlocked
		o['area'] = area
		o['population'] = population
		o['density'] = float(population) / area
		gcoll.save(o)
			


def update_regions_by_governmentdata():
	c = Connection()
	db = c[DB_NAME]
	rcoll = db['regions']
	filename = 'government/governmentregions.csv'
	csvr = csv.DictReader(open(filename, 'r'), delimiter='\t', quoting=csv.QUOTE_NONE)
	i = 0
	for r in csvr:		
		i += 1
		for k in r.keys():
			r[k] = r[k].decode('cp1251')						
		r['url'] = r['url'].replace('government.ru', 'www.government.ru')
		reg = rcoll.find_one({'governmentRuLink' : r['url']})
		if reg is not None:
			reg['area'] = int(Decimal(r['area'])*1000)
			reg['population'] = int(r['population'])
			reg['admCenter'] = r['capital']
		rcoll.save(reg)
	pass

def update_regions_by_dbpedia():
	c = Connection()
	db = c[DB_NAME]
	dbp = c[DB_LINKED]
	dbpcoll = dbp['dbpedia']
	rcoll = db['regions']
	all = []
	for r in rcoll.find():
		all.append(r)
	for r in all:
		dbpedia_name = r['dbPedia'].rsplit('/', 1)[1]
		dbpedia_res = 'http://dbpedia.org/resource/%s' %(dbpedia_name)
		pop = dbpcoll.find_one({'source' : dbpedia_res, 'key' : 'http://dbpedia.org/property/population'})
		if not pop:
			pop = dbpcoll.find_one({'source' : dbpedia_res, 'key' : 'http://dbpedia.org/property/popCensus'})
		if not pop:
			print r['nameEN']
		else:
			r['population'] = int(pop['value'].replace(' ',''))
		area = dbpcoll.find_one({'source' : dbpedia_res, 'key' : 'http://dbpedia.org/property/area'})
		if not area:
			area = dbpcoll.find_one({'source' : dbpedia_res, 'key' : 'http://dbpedia.org/property/areaKm'})
		if not area:
			print r['nameEN']
		else:
			r['area'] = int(area['value'].replace(' ',''))
		if r.has_key('area') and r.has_key('population'):
			r['density'] = float(r['population']) / r['area']
		rcoll.save(r)
		
		

		
def update_groups():
	update_regions_by_governmentdata()
	update_regions_by_dbpedia()
	update_groups_by_regions()


def generate_compare_values():
	c = Connection()
	db = c[DB_NAME]
	rcoll = db['regions']
	gcoll = db['groups']
	db.drop_collection('comparevals')
	bcoll = db['comparevals']
	all = []
	rkeys = ['area', 'density', 'population']
	gkeys = ['area', 'density', 'population']
	for r in rcoll.find():
		all.append(r)
	i = 0
	for o in all:
		i += 1	
		if i % 10 == 0: print i
		for r in all:
			for k in rkeys:
				c = {'ctype' : 'regcompare'}
				if o['subjectCode'] == r['subjectCode']: continue
				c['left'] = o['subjectCode']
				c['right'] = r['subjectCode']
				c['key'] = k
				c['absdiff'] = o[k] - r[k]
				c['divdiff'] = float(o[k]) / r[k]
				bcoll.save(c)
	all = []
	for r in gcoll.find({'area' : {'$exists': True}}):
		all.append(r)
	i = 0
	for o in all:
		i += 1
		if i % 10 == 0: print i
		for r in all:
			for k in gkeys:
				c = {'ctype' : 'groupcompare'}
				if o['key'] == r['key']: continue
				c['left'] = o['key']
				c['right'] = r['key']
				c['key'] = k
				c['absdiff'] = o[k] - r[k]
				c['divdiff'] = float(o[k]) / r[k]
				bcoll.save(c)
			c = {'ctype' : 'groupcompare'}
			if o['key'] == r['key']: continue
			c['left'] = o['key']
			c['right'] = r['key']
			c['key'] = 'similar'
			c['absdiff'] = None
			area_diff = float(o['area']) / r['area']
			pop_diff = float(o['population'])  / r['population']
			if area_diff != 0 and pop_diff != 0:
				c['divdiff'] = abs(area_diff + pop_diff) / 2
			else:
				c['divdiff'] = 0
			bcoll.save(c)
			


def build_attr_rating(ratingkey='area', attrname='area'):
	c = Connection()
	db = c[DB_NAME]
	rcoll = db['regions']	
	gtypecoll = db['grouptypes']	
	gcoll = db['groups']

	valuecoll = db['ratingvalues']
	valuecoll.remove({'key' : ratingkey})

	# Generates ratings by attribute for each region
	all = rcoll.find().sort(attrname, -1)
	position = 1
	last_val = None
	item = {'key' : ratingkey, 'ratingtype' : 'allregions'}
	for o in all:
		if last_val is None:
			last_val = o[attrname]
			item['pos'] = position
			item['members'] = {'num' : 1, 'list' : [{'key' : o['subjectCode'], 'name' : o['nameRU']}, ]}
			item['value'] = o[attrname]
		elif last_val == o[attrname]:
			item['members']['num'] += 1
			item['members']['list'].append({'key' : o['subjectCode'], 'name' : o['nameRU']})
		else:
			valuecoll.save(item)
			position += 1
			item = {'key' : ratingkey, 'ratingtype' : 'allregions'}
			item['pos'] = position
			item['members'] = {'num' : 1, 'list' : [{'key' : o['subjectCode'], 'name' : o['nameRU']}, ]}
			item['value'] = o[attrname]
	valuecoll.save(item)

	# Generates ratings by group type
	gtypes = gtypecoll.find()
	for gtype in gtypes:
		gtypekey = gtype['key']
		all = gcoll.find({'grouptype' : gtypekey}).sort(attrname, -1)
		position = 1
		last_val = None
		item = {'key' : ratingkey, 'ratingtype' : 'bygrouptype', 'gtype_key' : gtypekey}
		for o in all:
			if not o.has_key(attrname): continue
			if last_val is None:
				last_val = o[attrname]
				item['pos'] = position
				item['members'] = {'num' : 1, 'list' : [{'key' : o['key'], 'name' : o['name']}, ]}
				item['value'] = o[attrname]
			elif last_val == o[attrname]:
				item['members']['num'] += 1
				item['members']['list'].append({'key' : o['key'], 'name' : o['name']})
			else:
				valuecoll.save(item)
				position += 1
				item = {'key' : ratingkey, 'ratingtype' : 'bygrouptype', 'gtype_key' : gtypekey}
				item['pos'] = position
				item['members'] = {'num' : 1, 'list' : [{'key' : o['key'], 'name' : o['name']}, ]}
				item['value'] = o[attrname]
		valuecoll.save(item)

					
	# Generates ratings by group 
	groups = gcoll.find()
	for g in groups:
		gkey = g['key']
		all = rcoll.find({'subjectCode': {'$in' : g['regions']['list']}}).sort(attrname, -1)
		position = 1
		last_val = None
		item = {'key' : ratingkey, 'ratingtype' : 'bygroup', 'group_key' : gkey}
		for o in all:
			if not o.has_key(attrname): continue
			if last_val is None:
				last_val = o[attrname]
				item['pos'] = position
				item['members'] = {'num' : 1, 'list' : [{'key' : o['subjectCode'], 'name' : o['nameRU']}, ]}
				item['value'] = o[attrname]
			elif last_val == o[attrname]:
				item['members']['num'] += 1
				item['members']['list'].append({'key' : o['subjectCode'], 'name' : o['nameRU']})
			else:
				valuecoll.save(item)
				position += 1
				item = {'key' : ratingkey, 'ratingtype' : 'bygroup', 'group_key' : gkey}
				item['pos'] = position
				item['members'] = {'num' : 1, 'list' : [{'key' : o['subjectCode'], 'name' : o['nameRU']}, ]}
				item['value'] = o[attrname]
		valuecoll.save(item)

	

def build_db_attr_rating(ratingkey='numpostoff', db_name='rpost', cname='regions', attrname='count', filter=None):
	print 'Building', ratingkey
	c = Connection()
	db = c[DB_NAME]
	regcoll = db['regions']
	sdb = c[db_name]
	rcoll = sdb[cname]	
	gtypecoll = db['grouptypes']	
	gcoll = db['groups']

	valuecoll = db['ratingvalues']
	valuecoll.remove({'key' : ratingkey})

	# Generates ratings by attribute for each region
	if filter:
		all = rcoll.find(filter).sort(attrname, -1)
	else:
		all = rcoll.find().sort(attrname, -1)
	position = 1
	last_val = None
	item = {'key' : ratingkey, 'ratingtype' : 'allregions'}			
	for o in all:
		regname = regcoll.find_one({'subjectCode' : o['subjectCode']})['nameRU']
		if last_val is None:
			last_val = o[attrname]
			item['pos'] = position
			item['members'] = {'num' : 1, 'list' : [{'key' : o['subjectCode'], 'name' : regname}, ]}
			item['value'] = o[attrname]
		elif last_val == o[attrname]:
			item['members']['num'] += 1
			item['members']['list'].append({'key' : o['subjectCode'], 'name' : regname})
		else:
			valuecoll.save(item)
			position += 1
			item = {'key' : ratingkey, 'ratingtype' : 'allregions'}
			item['pos'] = position
			item['members'] = {'num' : 1, 'list' : [{'key' : o['subjectCode'], 'name' : regname}, ]}
			item['value'] = o[attrname]
	valuecoll.save(item)

					
	# Generates ratings by group 
	groups = gcoll.find()
	for g in groups:
		gkey = g['key']
		print 'Building', ratingkey, 'for', gkey
		query = {'subjectCode': {'$in' : g['regions']['list']}}
		if filter: query.update(filter)
		all = rcoll.find(query).sort(attrname, -1)
		position = 1
		last_val = None
		item = {'key' : ratingkey, 'ratingtype' : 'bygroup', 'group_key' : gkey}
		for o in all:
			if not o.has_key(attrname): continue
			regname = regcoll.find_one({'subjectCode' : o['subjectCode']})['nameRU']
			if last_val is None:
				last_val = o[attrname]
				item['pos'] = position
				item['members'] = {'num' : 1, 'list' : [{'key' : o['subjectCode'], 'name' : regname}, ]}
				item['value'] = o[attrname]
			elif last_val == o[attrname]:
				item['members']['num'] += 1
				item['members']['list'].append({'key' : o['subjectCode'], 'name' : regname})
			else:
				valuecoll.save(item)
				position += 1
				item = {'key' : ratingkey, 'ratingtype' : 'bygroup', 'group_key' : gkey}
				item['pos'] = position
				item['members'] = {'num' : 1, 'list' : [{'key' : o['subjectCode'], 'name' : regname}, ]}
				item['value'] = o[attrname]
		valuecoll.save(item)


		
def calculate_ratings():
	build_db_attr_rating(ratingkey='rusnumbers', db_name='perepis2002', cname='tom14_25_sh_0', attrname='value', filter={'nationkey' : 'russians', 'subjectCode' : {'$exists' : True}})
	build_db_attr_rating(ratingkey='russhare', db_name='perepis2002', cname='tom14_25_sh_0', attrname='reg_share', filter={'nationkey' : 'russians', 'subjectCode' : {'$exists' : True}})
	build_db_attr_rating(ratingkey='kredactives', db_name='cbr', cname='regmap', attrname='val', filter={'ind_id' : '601'})
	build_db_attr_rating(ratingkey='kredactivesptp', db_name='cbr', cname='regmap', attrname='count_by_persons', filter={'ind_id' : '601'})
	build_db_attr_rating(ratingkey='kredactivessh', db_name='cbr', cname='regmap', attrname='share', filter={'ind_id' : '601'})
	build_db_attr_rating(ratingkey='kredpassives', db_name='cbr', cname='regmap', attrname='val', filter={'ind_id' : '603'})
	build_db_attr_rating(ratingkey='kredpassivesptp', db_name='cbr', cname='regmap', attrname='count_by_persons', filter={'ind_id' : '603'})
	build_db_attr_rating(ratingkey='kredpassivessh', db_name='cbr', cname='regmap', attrname='share', filter={'ind_id' : '603'})
	build_attr_rating('area', 'area')
	build_attr_rating('population', 'population')
	build_attr_rating('density', 'density')
	build_db_attr_rating(ratingkey='numpostoff', db_name='rpost', cname='regions', attrname='count')
	build_db_attr_rating(ratingkey='numpostoffptp', db_name='rpost', cname='regions', attrname='count_by_persons')
	build_db_attr_rating(ratingkey='numpostoffptkm', db_name='rpost', cname='regions', attrname='count_by_area')
	build_db_attr_rating(ratingkey='regdebt', db_name='minfin', cname='regions', attrname='val', filter={'dtype' : 'regdebt'})
	build_db_attr_rating(ratingkey='mundebt', db_name='minfin', cname='regions', attrname='val', filter={'dtype' : 'mundebt'})
	build_db_attr_rating(ratingkey='fulldebt', db_name='minfin', cname='regions', attrname='val', filter={'dtype' : 'fulldebt'})
	build_db_attr_rating(ratingkey='fulldebtptp', db_name='minfin', cname='regions', attrname='count_by_persons', filter={'dtype' : 'fulldebt'})
	build_db_attr_rating(ratingkey='fulldebtptkm', db_name='minfin', cname='regions', attrname='count_by_area', filter={'dtype' : 'fulldebt'})
	build_db_attr_rating(ratingkey='regdebtptp', db_name='minfin', cname='regions', attrname='count_by_persons', filter={'dtype' : 'regdebt'})
	build_db_attr_rating(ratingkey='regdebtptkm', db_name='minfin', cname='regions', attrname='count_by_area', filter={'dtype' : 'regdebt'})
	build_db_attr_rating(ratingkey='mundebtptp', db_name='minfin', cname='regions', attrname='count_by_persons', filter={'dtype' : 'mundebt'})
	build_db_attr_rating(ratingkey='mundebtptkm', db_name='minfin', cname='regions', attrname='count_by_area', filter={'dtype' : 'mundebt'})
	build_db_attr_rating(ratingkey='mundebtptkm', db_name='minfin', cname='regions', attrname='count_by_area', filter={'dtype' : 'mundebt'})




def update_all():
	update_grouptypes()
	update_feddistricts()
	update_mildistricts()
	update_ecoregions()
	update_grouptype(tablename=u'feddistricts', gtype='feddist', schema={'key' : 'abbrEN', 'regions' : 'regions', 'name' : 'nameRU', 'websiteUrl' : 'websiteURL'},)
	update_grouptype(tablename=u'mildistricts', gtype='mildist', schema={'key' : 'abbrEN', 'regions' : 'regions', 'name' : 'nameRU', 'websiteUrl' : 'milruUrl'},)
	update_grouptype(tablename=u'ecoarea', gtype='fedecoregions', schema={'key' : 'abbrEN', 'regions' : 'regions', 'name' : 'nameRU'},)
	update_grouptype(tablename=u'bvu', gtype='basinrelated', schema={'key' : 'key', 'regions' : 'regions', 'name' : 'name', 'websiteUrl' : 'websiteUrl'},)
	update_grouptype(tablename=u'fedroads', gtype='roadrelated', schema={'key' : 'key', 'regions' : 'regions', 'name' : 'nameRU', 'websiteUrl' : 'wikipediaURL'},)
	update_grouptype(tablename=u'railregions', gtype='railroadreg', schema={'key' : 'key', 'regions' : 'regions', 'name' : 'name', 'websiteUrl' : 'websiteUrl'},)
	update_grouptype(tablename=u'arbdistricts', gtype='arbitrdist', schema={'key' : 'key', 'regions' : 'regions', 'name' : 'name', 'websiteUrl' : 'websiteUrl'},)
	update_grouptype(tablename=u'pipelines', gtype='bypipeline', schema={'key' : 'key', 'regions' : 'regions', 'name' : 'name', 'websiteUrl' : 'websiteUrl'},)
	update_grouptype(tablename=u'orgs', gtype='byorg', schema={'key' : 'key', 'regions' : 'regions', 'name' : 'name', 'websiteUrl' : 'websiteUrl'},)
	find_save_notransport(name=u'Регионы вне железнодорожной сети', key='norailroads', tablename='railregions')
	find_save_notransport(name=u'Регионы вне федеральных автодорог (магистралей)', key='nofedroads', tablename='fedroads')
	generate_borders()
	update_groups()
#	generate_compare_values()
	find_regions_rels()
#	find_groups_rels()	

	


def import_all():
	import_file('regions.csv', REGION_TRANS_SCHEMA, 'regions', addIndexes=['subjectCode', 'codeOKATO', 'codeKLADR', 'regType', 'timezoneUTC', 'taxCode', 'automobileCodes', 'borderRegions'])
	merge_file('reglinks.csv', EMPTY_SCHEMA, 'regions', mergekey='subjectCode')
	import_file('feddistricts.csv', FEDDIST_TRANS_SCHEMA, 'feddistricts')
	import_file('mildistricts.csv', MILDIST_TRANS_SCHEMA, 'mildistricts')
	import_file('ecoarea.csv', EMPTY_SCHEMA, 'ecoarea')
	import_file('fedroad.csv', FEDROADS_TRANS_SCHEMA, 'fedroads')
	import_file('bvu.csv', BVU_TRANS_SCHEMA, 'bvu')
	import_file('railregions.csv', RAILREGIONS_TRANS_SCHEMA, 'railregions')
	import_file('grouptypes.csv', GROUPTYPES_TRANS_SCHEMA, 'grouptypes')
	import_file('groups.csv', GROUPS_TRANS_SCHEMA, 'groups')
	import_file('pipelines.csv', PIPELINES_TRANS_SCHEMA, 'pipelines')
	import_file('orgs.csv', ORGS_TRANS_SCHEMA, 'orgs')
	import_file('arbdistricts.csv', ARBD_TRANS_SCHEMA, 'arbdistricts')
	import_file('ratings.csv', EMPTY_SCHEMA, 'ratings')
	import_file('ratingcats.csv', EMPTY_SCHEMA, 'ratingcats')
	import_file('ratingtypes.csv', EMPTY_SCHEMA, 'ratingtypes')



if __name__ == "__main__":
#	import_all()
#	update_all()
	calculate_ratings()