#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pymongo
from collections import Counter

client = pymongo.MongoClient('mongodb://localhost:27017')

db = client.osm
standard_tags = ['created','id','type','node_refs','members']

def count_tags(lst_doc):
	'''This method takes a list of documents from our 
	database and counts the tags in all documents in the list.
	It returns a dictionary with "tag":<number_of_occurences> pairs'''
	tags = {}
	for w in lst_doc:
		for tag in w:
			if tag not in standard_tags:
				tags[tag] = tags.get(tag,0)+1
	return tags

def street_types(streets):
	'''This method take a list of streets (as strings)
	and outputs a dictionary with the first words in each street name
	and the number of appearence of each of those words.'''
	types = {}
	for street in streets:
		first_word = street.split()[0]
		types[first_word] = types.get(first_word,0) + 1
	return types


expected_street_prefix = [
	 u'Acces',
	 u'Aeroportul',
	 u'Aleea',
	 u'Autostrada',
	 u'Bulevardul',
	 u'Calea',
	 u'DN7',
	 u'Drumul',
	 u'Intrarea',
	 u'Pasajul',
	 u'Pia\u021ba',
	 u'Podul',
	 u'Prelungirea',
	 u'Splaiul',
	 u'Strada',
	 u'\u0218oseaua']

street_mappings = {
	  u'A1': 'Autostrada A1',
	  u'acces': u'Acces',
	  u'Alee': u'Aleea',
	  u'Cale': u'Calea',
	  u'Drum': u'Drumul',
	  u'Intrare': u'Intrarea',
	  u'Intrares': u'Intrarea',
	  u'Pasaj': u'Pasajul',
	  u'Pia\u021beta': u'Pia\u021ba',
	  u'Pod': u'Podul',
	  u'Strava': u'Strada',
	  u'intrare': u'Intrarea',
	  u'\xcentrarea': u'Intrarea',
	  u'Soseaua': u'\u0218oseaua',
	  u'\u015eoseaua': u'\u0218oseaua'}

def correct_street_name(street):
	first_word = street.split()[0]
	if first_word in expected_street_prefix:
		return street
	if first_word in street_mappings:
		return street.replace(first_word,street_mappings[first_word])
	return None

def update_address(building, street_name):
	''' First we simply update the address dictionary with the existing tags,
	address['street']=correct_street_name(address['street'])
	then we update the street name. '''
	address = {}
	tags = building.keys()
	for addr_field in tags:
		if addr_field.find('addr')==0:
			address[addr_field[5:]] = building[addr_field]
			db.bucharest.update({'_id':building['_id']},
								{'$unset':{addr_field:''}})
	address['street']=street_name
	db.bucharest.update({'_id':building['_id']}, {'$set':{'address':address}})


def audit_street_names(db):
	'''Function for auditing all street names.'''
	# Auditing addresses
	buildings = db.bucharest.find({'type':'way',
								   'building':{'$exists':1},
								   'addr:street':{'$exists':1}})
	for building in buildings:
		street_name = correct_street_name(building['addr:street'])
		if street_name:
			update_address(building, street_name)
	highways = db.bucharest.find({'type':'way',
								   'highway':{'$exists':1},
								   'name':{'$exists':1}})
	for street in highways:
		street_name = correct_street_name(street['name'])
		if street_name:
			db.bucharest.update({'_id':street['_id']},
								{'$set':{'name':street_name}})

def audit_street_names2(db):
	''' There are a few elements that are marked as highways,
	but also contain a addr:street tag.
	Example:
	{u'_id': ObjectId('5576f7b53b515e3dd2c18711'),
	 u'addr:city': u'Bucure\u0219ti',
	 u'addr:street': u'Strada Novaci',
	 u'created': {u'changeset': u'21773724',
	              u'timestamp': u'2014-04-18T18:02:18Z',
	              u'uid': u'128281',
	              u'user': u'razor74',
	              u'version': u'1'},
	 u'highway': u'tertiary',
	 u'id': u'275299472',
	 u'is_in:city': u'Bucure\u0219ti',
	 u'lanes': u'2',
	 u'maxspeed': u'RO:urban',
	 u'name': u'Strada Novaci',
	 u'node_refs': [u'2407819871',
	                u'2407824788',
	                u'256366536',
	                u'2197697277',
	                u'2197695228',
	                u'256366244',
	                u'2407824798',
	                u'256366205',
	                u'256365372',
	                u'256366349',
	                u'14485326'],
	 u'smoothness': u'good',
	 u'surface': u'asphalt',
	 u'type': u'way'}

	The "addr:street" tag is not needed since the information is
	in the "name" tag.
	This function addresses only those elements.
	'''
	highways = db.bucharest.find({'type':'way',
								   'highway':{'$exists':1}})
	for street in highways:
		if 'addr:street' in street:
			if 'name' not in street:
				db.bucharest.update({'_id':street['_id']},
									{'$set':{'name':street['addr:street']}})
			db.bucharest.update({'_id':street['_id']},
								{'$unset':{'addr:street':''}})		
		if 'addr:city' in street:
			if 'is_in:city' not in street:
				db.bucharest.update({'_id':street['_id']},
									{'$set':{'is_in:city':'București'}})
			db.bucharest.update({'_id':street['_id']},
								{'$unset':{'addr:city':''}})

def audit_amenities(db):
	''' Adds "amenity" tag to an element if the element contains
	a tag that is known to be an amenity type. '''
	MIN_APPEREANCE_OF_AMENITY = 3 
	amenities = db.bucharest.find({'amenity':{'$exists':1}})
	am_tags = [am['amenity'] for am in amenities]
	tags_count = Counter(am_tags)
	updated = {}
	for tag in tags_count:
		if tags_count[tag] >= MIN_APPEREANCE_OF_AMENITY:
			wr_res = db.bucharest.update({tag:{'$exists':1}},
								{'$set':{'amenity':tag}},
								upsert=False,
								multi=True)
			db.bucharest.update({tag:'yes'},
								{'$unset':{tag:''}},
								upsert=False,
								multi=True)

			updated[tag]=wr_res['n']
	return updated

def main():
	audit_street_names(db)

if __name__ == '__main__':
	main()