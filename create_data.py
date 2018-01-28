from broth import Broth
from bz2 import BZ2File
from config import *
from csv import DictReader
from datetime import datetime
from geojson import dumps as geojson_dumps
from geojson import Feature, Point, FeatureCollection
from gzip import GzipFile
import json
from os import mkdir
from os import remove
from os.path import isdir
from os.path import isfile
from os.path import dirname, join, realpath
import pickle
from requests import get
from re import findall
from re import search
from shutil import rmtree
from string import ascii_letters
from subprocess import call
from subprocess import check_output
from urllib.request import urlretrieve
from wake import clean_title, download_if_necessary, get_most_recent_available_dump
from wake import get_english_wikipedia_pages


blacklist = ["User talk:", "Talk:", "Comments:", "User:", "File:", "Category:", "Wikinews:", "Template:", "Category talk:", "MediaWiki:", "User:"]
non_starters = ["!", "category", "citation", "cite", "clear", "coat of arms", "collapsible", "convert", "dead", "default", "file", "flag", "flagicon", "formatnum",  "image", "incomplete", "infobox", "isbn", "lang", "main", "nomorelinks", "quote", "redirect", "see also", "template", "term-stub", "transl", "un_population", "update", "utc", "webarchive", "wikipedia", "wikt"]

def load_coordinates_dictionary():
    
    path_to_json = "/tmp/title2coords.json"
    if isfile(path_to_json):
        with open(path_to_json) as f:
            text = f.read()
            if text:
                return json.loads(text)

    # load gazetteer
    url_to_gazetteer = "https://s3.amazonaws.com/firstdraftgis/wikidata-gazetteer.tsv"
    path_to_gazetteer = download_if_necessary(url_to_gazetteer)
    print("path_to_gazetteer:", path_to_gazetteer)

    title2coords = {}
    with open(path_to_gazetteer) as f:
        for line in DictReader(f, delimiter="\t"):
            title2coords[line["enwiki_title"]] = line
    print("title2coords loaded")
    
    with open(path_to_json, "w") as f:
        f.write(json.dumps(title2coords))
    
    return title2coords

def run():
    try:
        
        # clear old version
        if isdir(path_to_data):
            rmtree(path_to_data)

        # create folder to store output/maps if doesn't exist
        mkdir(path_to_data)

        title2coords = load_coordinates_dictionary()

        page_count = 0

        for page in get_english_wikipedia_pages():
            
            page_count += 1
            
            if page_count % 100000 == 0:
                print("page_count:", page_count)
            
            #print("page:", page)
            
            page_id = page.find("id").text
            #print("page_id:", page_id)
            page_title = page.find("title").text
            #print("page_title:", page_title)
            page_text = page.find("revision/text").text
            #print("page_text:", page_text)

            if page_id and page_title and page_text:
                if page_title not in blacklist and not page_text.startswith("#REDIRECT"):
                    #print("page_id:", page_title)

                    features = []
                    # this accidentally picks up wikilinks inside of tags 
                    for link in findall("(?<={{)[^|}]+", page_text) + findall("(?<=\[\[)[^|}\]]+", page_text):
                        cleaned_title = clean_title(link)
                        if cleaned_title in title2coords:
                            place = title2coords[cleaned_title]
                            geometry = Point((float(place['longitude']), float(place['latitude'])))
  
                            properties = {}
                            for propname in ["enwiki_title", "wikidata_id", "geonames_id", "osm_id"]:
                                value = place[propname]
                                if value:
                                    properties[propname] = value
  
                            features.append(Feature(geometry=geometry, properties=properties))
                    
                    if len(features) > 1:
                        
                        feature_collection = FeatureCollection(features)
                        map_as_string = geojson_dumps(feature_collection, sort_keys=True)
                                
                        path_to_folder = join(path_to_data, page_id)
                        mkdir(path_to_folder)
                                
                        path_to_map = join(path_to_folder, page_id + ".geojson")
                        with open(path_to_map, "wb") as f:
                            f.write(map_as_string.encode("utf-8"))
    
                        path_to_text = join(path_to_folder, page_id + ".txt")
                        with open(path_to_text, "wb") as f:
                            f.write(page_text.encode("utf-8"))

    except Exception as e:
        print(e)

run()
