from broth import Broth
from bz2 import BZ2File
from config import *
from csv import DictReader, writer, QUOTE_ALL
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

def reset_data_directory():
    # clear old version
    if isdir(path_to_data):
        rmtree(path_to_data)

    # create folder to store output/maps if doesn't exist
    mkdir(path_to_data)    

def load_coordinates_dictionary(save_json=False):
    
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
    
    if save_json:
        with open(path_to_json, "w") as f:
            f.write(json.dumps(title2coords))
    
    return title2coords
    
def quick_run():
    
    try:

        reset_data_directory()
    
        
        url_to_gazetteer = "https://s3.amazonaws.com/firstdraftgis/wikidata-gazetteer.tsv"
        path_to_gazetteer = download_if_necessary(url_to_gazetteer)
        print("path_to_gazetteer:", path_to_gazetteer)    
    
        place_titles = set()
        with open(path_to_gazetteer) as f:
            for line in DictReader(f, delimiter="\t"):
                enwiki_title = line["enwiki_title"]
                if enwiki_title: # probably unnecessary, but playing it safe
                    place_titles.add(enwiki_title)
        print("created place_titles")

        page_count = 0
        
        output_path = join(path_to_data, "genesis.tsv")
        open(output_path, 'w').close() # create tsv file
    
        for page in get_english_wikipedia_pages():
            
            page_count += 1
            
            if page_count % 100000 == 0:
                print("page_count:", page_count)
                
            page_id = page.find("id").text
            #print("page_id:", page_id)
            page_title = page.find("title").text
            #print("page_title:", page_title)
            page_text = page.find("revision/text").text
            #print("page_text:", page_text)            

            if page_id and page_title and page_text:
                if page_title not in blacklist and not page_text.startswith("#REDIRECT"):
                    #print("page_id:", page_title)

                    places_in_text = set()

                    # this accidentally picks up wikilinks inside of tags 
                    for link in findall("(?<={{)[^|}]+", page_text) + findall("(?<=\[\[)[^|}\]]+", page_text):
                        cleaned_title = clean_title(link)
                        if cleaned_title in place_titles and ";" not in cleaned_title:
                            places_in_text.add(cleaned_title)

                    if len(places_in_text) > 1:
                        with open(output_path, "a") as f:
                            output_writer = writer(f, delimiter="\t", quoting=QUOTE_ALL)
                            output_writer.writerow([page_id, ";".join(list(places_in_text))])
                        
    except Exception as e:
        print(e)

def run():
    try:
        
        reset_data_directory()

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
                        
                        if create_geojson:        
                            path_to_map = join(path_to_folder, page_id + ".geojson")
                            with open(path_to_map, "wb") as f:
                                f.write(map_as_string.encode("utf-8"))
    
                        path_to_text = join(path_to_folder, page_id + ".txt")
                        with open(path_to_text, "wb") as f:
                            f.write(page_text.encode("utf-8"))

    except Exception as e:
        print(e)

quick_run()
