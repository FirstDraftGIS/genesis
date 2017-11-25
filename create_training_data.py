from broth import Broth
from bz2 import BZ2File
from datetime import datetime
from gzip import GzipFile
from os import mkdir
from os import rmdir
from os import remove
from os.path import isdir
from os.path import isfile
from os.path import dirname, join, realpath
from requests import get
from re import findall
from re import search
from subprocess import call
from urllib.request import urlretrieve

start_line = 0
end_line = 5000

def download_if_necessary(url):
    path_to_downloaded_file = "/tmp/" + url.split("/")[-1]
    if not isfile(path_to_downloaded_file):
        urlretrieve(url, path_to_downloaded_file)
        print("downloaded:", url, "to", path_to_downloaded_file)
    return path_to_downloaded_file

def get_most_recent_available_dump():

    try:

        enwiki_url = "https://dumps.wikimedia.org/enwiki/"

        broth = Broth(get(enwiki_url).text)
        print("broth:", type(broth))
        dumps = [a.get("href").rstrip("/") for a in broth.select("a") if not a.text.startswith("latest") and a.get("href") != "../"]
        dumps.reverse()
        print("dumps:", dumps)

        for dump in dumps:
           jobs = get(enwiki_url + dump + "/dumpstatus.json").json()['jobs']
           if jobs['geotagstable']['status'] == "done" and jobs['pagepropstable']['status'] == "done" and jobs['articlesdump']['status'] == "done":
               print("geotags dump on " + dump + " is ready")
               return dump, jobs

    except Exception as e:
        print(e)

def load_geotags(ymd):
    try:
        print("starting load_geotags")
        start = datetime.now()

        downloadpath = download_if_necessary("https://dumps.wikimedia.org/enwiki/" + ymd + "/enwiki-" + ymd + "-geo_tags.sql.gz")
        print("downloadpath:", downloadpath)

        call("mysql -u root genesis -e 'DROP TABLE IF EXISTS geo_tags'", shell=True)
        print("dropped old geo_tags table")

        call("zcat " + downloadpath + " | mysql -u root genesis", shell=True)
        print("loaded geo tags into the database")

        end = datetime.now()
        print("loading geo_tags took " + str((end-start).total_seconds()) + " seconds")

    except Exception as e:
        print(e)

def load_page_titles(path_to_pages):
    start = datetime.now()
    print("starting load_page_titles")
    call("mysql -u root genesis -e 'TRUNCATE TABLE page_titles'", shell=True)
    with BZ2File(path_to_pages) as f:
        print("f:", f)
        count = 0
        page_id = None
        page_title = None
        for line in f:
            line = line.decode().strip()
            count += 1
            #print(line)
            if line == "<page>":
                page_title = None
                page_id = None
            elif page_id is None and line.startswith("<id>"):
                page_id = line.replace("<id>","").replace("</id>","")
            elif page_title is None and line.startswith("<title>"):
                page_title = line.replace("<title>","").replace("</title>","")
            elif line == "</page>":
                #print("page_title:", page_title)
                #print("page_id:", page_id)
                call("mysql -u root genesis -e \"INSERT INTO page_titles VALUES(" + page_id + ",'" + page_title.replace("'","\\'") + "')\"", shell=True)
                page_title = None
                page_id = None
                
            if count >= 10000000000:
                break
    print("loading page titles took " + str((datetime.now() - start).total_seconds()) + " seconds")

def create_maps(path_to_pages):

    try:

        blacklist = ["User talk:", "Talk:", "Comments:", "User:", "File:", "Category:", "Wikinews:", "Template:", "Category talk:", "MediaWiki:", "User:"]
        non_starters = ["!", "category", "cite", "clear", "file", "isbn", "lang", "main", "quote", "redirect", "see also", "wikipedia"]

        path = "/tmp/genesis"
        if isdir(path):
            print("trying to remove") 
            rmdir(path)
            print ("removed")
        mkdir(path)
        with BZ2File(path_to_pages) as f:
            print("f:", f)
            count = 0

            in_text = False            
            page_id = None
            title = None
            text = None

            for line in f:
                try:
                    count += 1
                    if count <= start_line:
                        continue

                    #print "line", count,":", line,

                    line = line.decode().strip()

                    if line.startswith("<page"):
                        print("beginning page")
                        in_text = False
                        page_id = None
                        title = None
                        text = None
                    elif line == "</page>":
                        print("ending page")
                    if line.startswith("<title"):
                        title = line.strip("<title>").strip("</title>")
                        #if not any(phrase in line for phrase in blacklist):
                        #    print "title:", title
                        #number_of_titles += 1
                    elif line.startswith("<text"):
                        in_text = True
                        text = line.lstrip('<text xml:space="preserve">')
                    elif line.endswith("</text>"):

                        if title not in blacklist and not text.startswith("#REDIRECT"):
                            text += line.rstrip("</text>")
                            print("title:", title)
                            #print "text:", text

                            # this accidentally picks up wikilinks inside of tags 
                            possible_locations = findall("(?<={{)[^|}]+", text) + findall("(?<=\[\[)[^|}\]]+", text)
                            possible_locations = [l for l in possible_locations if not any(l.lower().startswith(w) for w in non_starters)]
                            print("possible_locations:", possible_locations)

                            #page_ids = PageProps.objects.filter(pp_propname="displaytitle", pp_value__in=display_titles).values_list("pp_page", flat=True)
                            
                            #print("page_ids:", page_ids)
                            #geo_tags = GeoTags.objects.filter(gt_page_id__in=page_ids)
                            #print("geo_tags:", geo_tags)

                        in_text = False
                        text = None
                        title = None
                        #cursor.execute("select * FROM page_props WHERE pp_propname = 'displaytitle' AND pp_value='<i>Main Page</i>' LIMIT 10;"
                    elif in_text:
                        text += line

                    if count >= 1000:
                        exit()
                except Exception as e:
                    print("caught exception on line " + str(count) + ": " + str(e))
    except Exception as e:
        print("caught exception " + str(e))

   

def run():
    try:
        print("starting load_geotags")
        start = datetime.now()
        ymd, jobs = get_most_recent_available_dump()
        #call("mysql -u root -e 'CREATE USER IF NOT EXISTS ubuntu'", shell=True)
        #call("mysql -u root -e 'CREATE DATABASE IF NOT EXISTS genesis'", shell=True)
        #load_geotags(ymd)
        path_to_pages = download_if_necessary("https://dumps.wikimedia.org/enwiki/" + ymd + "/enwiki-" + ymd + "-pages-articles.xml.bz2")
        print("path_to_pages:", path_to_pages)
        #call("mysql -u root genesis -e 'CREATE TABLE page_titles (id int(10), title VARCHAR(200))'", shell=True)
        #load_page_titles(path_to_pages)
        create_maps(path_to_pages)
        exit()
    except Exception as e:
        print(e)

run()
