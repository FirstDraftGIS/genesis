from broth import Broth
from bz2 import BZ2File
from datetime import datetime
from geojson import dumps as geojson_dumps
from geojson import Feature, Point, FeatureCollection
from gzip import GzipFile
from os import mkdir
from os import remove
from os.path import isdir
from os.path import isfile
from os.path import dirname, join, realpath
from requests import get
from re import findall
from re import search
from shutil import rmtree
from string import ascii_letters
from subprocess import call
from subprocess import check_output
from urllib.request import urlretrieve

start_line = 0
end_line = 1000000000000

blacklist = ["User talk:", "Talk:", "Comments:", "User:", "File:", "Category:", "Wikinews:", "Template:", "Category talk:", "MediaWiki:", "User:"]
non_starters = ["!", "category", "citation", "cite", "clear", "coat of arms", "collapsible", "convert", "dead", "default", "file", "flag", "flagicon", "formatnum",  "image", "incomplete", "infobox", "isbn", "lang", "main", "nomorelinks", "quote", "redirect", "see also", "template", "term-stub", "transl", "un_population", "update", "utc", "webarchive", "wikipedia", "wikt"]


def run_sql(sql_statement, debug=False):
    try:
        if debug: print("starting run_sql with:", sql_statement)
        sql_statement = sql_statement.replace('"', '\\"')
        bash_command = '''mysql -u root genesis -e "''' + sql_statement + '''"'''
        if debug: print("bash_command:", bash_command)
        output = check_output(bash_command, shell=True).decode("utf-8")
        if debug: print("output: " + output)
        # format as rows of dictionary objects
        lines = output.strip().split("\n")
        if lines:
            header = lines[0].split("\t")
            if debug: print("header:", header)
            if len(lines) > 1:
                result = [dict(zip(header, line.split("\t"))) for line in lines[1:]]
                if debug: print("result:", str(result))
                return result
    except Exception as e:
        print("run_sql caught exception " + str(e) + " while trying to run " + sql_statement)
        raise e
    
 

def download_if_necessary(url, debug=False):
    if debug: print("starting download_if_necessary with: " + url)
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
        raise e

def load_geotags(ymd):
    try:
        print("starting load_geotags")
        start = datetime.now()


        # do I have to create my own geotags with infoboxes?? like https://en.wikipedia.org/wiki/International_Food_Policy_Research_Institute
        # is infobox approach reliable??

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
        raise e

def clean_title(title):
    return title.replace("'","\\'").replace("`","\\`").replace('"','\\"').rstrip("\\")

def insert_values_into_page_titles(values, debug=False):
    if debug: print("starting insert_values_into_page_titles with " + str(len(values)))
    try:
        run_sql("INSERT INTO page_titles VALUES " + ",".join([ "(" + temp_page_id + ",'" + temp_page_title + "')" for temp_page_id, temp_page_title in values]))
    except Exception as e:
        if len(values) == 1:
            print("[insert_values_into_page_titles] only had one values, so skipping")
        else:
            print("insert_values_into_page_titles caught exception, so will try division")
            half_length = round(len(values) / 2)
            print("half_length:", str(half_length))
            chunks = [ values[:half_length], values[half_length:] ]
            for chunk in chunks:
                insert_values_into_page_titles(chunk, debug=True)

def load_page_titles(path_to_pages):
    start = datetime.now()
    print("starting load_page_titles")
    call("mysql -u root genesis -e 'TRUNCATE TABLE page_titles'", shell=True)
    with BZ2File(path_to_pages) as f:
        print("f:", f)
        count = 0
        page_id = None
        page_title = None

        values = []
        for line in f:
            line = line.decode("utf-8", "replace").strip()
            count += 1
            #print(line[:10])
            if line == "<page>":
                page_title = None
                page_id = None
            elif page_id is None and line.startswith("<id>"):
                page_id = line.replace("<id>","").replace("</id>","")
            elif page_title is None and line.startswith("<title>"):
                page_title = line.replace("<title>","").replace("</title>","")
            elif line == "</page>":
                title_in_lower_case = page_title.lower()
                if page_title not in blacklist and len(title_in_lower_case) > 3 and not any(title_in_lower_case.startswith(w) for w in non_starters):
                    values.append((page_id, clean_title(page_title)))
                page_title = None
                page_id = None
               
            if len(values) >= 2000:
                insert_values_into_page_titles(values)
                values = []
            if count >= 1000000000000000000000:
                break

        if values:
            insert_values_into_page_titles(values)
 

    print("loading page titles took " + str((datetime.now() - start).total_seconds()) + " seconds")

def load_test_page_titles(path_to_titles):
    call("mysql -u root genesis -e 'TRUNCATE TABLE page_titles'", shell=True)
    with open("test_title.txt") as f:
        title = clean_title(f.read())
        run_sql("INSERT INTO page_titles VALUES (1,'" + title + "')")
 
def create_maps(path_to_pages, debug_level=1):

    try:

        path_to_data = "/tmp/genesis"
        if isdir(path_to_data):
            print("trying to remove") 
            rmtree(path_to_data)
            print ("removed")
        mkdir(path_to_data)
        with BZ2File(path_to_pages) as f:
            print("f:", f)

            in_text = False            
            page_id = None
            possible_locations = None
            title = None
            text = None

            start_jumping = datetime.now() 
            for i in range(start_line):
                f.readline()
            count = start_line
            print("it took " + str((datetime.now() - start_jumping).total_seconds()) + " seconds to jump to line " + str(start_line))

            for line in f:
                try:
                    count += 1

                    if count % 10000 == 0:
                        print("line: " + str(count))

                    #print "line", count,":", line,

                    line = line.decode("utf-8").strip()

                    if line.startswith("<page"):
                        if debug_level == 2: print("beginning page")
                        in_text = False
                        page_id = None
                        possible_locations = None
                        title = None
                        text = None
                    elif page_id is None and line.startswith("<id>"):
                        page_id = line.replace("<id>","").replace("</id>","")
                    elif line == "</page>":
                        if debug_level == 2: print("ending page")
                        if page_id and text and title not in blacklist and not text.startswith("#REDIRECT"):
                            if debug_level == 2: print("title:", title)
                            #print "text:", text

                            # this accidentally picks up wikilinks inside of tags 
                            possible_locations = findall("(?<={{)[^|}]+", text) + findall("(?<=\[\[)[^|}\]]+", text)
                            possible_locations = [l for l in possible_locations if not any(l.lower().startswith(w) for w in non_starters) and len(l) > 3]
                            possible_locations = list(set(possible_locations)) #removing duplicates
                            if possible_locations:
                                if debug_level == 2: print("possible_locations:", possible_locations)
                                # may have to chunk up list, so doesn't hit max limit for bash statement
                                rows = run_sql("SELECT * FROM wikiplaces WHERE TITLE IN (\'" + "\',\'".join([clean_title(l) for l in possible_locations]) + "\');", debug=False)
                                if debug_level == 2: print("rows:", rows)
                                if rows:
                                    features = []
                                    for row in rows:
                                        geometry = Point((float(row['longitude']), float(row['latitude'])))
                                        properties = {"title": row['title'], "page_id": row['page_id']}
                                        features.append(Feature(geometry=geometry, properties=properties))
                                    feature_collection = FeatureCollection(features)
                                    map_as_string = geojson_dumps(feature_collection, sort_keys=True)
                                    if debug_level == 2: print("map_as_string:", map_as_string)
                                    name_for_file_system = page_id + ("-" + title if len(title) < 25 and len(title) == len([char for char in title if char in ascii_letters]) else "")
                                    path_to_folder = join(path_to_data, name_for_file_system)
                                    mkdir(path_to_folder)

                                    path_to_map = join(path_to_folder, name_for_file_system + ".geojson")
                                    if debug_level == 2: print("path_to_map: " + path_to_map)
                                    with open(path_to_map, "wb") as f:
                                        f.write(map_as_string.encode("utf-8"))
                                        if debug_level == 2: print("wrote: " + path_to_map)

                                    path_to_text = join(path_to_folder, name_for_file_system + ".txt")
                                    if debug_level == 2: print("path_to_text: " + path_to_text)
                                    with open(path_to_text, "wb") as f:
                                        f.write(text.encode("utf-8"))
                                        if debug_level == 2: print("wrote: " + path_to_text)

 
                        in_text = False
                        page_id = None
                        possible_locations = None
                        title = None
                        text = None
                    if line.startswith("<title"):
                        title = line.strip("<title>").strip("</title>")
                    elif line.startswith("<text"):
                        in_text = True
                        text = line.lstrip('<text xml:space="preserve">')
                    elif text and line.endswith("</text>"):
                        text += line.rstrip("</text>")
                        in_text = False
                        #cursor.execute("select * FROM page_props WHERE pp_propname = 'displaytitle' AND pp_value='<i>Main Page</i>' LIMIT 10;"
                    elif in_text:
                        text += line

                    if count >= end_line:
                        exit()
                except Exception as e:
                    print("caught exception in make_maps on line " + str(count) + ": " + str(e))
                    raise e
    except Exception as e:
        print("caught exception in make_maps " + str(e))
        raise e


def create_wiki_places(): 
    print("starting create_wiki_places")
    filepath = "/tmp/wikiplaces.tsv"
    #if isfile(filepath): remove(filepath)
    run_sql("DROP TABLE wikiplaces", debug=True)
    run_sql("""
        CREATE TABLE wikiplaces AS
        SELECT gt_page_id AS page_id, title, gt_lat AS latitude, gt_lon AS longitude
        FROM page_titles
        INNER JOIN geo_tags
        ON page_titles.id = geo_tags.gt_page_id;
    """, debug=True)
 
    run_sql("""
        SELECT *
        FROM wikiplaces
        INTO OUTFILE '/tmp/wikiplaces.tsv'
        FIELDS TERMINATED BY '\t'
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n';
    """, debug=True)
    print("finishing create_wiki_places")

def run():
    try:
        print("starting load_geotags")
        start = datetime.now()
        ymd, jobs = get_most_recent_available_dump()
        call("mysql -u root -e 'CREATE USER IF NOT EXISTS ubuntu'", shell=True)
        #call("mysql -u root -e 'DROP DATABASE genesis'", shell=True)
        call("mysql -u root -e 'CREATE DATABASE IF NOT EXISTS genesis DEFAULT CHARACTER SET utf8mb4 DEFAULT COLLATE utf8mb4_unicode_ci'", shell=True)

        #load_geotags(ymd)
        run_sql("DELETE FROM geo_tags WHERE gt_primary != 1;", debug=True)

        path_to_pages = download_if_necessary("https://dumps.wikimedia.org/enwiki/" + ymd + "/enwiki-" + ymd + "-pages-articles.xml.bz2")
        print("path_to_pages:", path_to_pages)
        #run_sql("DROP TABLE IF EXISTS page_titles", debug=True)
        run_sql("CREATE TABLE IF NOT EXISTS page_titles (id int(10), title VARCHAR(500))", debug=True)

        #path_to_titles = download_if_necessary("https://dumps.wikimedia.org/enwiki/" + ymd + "/enwiki-" + ymd + "-all-titles.gz", debug=True)
        #print("path_to_titles:", path_to_titles)
        #load_test_page_titles(path_to_titles)


        #load_page_titles(path_to_pages)
        #create_wiki_places()
        
        
        create_maps(path_to_pages, debug_level=1)
        #call("zip -r genesis.zip genesis", cwd="/tmp/", shell=True)
    except Exception as e:
        print(e)

run()
