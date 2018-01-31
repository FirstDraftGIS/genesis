start_line = 0
end_line = 10000

# for downloading page-articles in chunks
number_of_chunks = 5000000000000

CHUNK = 16 * 1024

path_to_data = "/tmp/genesis"

create_geojson = False
create_table = True

blacklist = ["User talk:", "Talk:", "Comments:", "User:", "File:", "Category:", "Wikinews:", "Template:", "Category talk:", "MediaWiki:", "User:"]
non_starters = ["!", "category", "citation", "cite", "clear", "coat of arms", "collapsible", "convert", "dead", "default", "file", "flag", "flagicon", "formatnum",  "image", "incomplete", "infobox", "isbn", "lang", "main", "nomorelinks", "quote", "redirect", "see also", "template", "term-stub", "transl", "un_population", "update", "utc", "webarchive", "wikipedia", "wikt"]
