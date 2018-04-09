# genesis
Scripts that creates training and testing data for geo-parsing from Wikipedia 

# structure
Genesis includes two parts, (1) a `genesis.tsv` file and (2) a folder of raw text with maps.  The `genesis.tsv` file is useful for machine learning and easily imported into databases.  Secondly, the folder contains a folder for each page on the English Wikipedia with enough text with places to make a map.  Each of these subfolders includes two files.  The first is a .txt file that includes the text of the page.  The second is a geojson file that is a map of the places mentioned in the article.

# download links
- Machine Learning Tabular Training Data: https://s3.amazonaws.com/firstdraftgis/genesis.tsv.zip
- Folder: https://s3.amazonaws.com/firstdraftgis/wikidata-gazetteer.tsv

# license
The training data is released under Creative Commons Attribution-ShareAlike 3.0 license, which is the same license as much of Wikipedia's content: https://en.wikipedia.org/wiki/Wikipedia:Text_of_Creative_Commons_Attribution-ShareAlike_3.0_Unported_License
