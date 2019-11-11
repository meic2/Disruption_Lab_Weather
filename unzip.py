###### unzip all the files
import zipfile
archive = zipfile.ZipFile('download.zip', 'r')

archive.extractall()

