

```
wget https://rally-tracks.elastic.co/geopoint/documents.json.bz2
bzip2 -d documents.json.bz2
go build load.go
./load
```
