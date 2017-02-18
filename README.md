* scrapes linkedin employees per company url, 
including those out of your network, exploiting profile's
`also viewed` list behaviour

* writes Name, Title, Location, URL to `.json`

* this doesn't work with linkedin's new design

* uses redis caching server to reduce number of requests to linkedin servers