module github.com/Masterminds/squirrel/integration

go 1.25.7

require (
	github.com/Masterminds/squirrel v1.1.0
	github.com/go-sql-driver/mysql v1.4.1
	github.com/lib/pq v1.2.0
	github.com/mattn/go-sqlite3 v1.13.0
	github.com/stretchr/testify v1.4.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	google.golang.org/appengine v1.6.5 // indirect
	gopkg.in/yaml.v2 v2.2.2 // indirect
)

replace github.com/Masterminds/squirrel => ../
