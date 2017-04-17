# ruwaterloo-clj-scraper

A scraper for /r/uwaterloo. Scrapes submissions from /r/uwaterloo/new and
writes them into an sqlite database. The app itself dose not require your
reddit username/password.

## Usage
    export CLIENT_SECRET=foobar
    export CLIENT_ID=baz
    lein uberjar
    java -jar target/uberjar/ruwaterloo-clj-scraper-0.1.0-standalone.jar --user-agent

This scraper is a read-only OAuth2 app. To obtain a client secret and a
client id, navigate to the `apps` section of reddit preferences and create
a script-type app.

## Options

Run with `--help` flag to see the options. Notably, use `--wait` to scrape
in a loop. A user agent is required, and should be provided with `--user-agent`.

## Examples

    java -jar foo.jar --user-agent $USER_AGENT

## License

Copyright © 2017 Charlie Wang

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
