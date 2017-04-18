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

With the bare minimum of options specified (client-id, client-secret, and
user agent), the app will scrape as many posts as it can as far back as it
can until it either sees a nil in the `after` key of the listing, or until
it doesn't insert as many posts as it requests.  The former happens when you
exhaust the posts that reddit provides on `/r/[subreddit]/new`; the latter when
the app tries to insert a post that already exists in the database. Currently,
reddit only provides up to 1000 posts, but sometimes it is off (less) by a few.

To go back as far as possible without stopping at the first duplicate, use
`--force`.

To periodically re-scrape from the beginning, use `--wait` and specify the time
to wait, in seconds, between scrape runs. This waiting period is independent
of the 1s rate limit between requests, so don't set this to nonsense like 0
(what's input validation?). This option can be combined with ``--force`.

## Options

Run with `--help` flag to see the options. Notably, use `--wait` to scrape
in a loop. A user agent is required, and should be provided with `--user-agent`.

## Examples

    java -jar foo.jar --user-agent $USER_AGENT

## Bugs

Doesn't handle errors. If reddit dies, or if the internet connection dies,
who knows what'll happen.

## License

Copyright © 2017 Charlie Wang

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
