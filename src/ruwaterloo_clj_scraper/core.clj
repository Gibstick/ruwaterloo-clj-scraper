(ns ruwaterloo-clj-scraper.core
  (:require
    [ruwaterloo-clj-scraper.reddit :refer :all]
    [clojure.string :as str]
    [clojure.core.async :as async :refer
     [timeout <! <!! >! >!! thread chan go go-loop sliding-buffer]]
    [clojure.tools.cli :refer [parse-opts]]
    [jdbc.core :as jdbc]
    [taoensso.timbre :as timbre])
  (:gen-class))

;; keys we don't want from the json
;; this isn't used because we grab the keys directly from the schema map
(def ^:const key-blacklist
  #{:approved_by
    :mod_reports
    :visited
    :report_reasons
    :user_reports
    :banned_by
    :media_embed
    :media
    :secure_media_embed
    :num_reports
    :hide_score
    :saved
    :clicked
    :secure_media
    :subreddit_type})

(def ruwaterloo-posts-schema
  [[:id "TEXT PRIMARY KEY NOT NULL"]
   [:subreddit_name_prefixed "TEXT"]
   [:created_utc "INTEGER"]
   [:permalink "TEXT"]
   [:distinguished "TEXT"]
   [:selftext "TEXT"]
   [:brand_safe "INTEGER"]                                  ; boolean
   [:author_flair_css_class "TEXT"]
   [:locked "INTEGER"]                                      ; boolean
   [:spoiler "INTEGER"]                                     ; boolean
   [:likes "INTEGER"]
   [:url "TEXT"]
   [:subreddit_id "TEXT"]
   [:suggested_sort "TEXT"]
   [:removal_reason "TEXT"]
   [:domain "TEXT"]
   [:thumbnail "TEXT"]
   [:stickied "INTEGER"]                                    ; boolean
   [:is_self "BOOLEAN"]
   [:over_18 "INTEGER"]                                     ; boolean
   [:archived "INTEGER"]                                    ; boolean
   [:name "TEXT"]
   [:created "INTEGER"]                                     ; Unix timestamp
   [:subreddit "TEXT"]
   [:gilded "INTEGER"]                                      ; boolean
   [:author_flair_text "TEXT"]
   [:ups "INTEGER"]
   [:selftext_html "TEXT"]
   [:author "TEXT"]
   [:num_comments "INTEGER"]
   [:score "INTEGER"]
   [:downs "INTEGER"]
   [:link_flair_text "TEXT"]
   [:link_flair_css_class "TEXT"]
   [:title "TEXT"]
   [:edited "INTEGER"]                                      ; boolean
   [:contest_mode "INTEGER"]                                ; boolean
   [:hidden "INTEGER"]                                      ; boolean
   [:quarantine "INTEGER"]                                  ; boolean
   ])

(def status-schema
  [[:subreddit "TEXT PRIMARY KEY NOT NULL"]
   [:after "TEXT"]
   [:before "TEXT"]
   [:ts "INTEGER"]])


;; jdbc doesn't support IF NOT EXISTS
(defn create-table-if-not-exists-ddl
  "CREATE TABLE $table-name IF NOT EXISTS (... $specs ...)"
  [table-name specs]
  (format "CREATE TABLE IF NOT EXISTS %s (%s)"
          table-name
          (->> specs
               (map #(format "%s %s" (name (nth % 0)) (nth % 1)))
               (str/join ", "))))


(defn initialize-db! [db]
  (jdbc/atomic db
    (jdbc/execute db (create-table-if-not-exists-ddl "ruwaterloo_posts" ruwaterloo-posts-schema))
    (jdbc/execute db (create-table-if-not-exists-ddl "status" status-schema)))
  nil)


(defn insert-from-post
  "Given a schema (table spec) and a post,
  get a vector of values suitable for an insert into the database."
  [schema {:keys [kind data] :as post}]
  (assert (= kind "t3") "Expected 't3' kind for post")
  (mapv (fn [[col _]] (col data)) ruwaterloo-posts-schema))

(defn insert-post-dml
  "Generates a sqlvec containing the INSERT query
  and vectors of parameters.

  Does OR IGNORE"
  [table schema post]
  (into [(format "INSERT OR IGNORE INTO %s VALUES (%s)"
                 table
                 (str/join ", " (repeat (count schema) "?")))]
        (insert-from-post schema post)))

(defn insert-posts!
  "Insert a sequence of posts into the database.
  The sequence of posts should normally come from
  (get-in resp [:data :children]) where resp is the response
  from reddit-get->json.

  Returns the number of posts inserted."
  [db table schema posts]
  (jdbc/atomic db
    (->>
      (map (fn [post] (jdbc/execute db (insert-post-dml table schema post)))
           posts)
      (reduce +))))


(defn- fetch-after [db subreddit]
  (first (jdbc/fetch db ["select after from status where subreddit = ?"
                         subreddit])))

(defn- scrape!*
  "This exists because jdbc/atomic wraps the body
  in a function, breaking recur targets."
  ([db reddit after force? rate-limiter]
   (let [limit 100
         params (cond-> {:limit limit} after (assoc :after after))
         resp (reddit-get->json reddit "https://oauth.reddit.com/r/uwaterloo/new.json"
                                params)
         posts (get-in resp [:data :children])
         next-after (get-in resp [:data :after])
         inserted (insert-posts! db "ruwaterloo_posts" ruwaterloo-posts-schema posts)]
     (assert (<= inserted limit) "Only inserts up to limit")

     (timbre/debugf "Next after: %s" next-after)
     (timbre/infof "Inserted %s" inserted)

     ;; next-after is nil (not present?) when we've exhausted the listing
     (when (or (and force? next-after)
               (= inserted limit))
       next-after))))


(defn scrape!
  "Scrape /r/uwaterloo as far back as possible.

  This basically pages through /r/uwaterloo/new until it tries
  to insert something that is already in the database. This
  could miss posts, if posts are somehow inserted into the
  middle of the timeline.  For example, if posts [a b d e] are
  inserted, and then post c magically appears, this function
  might miss it because it will stop at either a or b (the
  real problem is actually scaled up by 100 since we fetch in
  batches of 100).

  In practice this isn't much of a problem, because we can
  periodically do full-scrapes of all 1000 posts accessible at
  this endpoint.

  To force a full scrape, set :force? to true. When this is set,
  the function tries to page through all 1000 posts.

  This function runs synchronously and makes at most 1 request
  per second, as per reddit's rate limits. We don't do anything
  fancy -- if a request takes longer than 1 second, the next
  request will be made as soon as the current one finishes.

  Optionally set :rate-limiter in opts to a channel that controls
  rate limiting. The channel should be ready for taking at most
  once per second.
  "
  ([db reddit] (scrape! db reddit {}))
  ([db reddit {:keys [force? rate-limiter]}]
   (loop [after (fetch-after db "uwaterloo")]
     ;; This is really stupid.
     ;; jdbc/atomic wraps the body in a function, which creates a new recur target!
     (let [timer (or rate-limiter (timeout 1000))
           next-after
           (jdbc/atomic db
             (scrape!* db reddit after force? rate-limiter))]
       (when next-after
         (timbre/debugf "Waiting on rate limiter")
         (<!! timer)
         (recur next-after ))))))

;; inserting into status table is obsolete?
(comment
  (jdbc/with-db-transaction [db db]
                            (jdbc/delete! db :status ["subreddit = ?" "uwaterloo"])
                            (jdbc/insert! db :status [:subreddit :after :ts]
                                          ["uwaterloo" next-after ts])))


;; increasing levels of verbosity
(def timbre-log-levels [:warn :info :debug :trace])

(def cli-opts
  [["-a" "--user-agent USER-AGENT" "User Agent"
    :id :user-agent]
   ["-f" "--force" "Force scraping as many posts as possible, regardless of posts in DB"
    :id :force?]
   ["-w" "--wait WAIT-SECONDS" "Scrape continuously, waiting WAIT-SECONDS between scrapes."
    :id :wait-seconds
    :default false
    :parse-fn #(Integer/parseInt %)
    :validate [#(> % 0) "Must be a positive number."]]
   ["-v" nil "Verbosity level; may be specified multiple times to increase value"
    :id :verbosity
    :default 0
    ;; Use assoc-fn to create non-idempotent options
    :assoc-fn (fn [m k _] (update-in m [k] inc))]
   ["-h" "--help" "Show this help message"]
   ])


;; inspired by https://github.com/clojure/tools.cli
(defn validate-args
  [args]
  (let [{:keys [options arguments errors summary] :as parsed}
        (parse-opts args cli-opts)]

    (cond
      errors
      {:exit-message (str (str/join \newline errors) "\n\n" summary)
       :exit-code    1}

      (:help options)
      {:exit-message summary :exit-code 0}

      :else
      (assoc options :ok? true))))

(defn -main
  [& args]
  (timbre/set-level! :error)
  (let [dbspec {:subprotocol "sqlite"
                :subname     "ruwaterloo.sqlite3"}

        client-id     (System/getenv "CLJ_SCRAPER_CLIENT_ID")
        client-secret (System/getenv "CLJ_SCRAPER_CLIENT_SECRET")

        _ (assert client-id)
        _ (assert client-secret)

        {:keys [ok? user-agent force? wait-seconds verbosity
                exit-code exit-message]}
        (validate-args args)

        rate-limiter (chan 1)

        reddit
        (and ok? (reddit-oauth2-readonly client-id client-secret user-agent))]
    (with-open [conn (jdbc/connection dbspec)]
      (timbre/set-level!
        (->> (count timbre-log-levels) dec (min verbosity) (nth timbre-log-levels)))

      (if-not ok?
        (do
          (println exit-message)
          (System/exit exit-code))
        (do
          (initialize-db! conn)
          (go-loop []
            (timbre/debug "Allowing 1 request")
            (<! (timeout 1000))
            (>! rate-limiter true)
            (recur))
          (loop []
            (timbre/info "Scraping")
            (scrape! conn reddit
                     {:force? force? :rate-limiter rate-limiter})
            (when wait-seconds
              (timbre/infof "Waiting %ss between scrapes" wait-seconds)
              (<!! (timeout (* wait-seconds 1000)))
              (recur))))))))
