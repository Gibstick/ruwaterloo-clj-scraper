(ns ruwaterloo-clj-scraper.core
  (:require
    [ruwaterloo-clj-scraper.reddit :refer :all]
    [clojure.java.jdbc :as jdbc]
    [clojure.string :as str]
    [clojure.core.async :refer [timeout <! <!! >! >!! thread]]
    [clojure.tools.cli :refer [parse-opts]]
    [taoensso.timbre :as timbre])
  (:gen-class))

#_(def reddit
  (reddit-oauth2-readonly "client-id-here" "client-secret-here" "user-agent-here"))

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

;; TODO: is the status table obsolete, since we can fetch all 1000 in 10 seconds?
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
  (jdbc/with-db-transaction [db db]
    (jdbc/execute! db (create-table-if-not-exists-ddl "ruwaterloo_posts" ruwaterloo-posts-schema))
    (jdbc/execute! db (create-table-if-not-exists-ddl "status" status-schema)))
  nil)


(defn insert-from-post
  "Given a schema (table spec) and a post,
  get a vector of values suitable for an insert into the database."
  [schema {:keys [kind data] :as post}]
  (assert (= kind "t3") "Expected 't3' kind for post")
  (mapv (fn [[col _]] (col data)) ruwaterloo-posts-schema))

(defn insert-posts-dml
  "Generates a vector containing the INSERT query
  and vectors of paramters to be inserted using jdbc/execute!
  with :multi true.

  Does OR IGNORE"
  [table schema posts]
  (into [(format "INSERT OR IGNORE INTO %s VALUES (%s)"
                 table
                 (str/join ", " (repeat (count schema) "?")))]
        (map #(insert-from-post schema %) posts)))

(defn insert-posts!
  "Insert a sequence of posts into the database.
  The sequence of posts should normally come from
  (get-in resp [:data :children]) where resp is the response
  from reddit-get->json.

  Returns the number of posts inserted."
  [db table schema posts]
  (->> (jdbc/execute! db (insert-posts-dml table schema posts) {:multi? true})
       (reduce +)))


(defn- after [db subreddit]
  (first (jdbc/query db ["select after from status where subreddit = ?"
                         subreddit])))

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

  To force a full scrape, set :force? to true. When this is set,
  the function does not look at the database to keep track of
  when to stop and tries to grab the full 1000 posts.

  In practice this isn't much of a problem, because we can
  periodically do full-scrapes of all 1000 posts accessible
  at this endpoint.

  This function runs synchronously and makes at most 1 request
  per second, as per reddit's rate limits. We don't do anything
  fancy -- if a request takes longer than 1 second, the next
  request will be made as soon as the current one finishes.
  "
  ([db reddit] (scrape! db reddit {}))
  ([db reddit {:keys [force?]}]
   (loop [after (after db "uwaterloo")
          seen #{}]
     (let [limit 100
           params (cond-> {:limit limit} after (assoc :after after))
           ;; throttling to at most 1 per second
           timer (timeout 1000)

           resp (reddit-get->json reddit "https://oauth.reddit.com/r/uwaterloo/new.json"
                                  params)
           ts (System/currentTimeMillis)
           posts (get-in resp [:data :children])
           next-after (get-in resp [:data :after])
           inserted (insert-posts! db "ruwaterloo_posts" ruwaterloo-posts-schema posts)

           ;; keep track of seen post IDs if :force?
           post-ids
           (and force? (into #{} (map #(get-in % [:data :id])) posts))

           intersect
           (clojure.set/intersection post-ids seen)
           ]
       (assert (<= inserted limit) "Only inserts up to limit")

       (timbre/infof "Inserted %s" inserted)
       (timbre/debugf "%s post(s) were repeated" (count intersect))

       (when (or (and force? (empty? intersect))
                 (= inserted limit))
         (<!! timer)
         (recur next-after (clojure.set/union post-ids seen)))))))

;; TODO: global rate limiter to simplify rate-limiting logic

;; inserting into status table is obsolete?
(comment
  (jdbc/with-db-transaction [db db]
    (jdbc/delete! db :status ["subreddit = ?" "uwaterloo"])
    (jdbc/insert! db :status [:subreddit :after :ts]
                  ["uwaterloo" next-after ts])))


;; increasing levels of verbosity
(def timbre-log-levels [:warn :info :debug :trace])

(def cli-opts
  [["-u" "--client-id CLIENT-ID" "Client ID"
    :id :client-id]
   ["-p" "--client-secret CLIENT-SECRET" "Client Secret"
    :id :client-secret]
   ["-a" "--user-agent USER-AGENT" "User Agent"
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
  (let [db {:classname   "org.sqlite.JDBC"
            :subprotocol "sqlite"
            :subname     "ruwaterloo.sqlite3"}

        {:keys [ok? client-id client-secret user-agent force? wait-seconds verbosity
                exit-code exit-message]}
        (validate-args args)

        reddit
        (and ok? (reddit-oauth2-readonly client-id client-secret user-agent))]
    (timbre/set-level!
      (->> (count timbre-log-levels) dec (min verbosity) (nth timbre-log-levels)))
    (if-not ok?
      (do
        (println exit-message)
        (System/exit exit-code))
      (do
        (initialize-db! db)
        (loop []
          (timbre/info "Scraping")
          (scrape! db reddit {:force? force?})
          (when wait-seconds
            (timbre/infof "Waiting %s seconds" wait-seconds)
            (<!! (timeout (* wait-seconds 1000)))
            (recur)))))))
