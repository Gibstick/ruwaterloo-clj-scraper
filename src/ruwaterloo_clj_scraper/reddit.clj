(ns ruwaterloo-clj-scraper.reddit
  (:require
    [clj-http.client :as http]
    [clojure.data.json :as json])
  (:import java.util.Base64))

(defn base64-encode
  "Base64 encodes a string"
  [s]
  (.encodeToString (Base64/getEncoder) (.getBytes s)))


(defrecord Reddit
  [access-token expires-ms-timestamp user-agent renew-fn])


(defn reddit-oauth2-readonly
  "Perform a read-only oauth request for reddit and return a
  read-only Reddit handle. If the request fails, an exception
  from clj-http.get is thrown.

  client-id, client secret: should be from reddit's preferences
  user-agent: a unique user agent string, (see reddit's guidelines)"
  [client-id client-secret user-agent]
  (let [headers
        {"User-Agent"
         user-agent
         "Authorization"
         (->> (format "%s:%s" client-id client-secret)
              base64-encode
              (format "Basic %s"))}
        form-params
        {:grant_type "client_credentials"}]

    (let [ts (System/currentTimeMillis)
          resp (http/post "https://www.reddit.com/api/v1/access_token"
                          {:headers headers :form-params form-params})
          body (json/read-str (:body resp))]
      (->Reddit (body "access_token")
                (+ ts (* (body "expires_in") 1000))
                user-agent
                (fn [] (reddit-oauth2-readonly client-id client-secret user-agent))))))

(defn reddit-refresh-readonly
  "Request a new oauth2 readonly token if one is about to expire,
  and return a new Reddit instance. Otherwise, do nothing."
  [reddit]
  (if (>= (System/currentTimeMillis)
          (- (:expires-ms-timestamp reddit) 180000))
    ((:renew-fn reddit))
    reddit))

(defn reddit-get
  "Perform a GET request to the reddit API

  reddit: a valid Reddit instance
  url: the ful URL of the API endpoint
  query-params: optional query paramters for the request

  Returns a clj-http get response map. Does not throw an
  exception if the requets fails."
  ([reddit url]
   (reddit-get reddit url {}))
  ([reddit url query-params]
   (let [query-params (assoc query-params :raw_json 1)
         reddit (reddit-refresh-readonly reddit)
         headers {"User-Agent"    (:user-agent reddit)
                  "Authorization" (str "Bearer: " (:access-token reddit))}]
     (http/get url {:headers          headers
                    :query-params     query-params
                    :throw-exceptions false}))))

(defn reddit-get->json
  "Wraps reddit-get with a call to extract the parsed json body.
  Keys will be transformed from strings into keywords."
  ([reddit url]
   (reddit-get->json reddit url {}))
  ([reddit url query-params]
   (-> (reddit-get reddit url query-params)
       :body
       (json/read-str :key-fn keyword))))
