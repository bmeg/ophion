(ns ophion.immutant
  (:require
   [clojure.data.json :as json]
   [taoensso.timbre :as timbre]
   [immutant.web :as web]
   [immutant.web.async :as async]
   [ophion.query :as query])
  (:import
   [java.io InputStreamReader]))

(timbre/refer-timbre)

(defn ophion-static
  [request]
  {:status 200
   :body "ophion"})

(def graph (query/tinkergraph))

(defn ophion
  [request]
  (info (str request))
  (async/as-channel
   request
   {:on-open
    (fn [stream]
      (let [reader (InputStreamReader. (:body request))
            query (json/read reader :key-fn keyword)
            result (query/evaluate graph query)]
        (map
         (comp async/send! query/translate)
         (iterator-seq result))))}))

(defn start
  []
  (info "starting server")
  (web/run #'ophion {:port 4443}))

(defn -main
  []
  (start))
