(ns ophion.httpkit
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io]
   [cheshire.core :as json]
   [polaris.core :as polaris]
   [taoensso.timbre :as log]
   [ring.middleware.resource :as ring]
   [org.httpkit.server :as http]
   [protograph.core :as protograph]
   [protograph.kafka :as kafka]
   [ophion.config :as config]
   [ophion.db :as db]
   [ophion.query :as query]
   [ophion.search :as search])
  (:import
   [protograph Protograph]
   [java.io InputStreamReader]
   [ch.qos.logback.classic Logger Level]))

(.setLevel
 (org.slf4j.LoggerFactory/getLogger
  (Logger/ROOT_LOGGER_NAME))
 Level/INFO)

(defn append-newline
  [s]
  (str s "\n"))

(def output
  (comp
   append-newline
   json/generate-string
   query/translate))

(defn default-graph
  []
  (let [config (config/read-config "config/ophion.clj")
        graph (db/connect (:graph config))
        search (search/connect (:search config))]
    {:graph graph
     :search search}))

(defn fetch-schema-handler
  [schema request]
  {:status 200
   :headers {"content-type" "application/json"}
   :body schema})

(defn find-vertex-handler
  [graph request]
  (let [gid (-> request :params :gid)
        vertex (query/find-vertex graph gid)
        out (query/vertex-connections vertex)]
    {:status 200
     :headers {"content-type" "application/json"}
     :body (json/generate-string out)}))

(defn vertex-query-handler
  [graph search request]
  (let [raw-query (json/parse-stream (InputStreamReader. (:body request)) keyword)
        query (query/delabelize raw-query)
        _ (log/info (mapv identity query))
        result (query/evaluate {:graph graph :search search} query)
        out (string/join (map output result))]
    (db/commit graph)
    {:status 200
     :headers {"content-type" "application/json"}
     :body out}))

(defn find-edge-handler
  [graph request]
  {:status 200
   :headers {"content-type" "application/json"}
   :body "found"})

(defn edge-query-handler
  [graph request]
  {:status 200
   :headers {"content-type" "application/json"}
   :body "found"})

(defn fetch-schema
  [schema]
  (fn [request]
    (#'fetch-schema-handler schema request)))

(defn find-vertex
  [graph]
  (fn [request]
    (#'find-vertex-handler graph request)))

(defn vertex-query
  [graph search]
  (fn [request]
    (#'vertex-query-handler graph search request)))

(defn find-edge
  [graph]
  (fn [request]
    (#'find-edge-handler graph request)))

(defn edge-query
  [graph]
  (fn [request]
    (#'edge-query-handler graph request)))

(defn home
  [request]
  {:status 200
   :headers {"content-type" "text/html"}
   :body (config/read-resource "public/viewer.html")})

(defn ophion-routes
  [graph search protograph]
  [["/" :home #'home]
   ["/schema/protograph" :schema (fetch-schema protograph)]
   ["/vertex/find/:gid" :vertex-find (find-vertex graph)]
   ["/vertex/query" :vertex-query (vertex-query graph search)]
   ["/edge/find/:out/:label/:in" :edge-find (find-edge graph)]
   ["/edge/query" :edge-query (edge-query graph)]])

(defn start
  []
  (log/info "starting server")
  (let [config (config/read-config "config/ophion.clj")
        graph (db/connect (:graph config))
        search (search/connect (:search config))
        protograph (protograph/load-protograph (or (get-in config [:protograph :path]) "config/protograph.yml"))
        schema (Protograph/writeJSON (.graphStructure protograph))
        routes (polaris/build-routes (ophion-routes graph search schema))
        router (ring/wrap-resource (polaris/router routes) "public")]
    (http/run-server
     router
     {:port (or (:port config) 4443)})))

(defn -main
  []
  (start))
