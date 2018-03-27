(ns ophion.server
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io]
   [clojure.tools.cli :as cli]
   [manifold.stream :as stream]
   [aleph.http :as http]
   [cheshire.core :as json]
   [taoensso.timbre :as log]
   [ring.middleware.resource :as resource]
   [ring.middleware.params :as params]
   [ring.middleware.keyword-params :as keyword]
   [polaris.core :as polaris]
   [protograph.template :as protograph]
   [ophion.config :as config]
   [ophion.db :as db]
   [ophion.query :as query]
   [ophion.search :as search]
   [ophion.mongo :as mongo]
   [ophion.store :as store]
   [ophion.aggregate :as aggregate])
  (:import
   [java.io InputStreamReader]
   [ch.qos.logback.classic Logger Level]))

;; (.setLevel
;;  (org.slf4j.LoggerFactory/getLogger
;;   (Logger/ROOT_LOGGER_NAME))
;;  Level/INFO)

(defn read-json
  [body]
  (json/parse-stream
   (InputStreamReader. body)
   keyword))

(defn append-newline
  [s]
  (str s "\n"))

(def output
  (comp
   append-newline
   json/generate-string))

(defn fetch-schema-handler
  [schema request]
  {:status 200
   :headers {"content-type" "application/json"}
   :body (json/generate-string schema)})

(defn aggregate-query-handler
  [mongo protograph request]
  (let [query (mapv identity (read-json (:body request)))
        label (get-in request [:params :label])
        result (aggregate/evaluate mongo label query)
        out (map output result)]
    {:status 200
     :headers {"content-type" "application/json"}
     :body (stream/->source out)}))

(defn fetch-schema
  [schema]
  (fn [request]
    (log/info "--> fetch schema")
    (#'fetch-schema-handler schema request)))

(defn aggregate-query
  [mongo protograph]
  (fn [request]
    (log/info "--> aggregate query")
    (#'aggregate-query-handler mongo protograph request)))

(defn home
  [request]
  {:status 200
   :headers {"content-type" "text/html"}
   :body (config/read-resource "public/index.html")})

(defn ophion-routes
  [graph protograph]
  [["/" :home #'home]
   ["/schema/protograph" :schema (fetch-schema protograph)]
   ["/query/:label" :aggregate-query (aggregate-query graph protograph)]])

(def parse-args
  [["-c" "--config CONFIG" "path to config file"]])

(defn start
  [env]
  (let [path (or (:config env) "resources/config/ophion.clj")
        config (config/read-path path)
        proto (or (get-in config [:protograph :path]) "resources/config/protograph.yaml")
        protograph (protograph/load-protograph proto)
        schema (protograph/graph-structure protograph)
        graph (mongo/connect! (:mongo config))
        routes (polaris/build-routes (ophion-routes graph schema))
        router (polaris/router routes)
        app (-> router
                (resource/wrap-resource "public")
                (keyword/wrap-keyword-params)
                (params/wrap-params))]
    (log/info schema)
    (http/start-server app {:port (or (:port config) 4443)})))

(defn -main
  [& args]
  (let [env (:options (cli/parse-opts args parse-args))]
    (start env)
    (while true
      (Thread/sleep 1111))))
