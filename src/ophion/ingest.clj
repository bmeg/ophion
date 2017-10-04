(ns ophion.ingest
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io]
   [clojure.tools.cli :as cli]
   [clojure.pprint :as pprint]
   [taoensso.timbre :as log]
   [cheshire.core :as json]
   [protograph.kafka :as kafka]
   [ophion.db :as db]
   [ophion.query :as query]
   [ophion.config :as config]
   [ophion.search :as search]
   [ophion.mongo :as mongo]
   [ophion.aggregate :as aggregate])
  (:import
   [com.mongodb BulkWriteException]))

;; (defn ingest-vertex
;;   [graph data]
;;   (aggregate/add-vertex! graph data))

;; (defn ingest-edge
;;   [graph data]
;;   (log/info "ingest edge" data)
;;   (aggregate/add-edge! graph data))

(defn lift-properties
  [data]
  (merge
   (:properties data)
   (dissoc data :properties)))

(defn ingest-vertex
  [graph data]
  (log/info "vertex" (:gid data))
  (let [vertex (query/add-vertex! graph data)
        id (.id vertex)]
    (merge
     (lift-properties data)
     {:_janusId id
      :_graph "vertex"})))

(defn ingest-edge
  [graph data]
  (log/info "edge" (:gid data))
  (let [edge (query/add-edge! graph data)
        id (.id edge)
        from-id (.getOutVertexId id)
        type-id (.getTypeId id)
        edge-id (.getRelationId id)
        to-id (.getInVertexId id)]
    (merge
     (lift-properties data)
     {:_janusId edge-id
      :_graph "edge"
      :_fromId from-id
      :_labelId type-id
      :_toId to-id})))

(defn ingest-message
  [graph producer prefix continuation message]
  (let [label (kafka/topic->label (.topic message))
        data (json/parse-string (.value message) keyword)
        _ (log/info label data)
        result (condp = label
                 "Vertex" (ingest-vertex graph data)
                 "Edge" (ingest-edge graph data))]
    (continuation result)))

(defn ingest-topic
  [config graph continuation]
  (let [host (get-in config [:kafka :host])
        port (or (get-in config [:kafka :port]) "9092")
        kafka-url (str host ":" port)
        group-id (kafka/uuid)
        prefix (get-in config [:protograph :prefix])
        input-topics [(str prefix ".Vertex") (str prefix ".Edge")]
        output-prefix (get-in config [:protograph :output])
        consumer (kafka/consumer {:host kafka-url :topics input-topics})
        producer (kafka/producer kafka-url)]
    (kafka/consume
     consumer
     (partial ingest-message graph producer output-prefix continuation))))

(defn ingest-file
  [path graph continuation]
  (doseq [file (kafka/dir->files path)]
    (log/info "ingesting" (.getName file))
    (let [label (kafka/path->label (.getName file))
          lines (line-seq (io/reader file))]
      (doseq [line lines]
        (let [data (json/parse-string line keyword)
              result (condp = label
                       "Vertex" (ingest-vertex graph data)
                       "Edge" (ingest-edge graph data))]
          (continuation result))))))

(defn ingest-batches
  [path graph]
  (doseq [file (kafka/dir->files path)]
    (let [label (kafka/path->label (.getName file))
          lines (line-seq (io/reader file))
          processed (map
                     (comp
                      (if (= label "Vertex")
                        aggregate/process-vertex
                        aggregate/process-edge)
                      #(json/parse-string % keyword))
                     lines)]
      (doseq [lines (partition-all 1000 processed)]
        (pprint/pprint
         (mongo/bulk-insert! graph (string/lower-case label) lines))))))

(defn ingest-batches-carefully
  [path graph]
  (doseq [file (kafka/dir->files path)]
    (let [label (kafka/path->label (.getName file))
          lines (line-seq (io/reader file))
          processed (map
                     (comp
                      (if (= label "Vertex")
                        aggregate/process-vertex
                        aggregate/process-edge)
                      #(json/parse-string % keyword))
                     lines)
          groups (group-by :gid processed)
          merged (map
                  (fn [[gid parts]]
                    (apply merge parts))
                  groups)]
      (pprint/pprint
       (mongo/bulk-insert! graph (string/lower-case label) merged)))))

(def parse-args
  [["-k" "--kafka KAFKA" "host for kafka server"
    :default "localhost:9092"]
   ["-i" "--input INPUT" "input file or directory"]
   ["-x" "--prefix PREFIX" "input topic prefix"]])

(defn assoc-env
  [config env]
  (-> config
      (assoc-in [:protograph :prefix] (:prefix env))
      (assoc-in [:kafka :host] (:kafka env))
      (assoc :command env)))

(defn -main
  [& args]
  (let [env (:options (cli/parse-opts args parse-args))
        config (config/read-config "config/ophion.clj")
        graph (db/connect (:graph config))
        ;; graph (mongo/connect! {:database "pillar"})
        search (search/connect (merge search/default-config (:search config)))
        indexed? (get-in config [:search :indexed?] #{})
        continuation (fn [message]
                       (when (indexed? (:label message))
                         (log/info "indexing" (:label message))
                         (search/index-message search message)))]
    (if (:topic env)
      (ingest-topic config graph continuation)
      ;; (ingest-batches (:input env) graph)
      (ingest-file (:input env) graph continuation))
    (log/info "ingest complete")))
