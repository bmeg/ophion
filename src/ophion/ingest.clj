(ns ophion.ingest
  (:require
   [clojure.string :as string]
   [clojure.java.io :as io]
   [clojure.tools.cli :as cli]
   [taoensso.timbre :as log]
   [cheshire.core :as json]
   [protograph.kafka :as kafka]
   [protograph.core :as protograph]
   [ophion.db :as db]
   [ophion.query :as query]
   [ophion.config :as config]
   [ophion.search :as search]
   [ophion.mongo :as mongo]
   [ophion.aggregate :as aggregate])
  (:import
   [protograph Protograph]))

(defn ingest-vertex
  [graph data]
  (aggregate/add-vertex! graph data))

(defn ingest-edge
  [graph data]
  (log/info "ingest edge" data)
  (aggregate/add-edge! graph data))

;; (defn ingest-vertex
;;   [graph data]
;;   (let [vertex (query/add-vertex! graph data)
;;         id (.id vertex)]
;;     {:id id
;;      :data data
;;      :graph "vertex"}))

;; (defn ingest-edge
;;   [graph data]
;;   (log/info "ingest edge" data)
;;   (let [edge (query/add-edge! graph data)
;;         id (.id edge)
;;         out-id (.getOutVertexId id)
;;         type-id (.getTypeId id)
;;         edge-id (.getRelationId id)
;;         in-id (.getInVertexId id)]
;;     {:id edge-id
;;      :out-id out-id
;;      :label type-id
;;      :in-id in-id
;;      :data data
;;      :graph "edge"}))

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
      (doseq [lines (partition 1000 processed)]
        ;; (try
        ;;   (mongo/insert-many! graph (string/lower-case label) lines)
        ;;   ;; (mongo/bulk-insert! graph (string/lower-case label) lines)
        ;;   (catch Exception e (println "failed")))
        (try
          (mongo/bulk-insert! graph (string/lower-case label) lines)
          (catch Exception e (println e)))))))

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
          groups (group-by :gid processed)]
      (doseq [lines (partition 1000 groups)]
        (let [merged (map
                      (fn [[gid parts]]
                        (apply merge parts))
                      lines)]
          (mongo/bulk-insert! graph (string/lower-case label) merged))))))

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
        ;; graph (db/connect (:graph config))
        graph (mongo/connect! {:database "pillar"})
        search (search/connect (merge search/default-config (:search config)))
        continuation (partial search/index-message search)]
    (if (:topic env)
      (ingest-topic config graph continuation)
      ;; (ingest-file (:input env) graph continuation)
      (ingest-batches (:input env) graph))))
