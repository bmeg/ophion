(ns ophion.mongo
  (:require
   [clojure.string :as string]
   [clojure.tools.cli :as cli]
   [taoensso.timbre :as log]
   [monger.core :as core]
   [monger.db :as db]
   [monger.conversion :as convert]
   [monger.collection :as mongo]
   [monger.util :as util]
   [protograph.template :as protograph]
   [ophion.config :as config])
  (:import
   [java.util.concurrent TimeUnit]
   [com.mongodb.client.model InsertManyOptions BulkWriteOptions]
   [com.mongodb DB WriteResult DBObject WriteConcern BulkWriteException
    AggregationOptions AggregationOptions$OutputMode]
   [java.util List Map]
   [org.bson.types ObjectId]))

(defn connect!
  [config]
  (let [connection (core/connect (select-keys config [:host :port]))
        database (name (:database config))]
    (core/get-db connection database)))

(defn insert!
  [db collection what]
  (mongo/insert db (name collection) what))

(defn merge!
  [db collection what]
  (mongo/update db (name collection) {:gid (:gid what)} {:$set what} {:upsert true}))

(defn upsert!
  [db collection where values]
  (mongo/update db (name collection) where values {:upsert true}))

(defn one
  [db collection gid]
  (mongo/find-one-as-map db (name collection) {:gid gid}))

(defn query
  [db collection where]
  (mongo/find-maps db (name collection) where))

(defn find-all
  [db collection]
  (mongo/find-maps db (name collection)))

(defn number
  ([db collection] (number db collection {}))
  ([db collection where]
   (mongo/count db (name collection) where)))

(defn collections
  [db]
  (set (db/get-collection-names db)))

(defn counts
  [db]
  (let [all (collections db)]
    (into
     {}
     (map
      (fn [collection]
        [collection (number db collection)])
      all))))

(defn purge!
  ([db]
   (let [present (collections db)]
     (doseq [collection present]
       (purge! db collection))))
  ([db collection]
   (mongo/remove db (name collection))))

(defn drop!
  [db collection]
  (mongo/drop db (name collection)))

(defn mapply
  [f & args]
  (apply f (apply concat (butlast args) (last args))))

(defn aggregate
  ([db collection pipeline] (aggregate db collection pipeline {}))
  ([db collection pipeline opts]
   (mapply mongo/aggregate db (name collection) pipeline opts)))

(defn build-aggregation-options
  ^AggregationOptions
  [{:keys [^Boolean allow-disk-use cursor ^Long max-time]}]
  (cond-> (AggregationOptions/builder)
    allow-disk-use       (.allowDiskUse allow-disk-use)
    cursor               (.outputMode AggregationOptions$OutputMode/CURSOR)
    max-time             (.maxTime max-time TimeUnit/MILLISECONDS)
    (:batch-size cursor) (.batchSize (int (:batch-size cursor)))
    true                 .build))

(defn raw-aggregate
  ([db collection pipeline] (aggregate db collection pipeline {}))
  ([db collection pipeline opts]
   (let [coll (.getCollection db collection)
         aggropts (build-aggregation-options opts)
         pipe (util/into-array-list (convert/to-db-object pipeline))]
     (iterator-seq (.aggregate coll pipe aggropts)))))

(defn disk-aggregate
  [db collection pipeline]
  (raw-aggregate
   db collection pipeline
   {:allow-disk-use true
    :cursor {:batch-size 1000}}))

(def document-limit 10000)

(defn bufferize-query
  ([db collection query buffer] (bufferize-query db collection query buffer 0))
  ([db collection query buffer page]
   (let [buffered-query (concat query [{:$skip (* buffer page)} {:$limit buffer}])
         results (aggregate
                  db collection
                  buffered-query
                  {:allow-disk-use true
                   :cursor {:batch-size document-limit}})]
     (if (or (empty? results) (< (count results) buffer))
       results
       (lazy-seq
        (concat
         results
         (bufferize-query db collection query buffer (inc page))))))))

(defn extract-failures
  [^BulkWriteException e]
  (group-by
   :message
   (map
    (fn [error]
      {:message (.getMessage error)
       :document (.getDetails error)})
    (.getWriteErrors e))))

(defn db-object
  [m]
  (convert/to-db-object m))

(defn bulk-insert!
  [db collection documents]
  (try
    (let [coll (.getCollection db (name collection))
          op (.initializeUnorderedBulkOperation coll)]
      (doseq [document documents]
        ;; (.insert op (convert/to-db-object document))
        (.updateOne
         (.upsert
          (.find op (db-object {:gid (:gid document)})))
         (db-object
          {:$set
           (db-object
            (merge (dissoc document :data) (:data document)))})))
      (.execute op))
    (catch BulkWriteException e
      (extract-failures e))))

(defn expand-fields
  [fields]
  (apply array-map (mapcat list fields (repeat 1))))

(defn index-name
  [fields]
  (string/join "-" (conj (mapv name fields) "index")))

(defn list-indexes
  [db collection]
  (mongo/indexes-on db (name collection)))

(defn index!
  ([db collection fields] (index! db collection fields {}))
  ([db collection fields opts]
   (let [array (expand-fields fields)
         index (index-name fields)]
     (mongo/ensure-index db (name collection) array (merge {:name index} opts)))))

(defn timestamp
  [record]
  (.getTimestamp (:_id record)))

(defn build-collection!
  [db collection indexes]
  (doseq [index indexes]
    (let [fields (butlast index)
          opts (last index)]
      (index! db collection fields opts))))

(defn build-indexes!
  [db indexes]
  (doseq [[collection dexes] indexes]
    (build-collection! db collection dexes)))

(def base-indexes
  {:vertex
   [[:gid {:unique true}]
    [:label {}]]
   :edge
   [[:gid {:unique true}]
    [:label {}]
    [:from {}]
    [:to {}]]
   :ophionquery
   [[:key {:unique true}]]
   :ophionresults
   [[:key {}]]})

(defn ensure-collection!
  [db element label]
  (if-not ((collections db) (name label))
    (build-collection! db label ((keyword element) base-indexes))))

(defn label-indexes
  [element {:keys [label fields]}]
  (let [indexes (map (fn [f] [f {}]) fields)]
    [label (concat (get base-indexes element) indexes)]))

(defn message-indexes
  [[key {:keys [vertexes edges]}]]
  (let [v (map (partial label-indexes :vertex) vertexes)
        e (map (partial label-indexes :edge) edges)]
    (into
     {}
     (concat v e))))

(defn protograph-indexes
  [protograph]
  (apply merge (map message-indexes protograph)))

;; check to see if indexes exist on boot?

(def parse-args
  [["-c" "--config CONFIG" "path to config file"]])

(defn -main
  [& args]
  (let [env (:options (cli/parse-opts args parse-args))
        path (or (:config env) "resources/config/ophion.clj")
        config (config/read-path path)
        protograph (protograph/load-protograph
                    (or
                     (get-in config [:protograph :path])
                     "resources/config/protograph.yml"))
        indexes (protograph-indexes protograph)
        db (connect! (get config :mongo))]
    (build-indexes! db indexes)))
