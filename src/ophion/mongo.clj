(ns ophion.mongo
  (:require
   [clojure.string :as string]
   [taoensso.timbre :as log]
   [monger.core :as db]
   [monger.conversion :as convert]
   [monger.collection :as mongo])
 (:import
  [com.mongodb.client.model InsertManyOptions BulkWriteOptions]
  [com.mongodb DB WriteResult DBObject WriteConcern BulkWriteException]
  [java.util List Map]
  [org.bson.types ObjectId]))

(defn connect!
  [config]
  (let [connection (db/connect (select-keys config [:host :port]))
        database (name (:database config))]
    (db/get-db connection database)))

(defn insert!
  [db collection what]
  (mongo/insert db (name collection) what))

(defn upsert!
  [db collection what]
  (mongo/update db (name collection) {:gid (:gid what)} {:$set what} {:upsert true}))

(defn update!
  [db collection where values]
  (mongo/update db (name collection) where values {:upsert true}))

(defn purge!
  ([db])
  ([db collection]
   (mongo/remove db (name collection))))

(defn one
  [db collection gid]
  (mongo/find-one-as-map db (name collection) {:gid gid}))

(defn query
  [db collection where]
  (mongo/find-maps db (name collection) where))

(defn find-all
  [db collection]
  (mongo/find-maps db (name collection)))

(defn count
  [db collection where]
  (mongo/count db (name collection) where))

(defn mapply
  [f & args]
  (apply f (apply concat (butlast args) (last args))))

(defn aggregate
  ([db collection pipeline] (aggregate db collection pipeline {}))
  ([db collection pipeline opts]
   ;; (log/info collection pipeline)
   (mapply mongo/aggregate db (name collection) pipeline opts)))

(defn extract-failures
  [^BulkWriteException e]
  (group-by
   :message
   (map
    (fn [error]
      {:message (.getMessage error)
       :document (.getDetails error)})
    (.getWriteErrors e))))

(defn bulk-insert!
  [db collection documents]
  (try
    (let [coll (.getCollection db (name collection))
          op (.initializeUnorderedBulkOperation coll)]
      (doseq [document documents]
        (.insert op (convert/to-db-object document)))
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

(defn build-indexes
  [db indexes]
  (doseq [[collection dexes] indexes]
    (doseq [index dexes]
      (let [fields (butlast index)
            opts (last index)]
        (index! db collection fields opts)))))

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

(defn -main
  [& args]
  (let [db (connect! {:database (or (second args) "ophion")})]
    (condp = (first args)
      "list"
      (do
        (println (list-indexes db :vertex))
        (println (list-indexes db :edge)))

      "build"
      (build-indexes db base-indexes))))
