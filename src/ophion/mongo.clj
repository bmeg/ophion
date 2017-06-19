(ns ophion.mongo
  (:require
   [clojure.string :as string]
   [taoensso.timbre :as log]
   [monger.core :as db]
   [monger.conversion :as convert]
   [monger.collection :as mongo])
 (:import
  [com.mongodb.client.model InsertManyOptions BulkWriteOptions]
  [com.mongodb DB WriteResult DBObject WriteConcern]
  [java.util List Map]
  [org.bson.types ObjectId]))

(defn connect!
  [config]
  (let [connection (db/connect)]
    (db/get-db connection (name (:database config)))))

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

(defn aggregate
  [db collection pipeline]
  (log/info collection pipeline)
  (mongo/aggregate db (name collection) pipeline))

;; (defn ^WriteResult insert-many!
;;   "Saves documents do collection. You can optionally specify WriteConcern as a third argument."
;;   ([^DB db ^String coll ^List documents]
;;    (.bulkWrite
;;     (.getCollection db (name coll))
;;     ^List (convert/to-db-object documents)
;;     (.ordered (new BulkWriteOptions) false)))
;;     ;; (convert/to-db-object
;;     ;;  {:writeConcern ^WriteConcern db/*mongodb-write-concern*
;;     ;;   :ordered false})
;;   ([^DB db ^String coll ^List documents ^WriteConcern concern]
;;    (.bulkWrite
;;     (.getCollection db (name coll))
;;     ^List (convert/to-db-object documents)
;;     (.ordered (new BulkWriteOptions) false))))

;; (defn bulk-insert!
;;   [db collection documents]
;;   (mongo/insert-batch db (name collection) documents))

(defn bulk-insert!
  [db collection documents]
  (let [coll (.getCollection db (name collection))
        op (.initializeUnorderedBulkOperation coll)]
    (doseq [document documents]
      (.insert op (convert/to-db-object document)))
    (.execute op)))

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
    [:to {}]]})

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
