(ns ophion.store
  (:require
   [clojure.set :as set]
   [ophion.mongo :as db]
   [ophion.query :as query]))

(defn store-query
  [graph db {:keys [user key focus path query current]}]
  (let [pure (query/delabelize query)
        insert {:user user
                :key key
                :focus focus
                :path path
                :current current
                :query pure}]
    (db/insert! db :ophionquery insert)
    (let [{:keys [results commit]} (query/perform graph pure)
          index (map-indexed
                 (fn [index result]
                   {:key key
                    :order index
                    :result result})
                 results)]
      (db/bulk-insert! db :ophionresults index)
      (commit)
      {:stored (count index)})))

(defn all-queries
  [db]
  (let [queries
        (map
         #(dissoc % :_id)
         (db/find-all db :ophionquery))]
    (group-by :focus queries)))

(defn load-query
  [db {:keys [user key]}]
  (db/query
   db :ophionquery
   {:user user
    :key key}))

(defn load-results
  [db key]
  (db/aggregate
   db :ophionresults
   [{:$match {:key key}}
    {:$sort {:order 1}}
    {:$replaceRoot {:newRoot "$result"}}]))

(defn queries-for-focus
  [db focus]
  (db/query
   db :ophionquery
   {:focus focus}))

(defn queries-for-user
  [db user]
  (db/query
   db :ophionquery
   {:user user}))

(defn results-map
  [db queries]
  (into
   {}
   (map
    (juxt
     identity
     (partial load-results db))
    queries)))

(defn query-comparison
  [db queries]
  (let [results (results-map db queries)
        sets (map
              (fn [result]
                (set (map :gid result)))
              (vals results))
        intersect (apply set/intersection sets)
        counts (into {} (map (fn [[k v]] [k (count v)]) results))]
    (assoc counts :_intersection (count intersect))))
