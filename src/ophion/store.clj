(ns ophion.store
  (:require
   [ophion.mongo :as db]))

(defn store-query
  [db {:keys [user key focus path query]}]
  (db/insert!
   db :ophionquery
   {:user user
    :key key
    :focus focus
    :path path
    :query query}))

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

