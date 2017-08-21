(ns ophion.store
  (:require
   [ophion.mongo :as db]))

(defn store-query
  [db {:keys [user key focus query]}]
  (db/insert!
   db :ophionquery
   {:user user
    :key key
    :focus focus
    :query query}))

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

