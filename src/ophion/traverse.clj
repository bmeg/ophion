(ns ophion.traverse
  (:require
   [clojure.walk :as walk]
   [clojure.string :as string]
   [cheshire.core :as json]
   [taoensso.timbre :as log]
   [ophion.mongo :as mongo]))

;; this is an experiment to create a sequence of queries rather than
;; using the aggregation pipeline for the whole thing.

(defn where
  [where]
  [{:$match where}])

(defn from-edge
  [db label froms where]
  (mongo/bufferize-query
   db :edge
   {:$match
    (merge
     where
     {:label label
      :from {:$in (map :gid froms)}})}
   document-limit))

(defn to-edge
  [db label tos where]
  (mongo/bufferize-query
   db :edge
   {:$match
    (merge
     where
     {:label label
      :to {:$in (map :gid tos)}})}
   document-limit))

(defn from-vertex
  [db edges where]
  (mongo/bufferize-query
   db :vertex
   {:$match
    (merge
     where
     {:gid {:$in (map :from edges)}})}
   document-limit))

(defn to-vertex
  [db edges where]
  (mongo/bufferize-query
   db :vertex
   {:$match
    (merge
     where
     {:gid {:$in (map :from edges)}})}
   document-limit))

(defn from
  [label])

(defn to
  [label])

