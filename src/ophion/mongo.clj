(ns ophion.mongo
  (:require
   [taoensso.timbre :as log]
   [monger.core :as db]
   [monger.collection :as mongo])
  (:import
   [org.bson.types ObjectId]))

(defn connect!
  [config]
  (let [connection (db/connect)]
    (db/get-db connection (name (:database config)))))

(defn insert!
  [db collection what]
  (mongo/insert db collection what))

(defn upsert!
  [db collection what]
  (mongo/update db collection {:gid (:gid what)} {:$set what} {:upsert true}))

(defn update!
  [db collection where values]
  (mongo/update db collection where values {:upsert true}))

(defn purge!
  ([db])
  ([db collection]
   (mongo/remove db collection)))

(defn one
  [db collection gid]
  (mongo/find-one-as-map db collection {:gid gid}))

(defn query
  [db collection where]
  (mongo/find-maps db collection where))

(defn find-all
  [db collection]
  (mongo/find-maps db collection))

(defn aggregate
  [db collection pipeline]
  (log/info collection pipeline)
  (mongo/aggregate db (name collection) pipeline))

(defn timestamp
  [record]
  (.getTimestamp (:_id record)))

(defn flat
  "this will squash properties if they are in the reserved set"
  [element]
  (merge
   (:properties element)
   (dissoc element :properties)))

(defn edge-gid
  [edge]
  (str
   "(" (:from edge) ")"
   "--" (:label edge) "->"
   "(" (:to edge) ")"))

(defn ingest-graph!
  [db {:keys [vertexes edges]}]
  (doseq [vertex vertexes]
    (upsert! db :vertex (assoc (flat vertex) :type "vertex")))
  (doseq [edge edges]
    (let [gid (edge-gid edge)]
      (upsert! db :edge (assoc (flat edge) :gid gid :type "edge")))))

(def gods-graph
  {:vertexes
   [{:label "location" :gid "sky" :properties {:name "sky"}}
    {:label "location" :gid "sea" :properties {:name "sea"}}
    {:label "location" :gid "tartarus" :properties {:name "tartarus"}}
    {:label "titan" :gid "saturn" :properties {:name "saturn" :age 5000}}
    {:label "titan" :gid "saturn" :properties {:name "saturn" :age 10000}}
    {:label "god" :gid "jupiter" :properties {:name "jupiter" :age 5000}}
    {:label "god" :gid "neptune" :properties {:name "neptune" :age 4500}}
    {:label "god" :gid "pluto" :properties {:name "pluto"}}
    {:label "god" :gid "pluto" :properties {:age 4000}}
    {:label "demigod" :gid "hercules" :properties {:name "hercules" :age 30}}
    {:label "human" :gid "alcmene" :properties {:name "alcmene" :age 45}}
    {:label "monster" :gid "nemean" :properties {:name "nemean"}}
    {:label "monster" :gid "hydra" :properties {:name "hydra"}}
    {:label "monster" :gid "cerberus" :properties {:name "cerberus"}}]
   :edges
   [{:from-label "god" :from "jupiter" :label "father" :to-label "god" :to "saturn"}
    {:from-label "god" :from "jupiter" :label "brother" :to-label "god" :to "neptune"}
    {:from-label "god" :from "jupiter" :label "brother" :to-label "god" :to "neptune"}
    {:from-label "god" :from "jupiter" :label "brother" :to-label "god" :to "pluto"}
    {:from-label "god" :from "jupiter" :label "lives" :to-label "location" :to "sky" :properties {:reason "likes wind" :much 0.3}}
    {:from-label "god" :from "neptune" :label "brother" :to-label "god" :to "jupiter"}
    {:from-label "god" :from "neptune" :label "brother" :to-label "god" :to "pluto"}
    {:from-label "god" :from "neptune" :label "lives" :to-label "location" :to "sea" :properties {:reason "likes waves" :much 0.4}}
    {:from-label "god" :from "pluto" :label "brother" :to-label "god" :to "jupiter"}
    {:from-label "god" :from "pluto" :label "brother" :to-label "god" :to "neptune"}
    {:from-label "god" :from "pluto" :label "lives" :to-label "location" :to "tartarus" :properties {:reason "likes death" :much 0.5}}
    {:from-label "demigod" :from "hercules" :label "father" :to-label "god" :to "jupiter"}
    {:from-label "demigod" :from "hercules" :label "mother" :to-label "human" :to "alcmene"}
    {:from-label "demigod" :from "hercules" :label "battled" :to-label "monster" :to "nemean" :properties {:trial 1}}
    {:from-label "demigod" :from "hercules" :label "battled" :to-label "monster" :to "hydra" :properties {:trial 2}}
    {:from-label "monster" :from "hercules" :label "battled" :to-label "monster" :to "cerberus" :properties {:trial 12}}
    {:from-label "monster" :from "cerberus" :label "lives" :to-label "location" :to "tartarus"}]})

